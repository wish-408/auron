/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.auron.flink.runtime.operator;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.auron.flink.arrow.FlinkArrowFFIExporter;
import org.apache.auron.flink.arrow.FlinkArrowReader;
import org.apache.auron.flink.arrow.FlinkArrowUtils;
import org.apache.auron.flink.configuration.FlinkAuronConfiguration;
import org.apache.auron.flink.metric.FlinkMetricNode;
import org.apache.auron.flink.runtime.operator.AuronPlanTreeRewriter.FlinkUdfGroup;
import org.apache.auron.flink.table.function.FlinkUdfLifecycle;
// import org.apache.auron.flink.table.function.FlinkUdfPayload;
import org.apache.auron.flink.table.function.FlinkUdfToken;
import org.apache.auron.flink.table.function.MergedUDFWrapperContext;
import org.apache.auron.jni.AuronAdaptor;
import org.apache.auron.jni.AuronCallNativeWrapper;
import org.apache.auron.jni.JniBridge;
import org.apache.auron.metric.MetricNode;
import org.apache.auron.protobuf.FFIReaderExecNode;
import org.apache.auron.protobuf.PhysicalPlanNode;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.types.logical.RowType;

/**
 * Flink {@link TableStreamOperator} that executes a pre-composed native Calc plan of shape
 * {@code Project[Filter[FFIReader]]} (or any single/combination of {@code Project} / {@code
 * Filter} above an {@code FFIReader} leaf) by streaming {@link RowData} through Auron's native
 * engine.
 *
 * <p>Architecture:
 *
 * <ul>
 *   <li>Input bridge: {@link FlinkArrowFFIExporter} buffers JVM {@link RowData} into an Arrow
 *       {@link VectorSchemaRoot} and exports it across the Arrow C-Data FFI to native code.
 *   <li>Native runtime: an {@link AuronCallNativeWrapper} executes the {@link PhysicalPlanNode}
 *       and pulls input batches from the exporter (registered in {@link JniBridge}'s resource
 *       map under a per-subtask resource ID).
 *   <li>Output bridge: {@link FlinkArrowReader} wraps each native output batch as Flink
 *       {@link RowData}; rows are forwarded via the inherited {@code output} collector.
 * </ul>
 *
 * <p>Resource ID convention: {@code FlinkAuronCalc-<flinkOpUniqueId>-<subtaskIndex>:<UUID>}. The
 * Flink operator unique ID and subtask index keep the key unique per subtask so native-side
 * registrations from concurrent subtasks of the same operator do not collide; the UUID
 * disambiguates re-runs after operator restart. The {@link FFIReaderExecNode} proto has no
 * {@code auron_operator_id} field, so identity lives exclusively in the resource ID.
 *
 * <p>Lifecycle and drain semantics:
 *
 * <ul>
 *   <li>{@link #processElement(StreamRecord)} buffers each row; if the exporter signals
 *       batch-full it eagerly drains the native pipeline.
 *   <li>{@link #processWatermark(Watermark)} drains BEFORE forwarding the watermark — otherwise
 *       a downstream operator could receive a watermark that crosses rows still buffered here.
 *   <li>{@link #prepareSnapshotPreBarrier(long)} drains so buffered rows reach downstream
 *       before the checkpoint barrier crosses them. Calc is stateless; no operator-state writes
 *       are needed.
 *   <li>{@link #finish()} owns the FINAL drain: Flink's StreamTask invokes it when input ends
 *       normally, so buffered rows reach downstream before the operator is torn down.
 *   <li>{@link #close()} is teardown-only — it never drains (failed or cancelled tasks must not
 *       forward buffered rows) and is idempotent via per-field nulling. Failure order: UDF
 *       lifecycle stop, native runtime, UDF lifecycle close, exporter, child allocator,
 *       {@code super.close()}; every step runs, the first failure propagates, later ones join
 *       as suppressed.
 *   <li>UDF payloads embedded in the plan (see
 *       {@link org.apache.auron.flink.table.function.FlinkUdfPayload}) are bound in
 *       {@link #open()}: one {@link FlinkUdfLifecycle} owns every UDF instance of this plan, and
 *       wrapper nodes are rewritten from serialized payloads to registry
 *       {@link org.apache.auron.flink.table.function.FlinkUdfToken}s before the plan first
 *       reaches native code. A native pipeline failure mid-drain stops the lifecycle first so
 *       in-flight UDF evals fail fast instead of outliving the failing drain.
 * </ul>
 *
 * <p>Valid construction (well-formed plan tree, matching row types, non-null operator ID) is
 * the caller's responsibility — no defensive null checks are performed.
 */
public class FlinkAuronCalcOperator extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, FlinkAuronOperator {

    private static final long serialVersionUID = 1L;

    /**
     * Soft row-count threshold above which the exporter reports {@link
     * FlinkArrowFFIExporter#isBatchFull()} and the operator eagerly drains the pipeline. 8192 is
     * the conventional batch size for vectorized engines (matches the Spark integration's
     * batch-size default of ~10000 and DataFusion's typical batch size).
     */
    private static final int BATCH_ROW_LIMIT = 8192;

    private final PhysicalPlanNode plan;
    private final RowType inputRowType;
    private final RowType outputRowType;
    private final String auronOperatorId;
    private final NativeRuntimeFactory nativeRuntimeFactory;

    // Per-subtask runtime state (transient — re-initialized in open()).
    private transient String resourceId;
    private transient BufferAllocator childAllocator;
    private transient FlinkArrowFFIExporter exporter;
    private transient FlinkMetricNode metricNode;
    private transient NativeRuntime nativeRuntime;
    private transient PhysicalPlanNode runtimePlan;
    private transient StreamRecord<RowData> outputRecord;
    // Owns every UDF instance of this plan; null when the plan carries no UDF payloads.
    private transient FlinkUdfLifecycle udfLifecycle;
    // Cached so reinitExporterAndRuntime() can reuse the value resolved once in open().
    private transient long nativeMemory;
    // Set for the final drain (finish()) so drainNative() skips its post-loop reinit.
    private transient boolean closing;

    /**
     * Production constructor: uses the default factory that creates an
     * {@link AuronCallNativeWrapper}.
     *
     * @param plan the pre-composed native plan tree (Project / Filter / FFIReader shape)
     * @param inputRowType the Flink row type of the input stream
     * @param outputRowType the Flink row type of the output stream
     * @param auronOperatorId the Auron operator ID for metric and identity propagation; the
     *     subtask-suffixed variant is held only in the runtime resource ID
     */
    public FlinkAuronCalcOperator(
            PhysicalPlanNode plan, RowType inputRowType, RowType outputRowType, String auronOperatorId) {
        this(plan, inputRowType, outputRowType, auronOperatorId, defaultNativeRuntimeFactory());
    }

    /** Test seam constructor accepting a {@link NativeRuntimeFactory}. */
    @VisibleForTesting
    FlinkAuronCalcOperator(
            PhysicalPlanNode plan,
            RowType inputRowType,
            RowType outputRowType,
            String auronOperatorId,
            NativeRuntimeFactory nativeRuntimeFactory) {
        this.plan = plan;
        this.inputRowType = inputRowType;
        this.outputRowType = outputRowType;
        this.auronOperatorId = auronOperatorId;
        this.nativeRuntimeFactory = nativeRuntimeFactory;
    }

    @Override
    public void open() throws Exception {
        super.open();
        StreamingRuntimeContext rc = (StreamingRuntimeContext) getRuntimeContext();
        String opIdWithSubtask = rc.getOperatorUniqueID() + "-" + rc.getIndexOfThisSubtask();

        this.childAllocator = FlinkArrowUtils.createChildAllocator("FlinkAuronCalc-" + opIdWithSubtask);
        this.metricNode = buildMetricTree(plan, getMetricGroup());
        this.exporter = new FlinkArrowFFIExporter(childAllocator, inputRowType, BATCH_ROW_LIMIT);
        // UUID disambiguates re-runs after operator restart so a stale registration cannot
        // collide with the new one.
        this.resourceId = "FlinkAuronCalc-" + opIdWithSubtask + ":" + UUID.randomUUID();
        JniBridge.putResource(resourceId, exporter);

        this.runtimePlan = injectFfiReaderLeaf(plan, resourceId);
        bindUdfLifecycle();

        this.nativeMemory =
                AuronAdaptor.getInstance().getAuronConfiguration().getLong(FlinkAuronConfiguration.NATIVE_MEMORY_SIZE);
        // Native runtime construction starts a tokio task that immediately begins streaming
        // output through the FFI Reader; on the first pull from an empty exporter the reader
        // sees end-of-stream and calls exporter.close(), nulling the writer. Defer construction
        // to the first non-empty drain so the runtime always starts against a populated buffer.

        this.outputRecord = new StreamRecord<>(null);
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        exporter.offer(element.getValue());
        if (exporter.isBatchFull()) {
            drainNative();
        }
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        drainNative();
        super.processWatermark(mark);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        drainNative();
        super.prepareSnapshotPreBarrier(checkpointId);
    }

    /**
     * Final drain of the operator. Flink 1.18's {@code StreamTask} invokes {@code finish()} when
     * input ends normally (the base {@code AbstractStreamOperator} implementation is a no-op),
     * which makes this the only correct home for the end-of-input drain: buffered rows must
     * reach downstream before {@link #close()} tears down the native runtime. The {@code
     * closing} flag set here suppresses {@link #drainNative()}'s post-cycle reinit so the final
     * drain does not allocate replacement resources only to close them immediately.
     */
    @Override
    public void finish() throws Exception {
        closing = true;
        if (exporter != null) {
            drainNative();
        }
        super.finish();
    }

    /**
     * Idempotent teardown without drain. Every step runs even when an earlier one failed; the
     * first failure propagates and later ones join it as suppressed exceptions. Steps in order:
     * UDF lifecycle stop (fast — in-flight evals see the stopped state), native runtime close,
     * UDF lifecycle close (full cleanup incl. registry removal), exporter close, child allocator
     * close, {@code super.close()}. Each resource is null-guarded so {@code close()} is safe
     * after a partial {@link #open()} (e.g. a UDF bind failure) and after an earlier {@code
     * close()}.
     */
    @Override
    public void close() throws Exception {
        Throwable failure = null;
        try {
            stopUdfLifecycle();
        } catch (Throwable error) {
            failure = error;
        }
        try {
            if (nativeRuntime != null) {
                NativeRuntime runtime = nativeRuntime;
                nativeRuntime = null;
                runtime.close();
            }
        } catch (Throwable error) {
            failure = suppress(failure, error);
        }
        try {
            if (udfLifecycle != null) {
                FlinkUdfLifecycle lifecycle = udfLifecycle;
                udfLifecycle = null;
                lifecycle.close();
            }
        } catch (Throwable error) {
            failure = suppress(failure, error);
        }
        try {
            if (exporter != null) {
                FlinkArrowFFIExporter openExporter = exporter;
                exporter = null;
                openExporter.close();
            }
        } catch (Throwable error) {
            failure = suppress(failure, error);
        }
        try {
            if (childAllocator != null) {
                BufferAllocator allocator = childAllocator;
                childAllocator = null;
                allocator.close();
            }
        } catch (Throwable error) {
            failure = suppress(failure, error);
        }
        try {
            super.close();
        } catch (Throwable error) {
            failure = suppress(failure, error);
        }
        if (failure instanceof Exception) {
            throw (Exception) failure;
        }
        if (failure != null) {
            throw (Error) failure;
        }
    }

    /**
     * Drains all output of the current cycle, then rebuilds the exporter + native runtime for
     * the next cycle. Each batch is emitted via {@link #emitArrowBatch(VectorSchemaRoot)}.
     *
     * <p>Early-returns when the exporter has no buffered rows: a drain on an empty exporter
     * would produce zero output and still tear down the wrapper (because
     * {@link FlinkArrowFFIExporter#exportNextBatch(long)} returns false on empty, and
     * {@link org.apache.auron.jni.AuronCallNativeWrapper#loadNextBatch} auto-closes the
     * native runtime on a false return). Skipping the round trip keeps the wrapper alive
     * across no-op watermark/checkpoint drains.
     *
     * <p>After a non-empty drain, the underlying {@code AuronCallNativeWrapper} has auto-closed
     * itself, and the native FFI Reader's drop has already called
     * {@link FlinkArrowFFIExporter#close()} on the Java exporter. Without per-cycle reinit, the
     * next {@code processElement} would either silently drop rows (the wrapper's invalid native
     * pointer fast-fails {@code loadNextBatch}) or NPE on {@code exporter.offer(row)} (writer
     * nulled by the FFI Reader's drop). Reinit is suppressed during operator {@link #finish()}
     * and the drain it performs via the {@link #closing} guard so the final drain does not
     * allocate replacement resources only to tear them down.
     *
     * <p>A pipeline failure mid-drain (e.g. a UDF eval error propagating up through JNI) first
     * stops the UDF lifecycle so in-flight evals fail fast, then rethrows — the operator
     * surfaces the failure and {@link #close()} completes the teardown.
     */
    private void drainNative() {
        if (exporter.isEmpty()) {
            return;
        }
        try {
            if (nativeRuntime == null) {
                this.nativeRuntime = nativeRuntimeFactory.create(
                        FlinkArrowUtils.getRootAllocator(), runtimePlan, metricNode, nativeMemory);
            }
            while (nativeRuntime.loadNextBatch(this::emitArrowBatch)) {
                // loop
            }
        } catch (RuntimeException | Error e) {
            stopUdfLifecycle();
            throw e;
        }
        if (!closing) {
            reinitExporterAndRuntime();
        }
    }

    /**
     * Re-prepares the exporter and re-registers it under {@link #resourceId} so the next drain
     * cycle can buffer rows. The native runtime is intentionally left {@code null}: the next
     * {@link #drainNative()} call constructs a fresh one once the exporter has rows, so the
     * tokio batch producer never starts against an empty buffer (which would otherwise short the
     * FFI Reader's read loop on its very first pull and close the exporter mid-cycle).
     */
    private void reinitExporterAndRuntime() {
        exporter.reset();
        JniBridge.putResource(resourceId, exporter);
        this.nativeRuntime = null;
    }

    /**
     * Binds every UDF payload embedded in {@link #runtimePlan}: decodes the payloads (in
     * traversal order of {@link AuronPlanTreeRewriter#collectUdfPayloads}), opens one lifecycle
     * owning every UDF instance of this plan, then rewrites the wrapper nodes from serialized
     * payloads to the returned registry tokens (same traversal order, so position {@code i} of
     * the tokens matches the wrapper at traversal position {@code i}). A plan without UDF
     * payloads leaves this operator UDF-less — no lifecycle object, no registry entries.
     *
     * <p>The lifecycle field is assigned BEFORE {@link FlinkUdfLifecycle#openAndBind} so a
     * failure inside the bind still leaves a closeable lifecycle for {@link #close()} to tear
     * down (openAndBind also rolls back internally on failure; the field guard keeps close()
     * safe regardless of which step threw).
     */
    private void bindUdfLifecycle() throws Exception {
        // List<FlinkUdfPayload> payloads = AuronPlanTreeRewriter.collectUdfPayloads(runtimePlan);
        // if (payloads.isEmpty()) {
        //     return;
        // }
        // this.udfLifecycle = new FlinkUdfLifecycle();
        // List<FlinkUdfToken> tokens = udfLifecycle.openAndBind(
        //         payloads,
        //         new FunctionContext(getRuntimeContext()),
        //         getRuntimeContext().getUserCodeClassLoader());
        // this.runtimePlan = AuronPlanTreeRewriter.rewriteUdfTokens(runtimePlan, tokens);

        List<FlinkUdfGroup> groups = AuronPlanTreeRewriter.collectUdfPayloadGroups(this.runtimePlan);
        if (groups.isEmpty()) {
            return;
        }

        this.udfLifecycle = new FlinkUdfLifecycle();

        for (FlinkUdfGroup group : groups) {
            List<FlinkUdfToken> tokens = udfLifecycle.openAndBind(
                    group.payloads(),
                    new FunctionContext(getRuntimeContext()),
                    getRuntimeContext().getUserCodeClassLoader());

            MergedUDFWrapperContext context = new MergedUDFWrapperContext(tokens);

            FlinkUdfToken groupToken = udfLifecycle.bind(context);
            group.bindToken(groupToken);

            this.runtimePlan = AuronPlanTreeRewriter.rewriteGroupedUdfTokens(this.runtimePlan, group);
        }
    }

    /** Stops the UDF lifecycle (idempotent) so in-flight and future evals fail fast. */
    private void stopUdfLifecycle() {
        if (udfLifecycle != null) {
            udfLifecycle.stop();
        }
    }

    /**
     * Joins {@code error} onto the first {@code failure} (as suppressed when both present) so
     * {@link #close()} can run every teardown step without masking the original exception.
     */
    private static Throwable suppress(Throwable failure, Throwable error) {
        if (failure == null) {
            return error;
        }
        if (failure != error) {
            failure.addSuppressed(error);
        }
        return failure;
    }

    /**
     * Wraps the Arrow batch as a Flink {@link FlinkArrowReader} and emits each row downstream
     * through the inherited {@code output} collector.
     */
    private void emitArrowBatch(VectorSchemaRoot root) {
        FlinkArrowReader reader = FlinkArrowReader.create(root, outputRowType);
        int n = reader.getRowCount();
        for (int i = 0; i < n; i++) {
            output.collect(outputRecord.replace(reader.read(i)));
        }
    }

    /**
     * Recursively walks the plan tree, locates the {@link FFIReaderExecNode} leaf, and rebuilds
     * the path so that the leaf has the runtime-bound resource ID. All other FFI Reader fields
     * ({@code num_partitions}, {@code schema}) are preserved.
     *
     * <p>Accepted shapes: {@code FFIReader}, {@code Filter[FFIReader]}, {@code Project[FFIReader]},
     * {@code Project[Filter[FFIReader]]}. Throws {@link IllegalArgumentException} on anything
     * else so misconfigurations fail fast at operator startup.
     *
     * @param node the plan node to walk
     * @param resourceId the resource ID to inject into the FFI Reader leaf
     * @return a new plan tree with the resource ID bound on the leaf
     */
    @VisibleForTesting
    static PhysicalPlanNode injectFfiReaderLeaf(PhysicalPlanNode node, String resourceId) {
        return AuronPlanTreeRewriter.rewriteFfiReaderLeaf(
                node,
                leaf -> leaf.toBuilder()
                        .setFfiReader(leaf.getFfiReader().toBuilder()
                                .setExportIterProviderResourceId(resourceId)
                                .build())
                        .build(),
                "FlinkAuronCalcOperator expects Project[Filter[FFIReader]] / "
                        + "Project[FFIReader] / Filter[FFIReader] / FFIReader shape; got: ");
    }

    /**
     * Recursively constructs a {@link FlinkMetricNode} whose shape mirrors {@code node}'s plan
     * tree. Native code walks the plan tree at finalization time and indexes into the parallel
     * metric tree via {@link org.apache.auron.metric.MetricNode#getChild(int)}; an empty children
     * list would throw {@link IndexOutOfBoundsException} on the first {@code getChild(0)} call.
     * All levels share the same Flink {@link org.apache.flink.metrics.MetricGroup} so named
     * counters aggregate at the operator scope.
     */
    private static FlinkMetricNode buildMetricTree(PhysicalPlanNode node, org.apache.flink.metrics.MetricGroup mg) {
        final List<org.apache.auron.metric.MetricNode> children;
        if (node.hasFfiReader()) {
            children = Collections.emptyList();
        } else if (node.hasProjection()) {
            children = Collections.singletonList(
                    buildMetricTree(node.getProjection().getInput(), mg));
        } else if (node.hasFilter()) {
            children =
                    Collections.singletonList(buildMetricTree(node.getFilter().getInput(), mg));
        } else {
            throw new IllegalArgumentException(
                    "Unexpected plan node type for metric tree: " + node.getPhysicalPlanTypeCase());
        }
        return new FlinkMetricNode(mg, children);
    }

    // ====================================================================
    // SupportsAuronNative
    // ====================================================================

    @Override
    public List<PhysicalPlanNode> getPhysicalPlanNodes() {
        return Collections.singletonList(plan);
    }

    @Override
    public RowType getOutputType() {
        return outputRowType;
    }

    @Override
    public String getAuronOperatorId() {
        return auronOperatorId;
    }

    @Override
    public MetricNode getMetricNode() {
        return metricNode;
    }

    // ====================================================================
    // Mock seam
    // ====================================================================

    /**
     * Test seam abstracting the native runtime so unit tests can drive the operator without
     * loading {@code libauron} or invoking real JNI.
     */
    @VisibleForTesting
    interface NativeRuntime extends AutoCloseable {
        /**
         * Pulls the next batch from the native engine and passes it to {@code consumer}.
         *
         * @param consumer callback that receives the next batch's {@link VectorSchemaRoot}
         * @return {@code true} if a batch was produced (consumer was called), {@code false} if
         *     the stream is exhausted
         */
        boolean loadNextBatch(Consumer<VectorSchemaRoot> consumer);

        @Override
        void close();
    }

    /**
     * Factory for {@link NativeRuntime} instances, swappable in tests. Extends {@link
     * java.io.Serializable} because the field that holds an instance is part of the operator and
     * Flink serializes operators to dispatch them to TaskManagers.
     */
    @VisibleForTesting
    interface NativeRuntimeFactory extends java.io.Serializable {
        /**
         * Creates a native runtime for the given plan.
         *
         * @param allocator the Arrow root allocator
         * @param plan the runtime-bound plan (with resource IDs injected)
         * @param metric the metric node to forward native engine metrics to
         * @param nativeMemory the native memory budget for the runtime in bytes
         * @return a new {@link NativeRuntime}
         */
        NativeRuntime create(BufferAllocator allocator, PhysicalPlanNode plan, MetricNode metric, long nativeMemory);
    }

    private static NativeRuntimeFactory defaultNativeRuntimeFactory() {
        return (allocator, plan, metric, mem) -> {
            AuronCallNativeWrapper wrapper = new AuronCallNativeWrapper(allocator, plan, metric, 0, 0, 0, mem);
            return new NativeRuntime() {
                @Override
                public boolean loadNextBatch(Consumer<VectorSchemaRoot> consumer) {
                    return wrapper.loadNextBatch(consumer);
                }

                @Override
                public void close() {
                    wrapper.close();
                }
            };
        };
    }
}
