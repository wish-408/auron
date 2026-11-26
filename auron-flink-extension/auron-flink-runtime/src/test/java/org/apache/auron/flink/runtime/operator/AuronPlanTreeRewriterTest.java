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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.auron.flink.table.function.FlinkUdfPayload;
import org.apache.auron.flink.table.function.FlinkUdfToken;
import org.apache.auron.protobuf.ArrowType;
import org.apache.auron.protobuf.EmptyMessage;
import org.apache.auron.protobuf.FFIReaderExecNode;
import org.apache.auron.protobuf.FilterExecNode;
import org.apache.auron.protobuf.KafkaScanExecNode;
import org.apache.auron.protobuf.LimitExecNode;
import org.apache.auron.protobuf.PhysicalBinaryExprNode;
import org.apache.auron.protobuf.PhysicalColumn;
import org.apache.auron.protobuf.PhysicalExprNode;
import org.apache.auron.protobuf.PhysicalPlanNode;
import org.apache.auron.protobuf.PhysicalSCAndExprNode;
import org.apache.auron.protobuf.PhysicalSparkUDFWrapperExprNode;
import org.apache.auron.protobuf.ProjectionExecNode;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the shared {@link AuronPlanTreeRewriter} used by both the standalone native Calc
 * operator and the Kafka source's Calc fusion to walk a {@code Project[Filter?[FFIReader]]} tree
 * down to its FFI Reader leaf.
 */
class AuronPlanTreeRewriterTest {

    private static PhysicalPlanNode ffiReader() {
        return PhysicalPlanNode.newBuilder()
                .setFfiReader(FFIReaderExecNode.newBuilder()
                        .setExportIterProviderResourceId("placeholder")
                        .build())
                .build();
    }

    private static PhysicalPlanNode kafkaScan() {
        return PhysicalPlanNode.newBuilder()
                .setKafkaScan(KafkaScanExecNode.newBuilder().setKafkaTopic("t").build())
                .build();
    }

    private static PhysicalPlanNode project(PhysicalPlanNode input) {
        return PhysicalPlanNode.newBuilder()
                .setProjection(ProjectionExecNode.newBuilder().setInput(input).build())
                .build();
    }

    private static PhysicalPlanNode filter(PhysicalPlanNode input) {
        return PhysicalPlanNode.newBuilder()
                .setFilter(FilterExecNode.newBuilder().setInput(input).build())
                .build();
    }

    @Test
    void replacesLeafUnderProjection() {
        PhysicalPlanNode out =
                AuronPlanTreeRewriter.rewriteFfiReaderLeaf(project(ffiReader()), leaf -> kafkaScan(), "bad: ");

        assertTrue(out.hasProjection());
        assertTrue(out.getProjection().getInput().hasKafkaScan());
    }

    @Test
    void replacesLeafUnderProjectionFilter() {
        PhysicalPlanNode out =
                AuronPlanTreeRewriter.rewriteFfiReaderLeaf(project(filter(ffiReader())), leaf -> kafkaScan(), "bad: ");

        assertTrue(out.hasProjection());
        assertTrue(out.getProjection().getInput().hasFilter());
        assertTrue(out.getProjection().getInput().getFilter().getInput().hasKafkaScan());
    }

    @Test
    void appliesLeafTransformToFfiReaderBearingNode() {
        // The transform receives the FFI Reader-bearing node, so an in-place edit (the Calc
        // operator's resource-ID injection) sees the FFI Reader and can rebuild it.
        PhysicalPlanNode out = AuronPlanTreeRewriter.rewriteFfiReaderLeaf(
                project(ffiReader()),
                leaf -> leaf.toBuilder()
                        .setFfiReader(leaf.getFfiReader().toBuilder()
                                .setExportIterProviderResourceId("rid")
                                .build())
                        .build(),
                "bad: ");

        assertEquals("rid", out.getProjection().getInput().getFfiReader().getExportIterProviderResourceId());
    }

    @Test
    void throwsWithMessagePrefixOnUnsupportedShape() {
        PhysicalPlanNode unsupported = PhysicalPlanNode.newBuilder()
                .setLimit(LimitExecNode.newBuilder().setLimit(1).build())
                .build();

        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> AuronPlanTreeRewriter.rewriteFfiReaderLeaf(unsupported, leaf -> leaf, "unexpected shape: "));
        assertTrue(ex.getMessage().startsWith("unexpected shape: "));
        assertTrue(ex.getMessage().contains("LIMIT"));
    }

    // ---- UDF wrapper collect / token rewrite ----

    @Test
    void udfGroupCollectReturnsEmptyForPlanWithoutWrappers() {
        PhysicalPlanNode plan = projectWithExprs(project(ffiReader()), column("a"), column("b"));

        assertTrue(AuronPlanTreeRewriter.collectUdfPayloadGroups(plan).isEmpty());
    }

    @Test
    void udfGroupCollectMergesStrictChainAndKeepsSeparateWrapperAsItsOwnGroup() {
        PhysicalExprNode inner = udfWrapper("calc:s", 0, intArrowType(), false, "udf1", column("a"));
        PhysicalExprNode middle = udfWrapper("calc:s", 1, intArrowType(), false, "udf2", inner);
        PhysicalExprNode outer = udfWrapper("calc:s", 2, utf8ArrowType(), true, "udf3", middle);
        PhysicalExprNode separate = udfWrapper("calc:s", 3, boolArrowType(), false, "udf4", column("b"));
        PhysicalPlanNode plan = projectWithExprs(ffiReader(), outer, separate);

        List<AuronPlanTreeRewriter.FlinkUdfGroup> groups = AuronPlanTreeRewriter.collectUdfPayloadGroups(plan);

        assertEquals(2, groups.size());
        assertEquals(
                Arrays.asList(0L, 1L, 2L),
                Arrays.asList(
                        groups.get(0).payloads().get(0).getRegistrationOrder(),
                        groups.get(0).payloads().get(1).getRegistrationOrder(),
                        groups.get(0).payloads().get(2).getRegistrationOrder()));
        assertEquals(3, groups.get(0).wrappers().size());
        assertEquals(1, groups.get(1).payloads().size());
        assertEquals(3L, groups.get(1).payloads().get(0).getRegistrationOrder());
    }

    @Test
    void udfGroupCollectDoesNotMergeWrapperSeparatedByNativeExpression() {
        PhysicalExprNode inner = udfWrapper("calc:s", 0, intArrowType(), false, "udf1", column("a"));
        PhysicalExprNode plus = binaryExpr(inner, column("one"));
        PhysicalExprNode outer = udfWrapper("calc:s", 1, intArrowType(), false, "udf2", plus);
        PhysicalPlanNode plan = projectWithExprs(ffiReader(), outer);

        List<AuronPlanTreeRewriter.FlinkUdfGroup> groups = AuronPlanTreeRewriter.collectUdfPayloadGroups(plan);

        assertEquals(2, groups.size());
        assertEquals(0L, groups.get(0).payloads().get(0).getRegistrationOrder());
        assertEquals(1, groups.get(0).payloads().size());
        assertEquals(1L, groups.get(1).payloads().get(0).getRegistrationOrder());
        assertEquals(1, groups.get(1).payloads().size());
    }

    @Test
    void udfGroupRewriteCollapsesChainToSingleWrapperWithGroupToken() {
        PhysicalExprNode inner = udfWrapper("calc:s", 0, intArrowType(), false, "udf1", column("x"));
        PhysicalExprNode middle = udfWrapper("calc:s", 1, intArrowType(), false, "udf2", inner);
        PhysicalExprNode outer = udfWrapper("calc:s", 2, utf8ArrowType(), true, "udf3", middle);
        PhysicalPlanNode plan = projectWithExprs(ffiReader(), outer);

        List<AuronPlanTreeRewriter.FlinkUdfGroup> groups = AuronPlanTreeRewriter.collectUdfPayloadGroups(plan);
        assertEquals(1, groups.size());
        AuronPlanTreeRewriter.FlinkUdfGroup group = groups.get(0);

        FlinkUdfToken token = new FlinkUdfToken(UUID.randomUUID(), 99);
        group.bindToken(token);

        PhysicalPlanNode rewritten = AuronPlanTreeRewriter.rewriteGroupedUdfTokens(plan, group);

        PhysicalSparkUDFWrapperExprNode wrapper =
                rewritten.getProjection().getExpr(0).getSparkUdfWrapperExpr();
        assertEquals(
                token,
                FlinkUdfToken.decode(ByteBuffer.wrap(wrapper.getSerialized().toByteArray())));
        assertEquals(utf8ArrowType(), wrapper.getReturnType());
        assertTrue(wrapper.getReturnNullable());
        assertEquals(1, wrapper.getParamsCount());
        assertEquals(group.wrappers().get(0).getParams(0), wrapper.getParams(0));
        assertEquals("udf3", wrapper.getExprString());

        // The original plan is immutable: the caller can retry or inspect it after rewrite.
        PhysicalSparkUDFWrapperExprNode originalOuter =
                plan.getProjection().getExpr(0).getSparkUdfWrapperExpr();
        assertNotEquals(originalOuter, wrapper);
        assertEquals(1, originalOuter.getParamsCount());
        assertTrue(originalOuter.getParams(0).hasSparkUdfWrapperExpr());
    }

    @Test
    void udfGroupRewriteThrowsWhenGroupTokenIsNotBound() {
        PhysicalExprNode inner = udfWrapper("calc:s", 0, intArrowType(), false, "udf1", column("a"));
        PhysicalExprNode outer = udfWrapper("calc:s", 1, intArrowType(), false, "udf2", inner);
        PhysicalPlanNode plan = projectWithExprs(ffiReader(), outer);

        List<AuronPlanTreeRewriter.FlinkUdfGroup> groups = AuronPlanTreeRewriter.collectUdfPayloadGroups(plan);
        assertEquals(1, groups.size());

        IllegalStateException ex = assertThrows(
                IllegalStateException.class, () -> AuronPlanTreeRewriter.rewriteGroupedUdfTokens(plan, groups.get(0)));
        assertTrue(ex.getMessage().contains("尚未绑定 token"));
    }

    @Test
    void udfCollectReturnsEmptyForPlanWithoutWrappers() {
        PhysicalPlanNode plan = projectWithExprs(project(ffiReader()), column("a"), column("b"));

        assertTrue(AuronPlanTreeRewriter.collectUdfPayloads(plan).isEmpty());
        // A no-op rewrite with an empty token list rebuilds the same plan.
        assertEquals(plan, AuronPlanTreeRewriter.rewriteUdfTokens(plan, new ArrayList<>()));
    }

    @Test
    void udfCollectAndRewriteShareOneTraversalOrder() {
        PhysicalExprNode first = udfWrapper("calc:s", 0, column("a"));
        PhysicalExprNode inner = udfWrapper("calc:s", 1, column("b"));
        PhysicalExprNode binary = binaryExpr(column("c"), inner);
        PhysicalPlanNode plan = projectWithExprs(ffiReader(), first, binary);

        List<FlinkUdfPayload> payloads = AuronPlanTreeRewriter.collectUdfPayloads(plan);
        assertEquals(2, payloads.size());
        assertEquals(0L, payloads.get(0).getRegistrationOrder());
        assertEquals(1L, payloads.get(1).getRegistrationOrder());
        assertEquals("calc:s", payloads.get(0).getScope());

        UUID owner = UUID.randomUUID();
        List<FlinkUdfToken> tokens = Arrays.asList(new FlinkUdfToken(owner, 0), new FlinkUdfToken(owner, 1));
        PhysicalPlanNode rewritten = AuronPlanTreeRewriter.rewriteUdfTokens(plan, tokens);

        ProjectionExecNode projection = rewritten.getProjection();
        // Position 0 wrapper was rewritten with token 0 and kept its param.
        PhysicalSparkUDFWrapperExprNode firstWrapper = projection.getExpr(0).getSparkUdfWrapperExpr();
        assertEquals(
                tokens.get(0),
                FlinkUdfToken.decode(
                        ByteBuffer.wrap(firstWrapper.getSerialized().toByteArray())));
        assertTrue(firstWrapper.getParams(0).hasColumn());
        // Position 1: the binary survived unchanged on the L branch and its R branch wrapper
        // (visited second by both passes) received token 1.
        PhysicalExprNode binaryNode = projection.getExpr(1);
        assertTrue(binaryNode.getBinaryExpr().getL().hasColumn());
        assertEquals(
                tokens.get(1),
                FlinkUdfToken.decode(ByteBuffer.wrap(binaryNode
                        .getBinaryExpr()
                        .getR()
                        .getSparkUdfWrapperExpr()
                        .getSerialized()
                        .toByteArray())));
        // The spine under the expressions is untouched.
        assertTrue(projection.getInput().hasFfiReader());
    }

    @Test
    void udfTraversalVisitsNestedWrapperParamsBeforeTheOuterWrapper() {
        // A wrapper whose param is another wrapper: the inner call re-binds first (it converts
        // first), so the collect order is inner-then-outer in both passes.
        PhysicalExprNode inner = udfWrapper("calc:s", 0, column("b"));
        PhysicalExprNode outer = udfWrapper("calc:s", 1, inner);
        PhysicalPlanNode plan = projectWithExprs(ffiReader(), outer);

        List<FlinkUdfPayload> payloads = AuronPlanTreeRewriter.collectUdfPayloads(plan);
        assertEquals(2, payloads.size());
        assertEquals(0L, payloads.get(0).getRegistrationOrder());
        assertEquals(1L, payloads.get(1).getRegistrationOrder());

        UUID owner = UUID.randomUUID();
        PhysicalPlanNode rewritten = AuronPlanTreeRewriter.rewriteUdfTokens(
                plan, Arrays.asList(new FlinkUdfToken(owner, 0), new FlinkUdfToken(owner, 1)));
        PhysicalSparkUDFWrapperExprNode outerWrapper =
                rewritten.getProjection().getExpr(0).getSparkUdfWrapperExpr();
        // Outer received token 1; its param (the inner wrapper) received token 0.
        assertEquals(
                new FlinkUdfToken(owner, 1),
                FlinkUdfToken.decode(
                        ByteBuffer.wrap(outerWrapper.getSerialized().toByteArray())));
        assertEquals(
                new FlinkUdfToken(owner, 0),
                FlinkUdfToken.decode(ByteBuffer.wrap(outerWrapper
                        .getParams(0)
                        .getSparkUdfWrapperExpr()
                        .getSerialized()
                        .toByteArray())));
    }

    @Test
    void udfRewriteThrowsWhenTokenListDoesNotCoverTheWrappers() {
        PhysicalPlanNode plan = projectWithExprs(
                ffiReader(), udfWrapper("calc:s", 0, column("a")), udfWrapper("calc:s", 1, column("b")));
        UUID owner = UUID.randomUUID();

        IllegalStateException tooFew = assertThrows(
                IllegalStateException.class,
                () -> AuronPlanTreeRewriter.rewriteUdfTokens(
                        plan, Collections.singletonList(new FlinkUdfToken(owner, 0))));
        assertTrue(tooFew.getMessage().contains("exhausted"));

        IllegalStateException tooMany = assertThrows(
                IllegalStateException.class,
                () -> AuronPlanTreeRewriter.rewriteUdfTokens(
                        plan,
                        Arrays.asList(
                                new FlinkUdfToken(owner, 0),
                                new FlinkUdfToken(owner, 1),
                                new FlinkUdfToken(owner, 2))));
        assertTrue(tooMany.getMessage().contains("unconsumed"));
    }

    @Test
    void udfTraversalThrowsOnUnknownExpressionType() {
        // An expression type the walk does not cover must fail fast: an unknown container could
        // otherwise hide a UDF payload from the binding pass.
        PhysicalExprNode unknown = PhysicalExprNode.newBuilder()
                .setScAndExpr(PhysicalSCAndExprNode.newBuilder().build())
                .build();
        PhysicalPlanNode plan = projectWithExprs(ffiReader(), unknown);

        IllegalArgumentException ex =
                assertThrows(IllegalArgumentException.class, () -> AuronPlanTreeRewriter.collectUdfPayloads(plan));
        assertTrue(ex.getMessage().contains("cannot traverse expression node"));
        assertTrue(ex.getMessage().contains("SC_AND_EXPR"));
    }

    @Test
    void udfTraversalThrowsOnUnknownPlanNodeType() {
        PhysicalPlanNode limit = PhysicalPlanNode.newBuilder()
                .setLimit(LimitExecNode.newBuilder().setLimit(1).build())
                .build();
        PhysicalPlanNode plan = projectWithExprs(limit, column("a"));

        IllegalArgumentException ex =
                assertThrows(IllegalArgumentException.class, () -> AuronPlanTreeRewriter.collectUdfPayloads(plan));
        assertTrue(ex.getMessage().contains("expects Project[Filter[FFIReader]]"));
        assertTrue(ex.getMessage().contains("LIMIT"));
    }

    @Test
    void udfCollectThrowsOnInvalidPayloadBytes() {
        // Bytes that are not a versioned FlinkUdfPayload must not survive as opaque serialized
        // state — they fail the collect pass instead of reaching native execution.
        PhysicalExprNode garbage = PhysicalExprNode.newBuilder()
                .setSparkUdfWrapperExpr(PhysicalSparkUDFWrapperExprNode.newBuilder()
                        .setSerialized(ByteString.copyFrom(new byte[] {1, 2, 3}))
                        .build())
                .build();
        PhysicalPlanNode plan = projectWithExprs(ffiReader(), garbage);

        IllegalArgumentException ex =
                assertThrows(IllegalArgumentException.class, () -> AuronPlanTreeRewriter.collectUdfPayloads(plan));
        assertTrue(ex.getMessage().contains("payload"));
    }

    // ---- UDF wrapper helpers ----

    private static PhysicalPlanNode projectWithExprs(PhysicalPlanNode input, PhysicalExprNode... exprs) {
        ProjectionExecNode.Builder builder = ProjectionExecNode.newBuilder().setInput(input);
        for (PhysicalExprNode expr : exprs) {
            builder.addExpr(expr);
        }
        return PhysicalPlanNode.newBuilder().setProjection(builder.build()).build();
    }

    private static PhysicalExprNode column(String name) {
        return PhysicalExprNode.newBuilder()
                .setColumn(PhysicalColumn.newBuilder().setName(name).build())
                .build();
    }

    private static PhysicalExprNode binaryExpr(PhysicalExprNode left, PhysicalExprNode right) {
        return PhysicalExprNode.newBuilder()
                .setBinaryExpr(PhysicalBinaryExprNode.newBuilder()
                        .setL(left)
                        .setR(right)
                        .build())
                .build();
    }

    private static PhysicalExprNode udfWrapper(String scope, long order, PhysicalExprNode... params) {
        return udfWrapper(scope, order, intArrowType(), false, "TestUdf", params);
    }

    private static PhysicalExprNode udfWrapper(
            String scope,
            long order,
            ArrowType returnType,
            boolean returnNullable,
            String exprString,
            PhysicalExprNode... params) {
        PhysicalSparkUDFWrapperExprNode.Builder wrapper = PhysicalSparkUDFWrapperExprNode.newBuilder()
                .setSerialized(
                        ByteString.copyFrom(new FlinkUdfPayload(scope, order, "TestUdf", new byte[] {1}).encode()));
        wrapper.setReturnType(returnType);
        wrapper.setReturnNullable(returnNullable);
        wrapper.setExprString(exprString);
        for (PhysicalExprNode param : params) {
            wrapper.addParams(param);
        }
        return PhysicalExprNode.newBuilder()
                .setSparkUdfWrapperExpr(wrapper.build())
                .build();
    }

    private static ArrowType intArrowType() {
        return ArrowType.newBuilder()
                .setINT32(EmptyMessage.getDefaultInstance())
                .build();
    }

    private static ArrowType utf8ArrowType() {
        return ArrowType.newBuilder().setUTF8(EmptyMessage.getDefaultInstance()).build();
    }

    private static ArrowType boolArrowType() {
        return ArrowType.newBuilder().setBOOL(EmptyMessage.getDefaultInstance()).build();
    }
}
