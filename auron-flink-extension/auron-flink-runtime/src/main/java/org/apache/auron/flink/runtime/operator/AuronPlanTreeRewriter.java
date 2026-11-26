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

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.UnaryOperator;
import org.apache.auron.flink.table.function.FlinkUdfPayload;
import org.apache.auron.flink.table.function.FlinkUdfToken;
import org.apache.auron.protobuf.FilterExecNode;
import org.apache.auron.protobuf.PhysicalBinaryExprNode;
import org.apache.auron.protobuf.PhysicalCaseNode;
import org.apache.auron.protobuf.PhysicalExprNode;
import org.apache.auron.protobuf.PhysicalPlanNode;
import org.apache.auron.protobuf.PhysicalScalarFunctionNode;
import org.apache.auron.protobuf.PhysicalSparkUDFWrapperExprNode;
import org.apache.auron.protobuf.PhysicalWhenThen;
import org.apache.auron.protobuf.ProjectionExecNode;

/**
 * Shared rewriter for native Calc plan trees of shape {@code Project[Filter?[FFIReader]]}.
 *
 * <p>Both the standalone native Calc operator (which injects a runtime resource ID into the FFI
 * Reader leaf) and the Kafka source's Calc fusion (which replaces the FFI Reader placeholder leaf
 * with a {@code KafkaScan}) walk the same Project/Filter spine down to the FFI Reader leaf and
 * rebuild the path around a transformed leaf. This helper captures that single walk, parameterized
 * by the leaf transform.
 *
 * <p>The second walk, {@link #collectUdfPayloads} / {@link #rewriteUdfTokens}, traverses every
 * nested expression of the Project/Filter spine (binary and unary wrappers, CASE branches,
 * casts, like terms, ext-function arguments, and nested UDF wrapper parameters) so UDF wrapper
 * nodes can be discovered and re-bound without touching params, return types, or the FFI Reader
 * leaf rewrite above.
 */
public final class AuronPlanTreeRewriter {
    public static final class FlinkUdfGroup {
        private final String groupId;
        private final List<FlinkUdfPayload> payloads;
        private final List<PhysicalSparkUDFWrapperExprNode> wrappers;
        private FlinkUdfToken token;

        public FlinkUdfGroup(String groupId) {
            this.groupId = groupId;
            this.payloads = new ArrayList<FlinkUdfPayload>();
            this.wrappers = new ArrayList<PhysicalSparkUDFWrapperExprNode>();
            this.token = null;
        }

        public String groupId() {
            return this.groupId;
        }

        public void bindToken(FlinkUdfToken token) {
            this.token = token;
        }

        public Optional<FlinkUdfToken> token() {
            return Optional.ofNullable(this.token);
        }

        public List<FlinkUdfPayload> payloads() {
            return this.payloads;
        }

        public List<PhysicalSparkUDFWrapperExprNode> wrappers() {
            return this.wrappers;
        }

        void add(PhysicalSparkUDFWrapperExprNode wrapper, FlinkUdfPayload payload) {
            wrappers.add(wrapper);
            payloads.add(payload);
        }
    }

    private AuronPlanTreeRewriter() {}

    /** Prefix of the {@link IllegalArgumentException} thrown on an unsupported plan node type. */
    private static final String UNEXPECTED_PLAN_SHAPE =
            "AuronPlanTreeRewriter expects Project[Filter[FFIReader]] / Project[FFIReader] / "
                    + "Filter[FFIReader] / FFIReader / KafkaScan shape; got: ";

    /** Prefix of the {@link IllegalArgumentException} thrown on an unsupported expression type. */
    private static final String UNEXPECTED_EXPR_TYPE =
            "AuronPlanTreeRewriter cannot traverse expression node for UDF binding; got: ";

    /**
     * Recursively walks {@code node} through {@code Projection} / {@code Filter} wrappers to the
     * {@code FFIReader} leaf, applies {@code leafTransform} to the FFI Reader-bearing node, and
     * rebuilds the wrappers around the transformed leaf. The transform may either edit the FFI
     * Reader in place (returning a node that still bears it) or replace it with a different node
     * (e.g. a {@code KafkaScan}).
     *
     * <p>Accepted shapes: {@code FFIReader}, {@code Filter[FFIReader]}, {@code Project[FFIReader]},
     * {@code Project[Filter[FFIReader]]}. Any other node type fails fast with an
     * {@link IllegalArgumentException} whose message starts with {@code unexpectedShapeMessage} so
     * misconfigurations surface at operator startup rather than as silent fallthrough.
     *
     * @param node the plan node to walk
     * @param leafTransform applied to the FFI Reader-bearing node to produce its replacement
     * @param unexpectedShapeMessage prefix for the {@link IllegalArgumentException} thrown on an
     *     unsupported node type; the offending {@code PhysicalPlanTypeCase} is appended
     * @return a new plan tree with the FFI Reader leaf transformed
     */
    public static PhysicalPlanNode rewriteFfiReaderLeaf(
            PhysicalPlanNode node, UnaryOperator<PhysicalPlanNode> leafTransform, String unexpectedShapeMessage) {
        if (node.hasFfiReader()) {
            return leafTransform.apply(node);
        }
        if (node.hasProjection()) {
            PhysicalPlanNode rewrittenInput =
                    rewriteFfiReaderLeaf(node.getProjection().getInput(), leafTransform, unexpectedShapeMessage);
            return node.toBuilder()
                    .setProjection(node.getProjection().toBuilder()
                            .setInput(rewrittenInput)
                            .build())
                    .build();
        }
        if (node.hasFilter()) {
            PhysicalPlanNode rewrittenInput =
                    rewriteFfiReaderLeaf(node.getFilter().getInput(), leafTransform, unexpectedShapeMessage);
            return node.toBuilder()
                    .setFilter(node.getFilter().toBuilder()
                            .setInput(rewrittenInput)
                            .build())
                    .build();
        }
        throw new IllegalArgumentException(unexpectedShapeMessage + node.getPhysicalPlanTypeCase());
    }

    /**
     * Decodes every UDF wrapper payload embedded in the plan's Project/Filter expressions, in
     * traversal order. The returned list's order is the contract with
     * {@link #rewriteUdfTokens}: position {@code i} of this list matches the token the rewrite
     * pass needs for the wrapper node it visits at traversal position {@code i} (identical
     * traversal code, hence identical order). A plan without UDF wrappers returns an empty list.
     *
     * @param plan the plan tree whose expressions are walked
     * @return the decoded payloads, in traversal order; empty when the plan carries no UDF
     * @throws IllegalArgumentException on a wrapper whose serialized bytes are not a valid
     *     {@link FlinkUdfPayload}, or on an expression/plan node type the walk does not cover —
     *     a UDF payload must never survive into native execution unbound
     */
    public static List<FlinkUdfPayload> collectUdfPayloads(PhysicalPlanNode plan) {
        List<FlinkUdfPayload> payloads = new ArrayList<>();
        transformUdfWrappers(plan, wrapper -> {
            payloads.add(FlinkUdfPayload.decode(
                    ByteBuffer.wrap(wrapper.getSerialized().toByteArray())));
            return wrapper;
        });
        return payloads;
    }

    /**
     * Replaces every UDF wrapper's serialized payload bytes with the encoded
     * {@link FlinkUdfToken} at the matching traversal position — the tokens returned by
     * {@link org.apache.auron.flink.table.function.FlinkUdfLifecycle#openAndBind} for the list
     * from {@link #collectUdfPayloads}. Params, return type, nullability, and every non-UDF node
     * are rebuilt unchanged.
     *
     * @param plan the plan tree whose expressions are re-bound
     * @param tokens the tokens bound for the payloads of this plan, in collectUdfPayloads order
     * @return a new plan tree with every wrapper's serialized bytes replaced by its token
     * @throws IllegalStateException if the token list does not exactly cover the wrappers the
     *     traversal finds (a mismatch means the plan changed between collect and rewrite)
     */
    public static PhysicalPlanNode rewriteUdfTokens(PhysicalPlanNode plan, List<FlinkUdfToken> tokens) {
        Iterator<FlinkUdfToken> remaining = tokens.iterator();
        PhysicalPlanNode rewritten = transformUdfWrappers(plan, wrapper -> wrapper.toBuilder()
                .setSerialized(ByteString.copyFrom(nextToken(remaining, wrapper).encode()))
                .build());
        if (remaining.hasNext()) {
            throw new IllegalStateException("UDF token list has unconsumed entries after plan rewrite");
        }
        return rewritten;
    }

    /** Pops the next token, failing with plan context when the token list runs short. */
    private static FlinkUdfToken nextToken(Iterator<FlinkUdfToken> remaining, Object wrapper) {
        try {
            return remaining.next();
        } catch (NoSuchElementException e) {
            throw new IllegalStateException("UDF token list exhausted while rewriting wrapper: " + wrapper, e);
        }
    }

    /**
     * Walks the Project/Filter spine and every nested expression, rebuilding the tree around
     * whatever {@code transform} does to each UDF wrapper node (wrapper parameters are recursed
     * before the transform sees the wrapper, so nested UDF calls re-bind first in both the
     * collect and the rewrite pass — this fixes the positional order the two passes share).
     */
    private static PhysicalPlanNode transformUdfWrappers(
            PhysicalPlanNode node, UnaryOperator<PhysicalSparkUDFWrapperExprNode> transform) {
        if (node.hasFfiReader() || node.hasKafkaScan()) {
            return node;
        }
        if (node.hasProjection()) {
            ProjectionExecNode.Builder projection = node.getProjection().toBuilder();
            for (int i = 0; i < projection.getExprCount(); i++) {
                projection.setExpr(i, transformExpr(projection.getExpr(i), transform));
            }
            projection.setInput(transformUdfWrappers(projection.getInput(), transform));
            return node.toBuilder().setProjection(projection.build()).build();
        }
        if (node.hasFilter()) {
            FilterExecNode.Builder filter = node.getFilter().toBuilder();
            for (int i = 0; i < filter.getExprCount(); i++) {
                filter.setExpr(i, transformExpr(filter.getExpr(i), transform));
            }
            filter.setInput(transformUdfWrappers(filter.getInput(), transform));
            return node.toBuilder().setFilter(filter.build()).build();
        }
        throw new IllegalArgumentException(UNEXPECTED_PLAN_SHAPE + node.getPhysicalPlanTypeCase());
    }

    /**
     * Recurses one expression node, rebuilding it with transformed UDF wrappers. Column and
     * literal leaves pass through; every container type the Flink Calc converter can produce is
     * covered; any other node type fails fast so an unknown container can never hide a payload
     * from the binding pass.
     */
    private static PhysicalExprNode transformExpr(
            PhysicalExprNode expr, UnaryOperator<PhysicalSparkUDFWrapperExprNode> transform) {
        if (expr.hasColumn() || expr.hasLiteral()) {
            return expr;
        }
        if (expr.hasBinaryExpr()) {
            PhysicalBinaryExprNode.Builder binary = expr.getBinaryExpr().toBuilder();
            binary.setL(transformExpr(binary.getL(), transform));
            binary.setR(transformExpr(binary.getR(), transform));
            return expr.toBuilder().setBinaryExpr(binary.build()).build();
        }
        if (expr.hasIsNullExpr()
                || expr.hasIsNotNullExpr()
                || expr.hasNotExpr()
                || expr.hasNegative()
                || expr.hasCast()
                || expr.hasTryCast()) {
            PhysicalExprNode child = transformExpr(singleChildOf(expr), transform);
            return withSingleChild(expr, child);
        }
        if (expr.hasCase()) {
            PhysicalCaseNode.Builder caseNode = expr.getCase().toBuilder();
            for (int i = 0; i < caseNode.getWhenThenExprCount(); i++) {
                PhysicalWhenThen whenThen = expr.getCase().getWhenThenExpr(i);
                caseNode.setWhenThenExpr(
                        i,
                        whenThen.toBuilder()
                                .setWhenExpr(transformExpr(whenThen.getWhenExpr(), transform))
                                .setThenExpr(transformExpr(whenThen.getThenExpr(), transform))
                                .build());
            }
            caseNode.setElseExpr(transformExpr(expr.getCase().getElseExpr(), transform));
            return expr.toBuilder().setCase(caseNode.build()).build();
        }
        if (expr.hasLikeExpr()) {
            return expr.toBuilder()
                    .setLikeExpr(expr.getLikeExpr().toBuilder()
                            .setExpr(transformExpr(expr.getLikeExpr().getExpr(), transform))
                            .setPattern(transformExpr(expr.getLikeExpr().getPattern(), transform))
                            .build())
                    .build();
        }
        if (expr.hasScalarFunction()) {
            PhysicalScalarFunctionNode.Builder scalarFunction = expr.getScalarFunction().toBuilder();
            for (int i = 0; i < scalarFunction.getArgsCount(); i++) {
                scalarFunction.setArgs(i, transformExpr(scalarFunction.getArgs(i), transform));
            }
            return expr.toBuilder().setScalarFunction(scalarFunction.build()).build();
        }
        if (expr.hasSparkUdfWrapperExpr()) {
            PhysicalSparkUDFWrapperExprNode.Builder wrapper = expr.getSparkUdfWrapperExpr().toBuilder();
            for (int i = 0; i < wrapper.getParamsCount(); i++) {
                wrapper.setParams(i, transformExpr(wrapper.getParams(i), transform));
            }
            return expr.toBuilder()
                    .setSparkUdfWrapperExpr(transform.apply(wrapper.build()))
                    .build();
        }
        throw new IllegalArgumentException(UNEXPECTED_EXPR_TYPE + expr.getExprTypeCase());
    }

    /** Returns the single child expression of a unary-style wrapper node. */
    private static PhysicalExprNode singleChildOf(PhysicalExprNode expr) {
        if (expr.hasIsNullExpr()) {
            return expr.getIsNullExpr().getExpr();
        }
        if (expr.hasIsNotNullExpr()) {
            return expr.getIsNotNullExpr().getExpr();
        }
        if (expr.hasNotExpr()) {
            return expr.getNotExpr().getExpr();
        }
        if (expr.hasNegative()) {
            return expr.getNegative().getExpr();
        }
        if (expr.hasCast()) {
            return expr.getCast().getExpr();
        }
        return expr.getTryCast().getExpr();
    }

    /** Rebuilds a unary-style wrapper node around the transformed child expression. */
    private static PhysicalExprNode withSingleChild(PhysicalExprNode expr, PhysicalExprNode child) {
        if (expr.hasIsNullExpr()) {
            return expr.toBuilder()
                    .setIsNullExpr(
                            expr.getIsNullExpr().toBuilder().setExpr(child).build())
                    .build();
        }
        if (expr.hasIsNotNullExpr()) {
            return expr.toBuilder()
                    .setIsNotNullExpr(
                            expr.getIsNotNullExpr().toBuilder().setExpr(child).build())
                    .build();
        }
        if (expr.hasNotExpr()) {
            return expr.toBuilder()
                    .setNotExpr(expr.getNotExpr().toBuilder().setExpr(child).build())
                    .build();
        }
        if (expr.hasNegative()) {
            return expr.toBuilder()
                    .setNegative(expr.getNegative().toBuilder().setExpr(child).build())
                    .build();
        }
        if (expr.hasCast()) {
            return expr.toBuilder()
                    .setCast(expr.getCast().toBuilder().setExpr(child).build())
                    .build();
        }
        return expr.toBuilder()
                .setTryCast(expr.getTryCast().toBuilder().setExpr(child).build())
                .build();
    }

    public static List<FlinkUdfGroup> collectUdfPayloadGroups(PhysicalPlanNode plan) {
        List<FlinkUdfGroup> groups = new ArrayList<>();
        IdentityHashMap<PhysicalSparkUDFWrapperExprNode, FlinkUdfGroup> groupByWrapper = new IdentityHashMap<>();

        transformUdfWrappers(plan, wrapper -> {
            FlinkUdfPayload payload = FlinkUdfPayload.decode(
                    ByteBuffer.wrap(wrapper.getSerialized().toByteArray()));

            if (wrapper.getParamsCount() == 1 && wrapper.getParams(0).hasSparkUdfWrapperExpr()) {
                PhysicalSparkUDFWrapperExprNode child = wrapper.getParams(0).getSparkUdfWrapperExpr();

                FlinkUdfGroup childGroup = groupByWrapper.get(child);
                if (childGroup != null) {
                    childGroup.add(wrapper, payload);
                    groupByWrapper.put(wrapper, childGroup);
                    return wrapper;
                }
            }

            FlinkUdfGroup group = new FlinkUdfGroup(Long.toString(payload.getRegistrationOrder()));
            group.add(wrapper, payload);
            groups.add(group);
            groupByWrapper.put(wrapper, group);

            return wrapper;
        });

        return groups;
    }

    public static PhysicalPlanNode rewriteGroupedUdfTokens(PhysicalPlanNode plan, FlinkUdfGroup group) {
        FlinkUdfToken token =
                group.token().orElseThrow(() -> new IllegalStateException("UDF group 尚未绑定 token: " + group.groupId()));

        PhysicalSparkUDFWrapperExprNode innermost = group.wrappers().get(0);
        PhysicalSparkUDFWrapperExprNode outermost =
                group.wrappers().get(group.wrappers().size() - 1);

        return transformUdfWrappers(plan, wrapper -> {
            if (!wrapper.equals(outermost)) {
                return wrapper;
            }

            return wrapper.toBuilder()
                    .setSerialized(ByteString.copyFrom(token.encode()))
                    .setReturnType(outermost.getReturnType())
                    .setReturnNullable(outermost.getReturnNullable())
                    .clearParams()
                    .addAllParams(innermost.getParamsList())
                    .setExprString(outermost.getExprString())
                    .build();
        });
    }
}
