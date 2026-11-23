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
package org.apache.auron.flink.table.planner.converter;

import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.auron.flink.arrow.FlinkArrowUtils;
import org.apache.auron.flink.table.function.FlinkUdfPayload;
import org.apache.auron.flink.utils.SchemaConverters;
import org.apache.auron.protobuf.PhysicalExprNode;
import org.apache.auron.protobuf.PhysicalSparkUDFWrapperExprNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.functions.utils.ScalarSqlFunction;
import org.apache.flink.table.planner.plan.utils.RexLiteralUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.LogicalTypeDataTypeConverter;

/**
 * Builds the native wrapper expression for a Calcite call to a Flink user-defined
 * {@link ScalarFunction}, recognized through its planner representation
 * {@link ScalarSqlFunction}.
 *
 * <p>The wrapper node leaves the engine-specific work to the runtime: its {@code serialized}
 * field carries a versioned {@link FlinkUdfPayload} whose call descriptor is the same
 * eight-item stream the runtime factory reads back (function copy, params/output Arrow schema
 * JSON, param/output row types, and per-argument literal configuration), and whose scope and
 * registration order let the operator's lifecycle share or isolate function instances the way
 * Flink's own codegen does.
 *
 * <p>Every converted call serializes its own deep copy of the configured function instance, so
 * two call sites of one function contribute two payloads; the lifecycle later resolves the
 * winning copy by registration order, mirroring native codegen's last-assignment-wins member
 * initialization.
 */
final class FlinkUdfCallConverter {

    private FlinkUdfCallConverter() {}

    /** Returns {@code true} if the call invokes a user-defined {@link ScalarFunction}. */
    static boolean isSupported(RexCall call) {
        return resolveFunction(call) != null;
    }

    /**
     * Converts a {@link ScalarSqlFunction} call into a native
     * {@link PhysicalSparkUDFWrapperExprNode} wrapped in a {@link PhysicalExprNode}.
     *
     * <p>Operands convert through the given factory (so nested UDF calls convert recursively and
     * share the same registration-order counter), and each operand's literal configuration is
     * captured in the descriptor exactly as the runtime helper factory expects it.
     *
     * @throws Exception if the function cannot be serialized or an operand cannot convert
     */
    static PhysicalExprNode convert(RexCall call, FlinkNodeConverterFactory factory, ConverterContext context)
            throws Exception {
        ScalarFunction function = resolveFunction(call);
        if (function == null) {
            throw new IllegalStateException("UDF call no longer resolves to a ScalarFunction: " + call);
        }

        List<RexNode> operands = call.getOperands();
        List<PhysicalExprNode> params = new ArrayList<>(operands.size());
        List<Field> paramFields = new ArrayList<>(operands.size());
        LogicalType[] paramTypes = new LogicalType[operands.size()];
        List<Boolean> argumentLiterals = new ArrayList<>(operands.size());
        List<Boolean> argumentNulls = new ArrayList<>(operands.size());
        List<Object> argumentValues = new ArrayList<>(operands.size());
        for (int i = 0; i < operands.size(); i++) {
            RexNode operand = operands.get(i);
            appendLiteralConfiguration(operand, argumentLiterals, argumentNulls, argumentValues);
            params.add(convertOperand(operand, factory, context));
            LogicalType logicalType = FlinkTypeFactory.toLogicalType(operand.getType());
            paramFields.add(FlinkArrowUtils.toArrowField(operand.toString(), logicalType));
            paramTypes[i] = logicalType;
        }

        LogicalType outputType = FlinkTypeFactory.toLogicalType(call.getType());
        byte[] descriptor = serializeCallDescriptor(
                function,
                new Schema(paramFields),
                new Schema(Collections.singletonList(FlinkArrowUtils.toArrowField("", outputType))),
                RowType.of(paramTypes),
                RowType.of(outputType),
                argumentLiterals,
                argumentNulls,
                argumentValues);
        FlinkUdfPayload payload = new FlinkUdfPayload(
                context.getUdfScope(), context.nextUdfRegistrationOrder(), function.functionIdentifier(), descriptor);

        return PhysicalExprNode.newBuilder()
                .setSparkUdfWrapperExpr(PhysicalSparkUDFWrapperExprNode.newBuilder()
                        .setSerialized(ByteString.copyFrom(payload.encode()))
                        .setReturnType(SchemaConverters.convertToAuronArrowType(outputType))
                        .setReturnNullable(call.getType().isNullable())
                        .addAllParams(params)
                        .setExprString(call.toString()))
                .build();
    }

    /**
     * Appends one operand's literal configuration: literal operands record their externalized
     * value (internal value converted through the DataFormat converter for the literal's type),
     * null literals record a null, and non-literal operands record no value — the runtime binds
     * those columns from the evaluated parameter batch.
     */
    private static void appendLiteralConfiguration(
            RexNode operand, List<Boolean> argumentLiterals, List<Boolean> argumentNulls, List<Object> argumentValues) {
        if (operand instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) operand;
            argumentLiterals.add(true);
            if (literal.isNull()) {
                argumentNulls.add(true);
                argumentValues.add(null);
                return;
            }
            LogicalType logicalType = FlinkTypeFactory.toLogicalType(literal.getType());
            DataFormatConverters.DataFormatConverter converter =
                    DataFormatConverters.getConverterForDataType(LogicalTypeDataTypeConverter.toDataType(logicalType));
            argumentValues.add(converter.toExternal(
                    RexLiteralUtil.toFlinkInternalValue(literal.getValueAs(Comparable.class), logicalType)));
            argumentNulls.add(false);
        } else {
            argumentLiterals.add(false);
            argumentNulls.add(false);
            argumentValues.add(null);
        }
    }

    /**
     * Serializes the eight call-descriptor items in the fixed order the runtime factory reads
     * them back: function copy, params schema JSON, output schema JSON, param row type, output
     * row type, argument literal flags, argument null flags, argument values.
     */
    private static byte[] serializeCallDescriptor(
            ScalarFunction function,
            Schema paramsSchema,
            Schema outputSchema,
            RowType paramRowType,
            RowType outputRowType,
            List<Boolean> argumentLiterals,
            List<Boolean> argumentNulls,
            List<Object> argumentValues)
            throws Exception {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(function);
                oos.writeObject(paramsSchema.toJson());
                oos.writeObject(outputSchema.toJson());
                oos.writeObject(paramRowType);
                oos.writeObject(outputRowType);
                oos.writeObject(argumentLiterals);
                oos.writeObject(argumentNulls);
                oos.writeObject(argumentValues);
            }
            return baos.toByteArray();
        }
    }

    /**
     * Resolves the user-defined ScalarFunction from either Flink 1.18 planner representation:
     * legacy {@link ScalarSqlFunction} and the newer {@link BridgingSqlFunction} bridge. A
     * bridging function's {@link FunctionDefinition} is the function instance itself.
     */
    private static ScalarFunction resolveFunction(RexCall call) {
        if (call.getOperator() instanceof ScalarSqlFunction) {
            return ((ScalarSqlFunction) call.getOperator()).scalarFunction();
        }
        if (call.getOperator() instanceof BridgingSqlFunction) {
            FunctionDefinition definition = ((BridgingSqlFunction) call.getOperator()).getDefinition();
            return definition instanceof ScalarFunction ? (ScalarFunction) definition : null;
        }
        return null;
    }

    /**
     * Delegates operand conversion to the factory, failing fast when no converter accepts it.
     */
    private static PhysicalExprNode convertOperand(
            RexNode operand, FlinkNodeConverterFactory factory, ConverterContext context) {
        Optional<PhysicalExprNode> result = factory.convertRexNode(operand, context);
        if (!result.isPresent()) {
            throw new IllegalStateException("Failed to convert UDF operand: " + operand + " (no converter registered)");
        }
        return result.get();
    }
}
