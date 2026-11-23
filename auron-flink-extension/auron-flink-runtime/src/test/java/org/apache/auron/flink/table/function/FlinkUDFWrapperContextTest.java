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
package org.apache.auron.flink.table.function;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.auron.flink.arrow.FlinkArrowUtils;
import org.apache.auron.flink.arrow.FlinkArrowWriter;
import org.apache.auron.functions.AuronUDFWrapperContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.LogicalTypeDataTypeConverter;
import org.junit.jupiter.api.Test;

class FlinkUDFWrapperContextTest {
    private static final ClassLoader LOADER = FlinkUDFWrapperContextTest.class.getClassLoader();
    private static final FunctionContext FUNCTION_CONTEXT = new FunctionContext(null, LOADER, new Configuration());

    public static class AddUdf extends ScalarFunction {
        public Integer eval(Integer a, Integer b) {
            return a == null || b == null ? null : a + b;
        }
    }

    public static class ConcatUdf extends ScalarFunction {
        public String eval(String a, String b) {
            return a == null || b == null ? null : a + b;
        }
    }

    static class ManualAddHelper implements FlinkUDFHelperInterface<Object> {
        @SuppressWarnings("rawtypes")
        private final DataFormatConverters.DataFormatConverter[] converters =
                new DataFormatConverters.DataFormatConverter[3];

        @Override
        public Object eval(ScalarFunction function, RowData input) {
            return ((AddUdf) function)
                    .eval(input.isNullAt(0) ? null : input.getInt(0), input.isNullAt(1) ? null : input.getInt(1));
        }

        @Override
        @SuppressWarnings("unchecked")
        public void initDataFormatConverters(int index, LogicalType logicalType) {
            converters[index] =
                    DataFormatConverters.getConverterForDataType(LogicalTypeDataTypeConverter.toDataType(logicalType));
        }
    }

    static class ManualConcatHelper implements FlinkUDFHelperInterface<Object> {
        @SuppressWarnings("rawtypes")
        private final DataFormatConverters.DataFormatConverter[] converters =
                new DataFormatConverters.DataFormatConverter[3];

        @Override
        @SuppressWarnings("unchecked")
        public Object eval(ScalarFunction function, RowData input) {
            Object a = converters[0].toExternal(input, 0);
            Object b = converters[1].toExternal(input, 1);
            Object result = ((ConcatUdf) function).eval((String) a, (String) b);
            if (result == null) {
                return null;
            }
            if (result instanceof StringData) {
                return result;
            }
            return converters[2].toInternal(result);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void initDataFormatConverters(int index, LogicalType logicalType) {
            converters[index] =
                    DataFormatConverters.getConverterForDataType(LogicalTypeDataTypeConverter.toDataType(logicalType));
        }
    }

    static class RecordingCompiler implements FlinkUDFHelper.Compiler {
        private final FlinkUDFHelperInterface<?> helper;
        private final List<String> classNames = new ArrayList<>();
        private final List<String> sourceCodes = new ArrayList<>();

        RecordingCompiler(FlinkUDFHelperInterface<?> helper) {
            this.helper = helper;
        }

        @Override
        public Class<?> compile(ClassLoader classLoader, String className, String sourceCode) {
            classNames.add(className);
            sourceCodes.add(sourceCode);
            return helper.getClass();
        }
    }

    @Test
    void generatedSourceUsesDeterministicClassAndInternalDirectPassThrough() throws Exception {
        byte[] descriptor =
                descriptor(new AddUdf(), RowType.of(new IntType(), new IntType()), RowType.of(new IntType()));
        RecordingCompiler compiler = new RecordingCompiler(new ManualAddHelper());
        try (FlinkUdfLifecycle lifecycle = new FlinkUdfLifecycle()) {
            lifecycle.openAndBind(
                    Collections.singletonList(payload("calc", 0, AddUdf.class.getName(), descriptor)),
                    FUNCTION_CONTEXT,
                    LOADER,
                    FlinkUDFHelper.contextFactory(compiler));
        }
        assertEquals(1, compiler.classNames.size());
        String className = compiler.classNames.get(0);
        assertTrue(className.startsWith("FlinkUDFHelper$"), className);
        String sourceCode = compiler.sourceCodes.get(0);
        assertTrue(
                sourceCode.contains("tmpFun.eval((java.lang.Integer) converters[0].toExternal(input, 0),"
                        + " (java.lang.Integer) converters[1].toExternal(input, 1))"),
                sourceCode);
        assertTrue(sourceCode.contains("instanceof java.lang.Integer"), sourceCode);
        assertTrue(sourceCode.contains("converters[2].toInternal(result)"), sourceCode);
        assertTrue(
                sourceCode.contains(DataFormatConverters.class.getName()
                        + ".getConverterForDataType("
                        + LogicalTypeDataTypeConverter.class.getName()
                        + ".toDataType(logicalType))"),
                sourceCode);
        String expectedPrefix = "public class " + className + " implements ";
        assertEquals(expectedPrefix, sourceCode.substring(0, expectedPrefix.length()));
    }

    @Test
    void compilesOncePerDeduplicatedContext() throws Exception {
        byte[] addDescriptor =
                descriptor(new AddUdf(), RowType.of(new IntType(), new IntType()), RowType.of(new IntType()));
        byte[] concatDescriptor = descriptor(
                new ConcatUdf(), RowType.of(new VarCharType(), new VarCharType()), RowType.of(new VarCharType()));
        RecordingCompiler compiler = new RecordingCompiler(new ManualAddHelper());
        try (FlinkUdfLifecycle lifecycle = new FlinkUdfLifecycle()) {
            lifecycle.openAndBind(
                    java.util.Arrays.asList(
                            payload("calc", 0, AddUdf.class.getName(), addDescriptor),
                            payload("calc", 1, AddUdf.class.getName(), addDescriptor),
                            payload("calc", 2, ConcatUdf.class.getName(), concatDescriptor)),
                    FUNCTION_CONTEXT,
                    LOADER,
                    FlinkUDFHelper.contextFactory(compiler));
        }
        assertEquals(2, compiler.classNames.size());
        assertNotEquals(compiler.classNames.get(0), compiler.classNames.get(1));
    }

    @Test
    void externalArgumentsAndOutputUseConvertersInGeneratedSource() throws Exception {
        byte[] descriptor = descriptor(
                new ConcatUdf(), RowType.of(new VarCharType(), new VarCharType()), RowType.of(new VarCharType()));
        RecordingCompiler compiler = new RecordingCompiler(new ManualConcatHelper());
        try (FlinkUdfLifecycle lifecycle = new FlinkUdfLifecycle()) {
            lifecycle.openAndBind(
                    Collections.singletonList(payload("calc", 0, ConcatUdf.class.getName(), descriptor)),
                    FUNCTION_CONTEXT,
                    LOADER,
                    FlinkUDFHelper.contextFactory(compiler));
        }
        String sourceCode = compiler.sourceCodes.get(0);
        assertTrue(sourceCode.contains("(java.lang.String) converters[0].toExternal(input, 0)"), sourceCode);
        assertTrue(sourceCode.contains("(java.lang.String) converters[1].toExternal(input, 1)"), sourceCode);
        assertTrue(sourceCode.contains("instanceof org.apache.flink.table.data.StringData"), sourceCode);
        assertTrue(sourceCode.contains("converters[2].toInternal(result)"), sourceCode);
    }

    @Test
    void evaluatesBatchWithNullsThroughArrowFFI() throws Exception {
        RowType paramRowType = RowType.of(new IntType(), new IntType());
        RowType outputRowType = RowType.of(new IntType());
        byte[] descriptor = descriptor(new AddUdf(), paramRowType, outputRowType);
        RecordingCompiler compiler = new RecordingCompiler(new ManualAddHelper());
        List<FlinkUdfToken> tokens;
        try (BufferAllocator allocator = FlinkArrowUtils.createChildAllocator("udf-wrapper-int-lifecycle-test");
                FlinkUdfLifecycle lifecycle = new FlinkUdfLifecycle()) {
            tokens = lifecycle.openAndBind(
                    Collections.singletonList(payload("calc", 0, AddUdf.class.getName(), descriptor)),
                    FUNCTION_CONTEXT,
                    LOADER,
                    FlinkUDFHelper.contextFactory(compiler));
            AuronUDFWrapperContext context = UdfRegistry.lookup(tokens.get(0));
            try (ArrowArray inputFfi = ArrowArray.allocateNew(allocator);
                    ArrowArray outputFfi = ArrowArray.allocateNew(allocator)) {
                try (VectorSchemaRoot inputRoot =
                        VectorSchemaRoot.create(FlinkArrowUtils.toArrowSchema(paramRowType), allocator)) {
                    FlinkArrowWriter writer = FlinkArrowWriter.create(inputRoot, paramRowType);
                    writer.write(intRow(1, 2));
                    writer.write(intRow(null, 5));
                    writer.write(intRow(4, null));
                    writer.finish();
                    Data.exportVectorSchemaRoot(allocator, inputRoot, null, inputFfi);
                    context.eval(inputFfi.memoryAddress(), outputFfi.memoryAddress());
                }
                try (VectorSchemaRoot resultRoot =
                        VectorSchemaRoot.create(FlinkArrowUtils.toArrowSchema(outputRowType), allocator)) {
                    Data.importIntoVectorSchemaRoot(allocator, outputFfi, resultRoot, null);
                    assertEquals(3, resultRoot.getRowCount());
                    IntVector output = (IntVector) resultRoot.getVector(0);
                    assertEquals(3, output.get(0));
                    assertTrue(output.isNull(1));
                    assertTrue(output.isNull(2));
                }
            }
            assertEquals(0, allocator.getAllocatedMemory(), "Arrow FFI buffers must be released after eval");
        }
    }

    @Test
    void externalStringConversionEvaluatesThroughArrowFFI() throws Exception {
        RowType paramRowType = RowType.of(new VarCharType(), new VarCharType());
        RowType outputRowType = RowType.of(new VarCharType());
        byte[] descriptor = descriptor(new ConcatUdf(), paramRowType, outputRowType);
        RecordingCompiler compiler = new RecordingCompiler(new ManualConcatHelper());
        List<FlinkUdfToken> tokens;
        try (BufferAllocator allocator = FlinkArrowUtils.createChildAllocator("udf-wrapper-string-lifecycle-test");
                FlinkUdfLifecycle lifecycle = new FlinkUdfLifecycle()) {
            tokens = lifecycle.openAndBind(
                    Collections.singletonList(payload("calc", 0, ConcatUdf.class.getName(), descriptor)),
                    FUNCTION_CONTEXT,
                    LOADER,
                    FlinkUDFHelper.contextFactory(compiler));
            AuronUDFWrapperContext context = UdfRegistry.lookup(tokens.get(0));
            try (ArrowArray inputFfi = ArrowArray.allocateNew(allocator);
                    ArrowArray outputFfi = ArrowArray.allocateNew(allocator)) {
                try (VectorSchemaRoot inputRoot =
                        VectorSchemaRoot.create(FlinkArrowUtils.toArrowSchema(paramRowType), allocator)) {
                    FlinkArrowWriter writer = FlinkArrowWriter.create(inputRoot, paramRowType);
                    writer.write(stringRow("a", "b"));
                    writer.write(stringRow(null, "x"));
                    writer.write(stringRow("y", null));
                    writer.finish();
                    Data.exportVectorSchemaRoot(allocator, inputRoot, null, inputFfi);
                    context.eval(inputFfi.memoryAddress(), outputFfi.memoryAddress());
                }
                try (VectorSchemaRoot resultRoot =
                        VectorSchemaRoot.create(FlinkArrowUtils.toArrowSchema(outputRowType), allocator)) {
                    Data.importIntoVectorSchemaRoot(allocator, outputFfi, resultRoot, null);
                    assertEquals(3, resultRoot.getRowCount());
                    VarCharVector output = (VarCharVector) resultRoot.getVector(0);
                    assertEquals("ab", output.getObject(0).toString());
                    assertTrue(output.isNull(1));
                    assertTrue(output.isNull(2));
                }
            }
            assertEquals(0, allocator.getAllocatedMemory(), "Arrow FFI buffers must be released after eval");
        }
    }

    private static FlinkUdfPayload payload(String scope, long order, String identifier, byte[] descriptor) {
        return new FlinkUdfPayload(scope, order, identifier, descriptor);
    }

    private static byte[] descriptor(ScalarFunction function, RowType paramRowType, RowType outputRowType)
            throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(function);
            out.writeObject(FlinkArrowUtils.toArrowSchema(paramRowType).toJson());
            out.writeObject(FlinkArrowUtils.toArrowSchema(outputRowType).toJson());
            out.writeObject(paramRowType);
            out.writeObject(outputRowType);
            out.writeObject(booleans(paramRowType.getFieldCount()));
            out.writeObject(booleans(paramRowType.getFieldCount()));
            out.writeObject(nulls(paramRowType.getFieldCount()));
        }
        return bytes.toByteArray();
    }

    private static List<Boolean> booleans(int size) {
        List<Boolean> values = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            values.add(false);
        }
        return values;
    }

    private static List<Object> nulls(int size) {
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            values.add(null);
        }
        return values;
    }

    private static GenericRowData intRow(Integer a, Integer b) {
        GenericRowData row = new GenericRowData(2);
        row.setField(0, a);
        row.setField(1, b);
        return row;
    }

    private static GenericRowData stringRow(String a, String b) {
        GenericRowData row = new GenericRowData(2);
        row.setField(0, a == null ? null : StringData.fromString(a));
        row.setField(1, b == null ? null : StringData.fromString(b));
        return row;
    }
}
