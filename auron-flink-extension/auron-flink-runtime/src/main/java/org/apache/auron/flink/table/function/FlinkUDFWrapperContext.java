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

import java.util.List;
import java.util.Objects;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.auron.flink.arrow.FlinkArrowReader;
import org.apache.auron.flink.arrow.FlinkArrowUtils;
import org.apache.auron.flink.arrow.FlinkArrowWriter;
import org.apache.auron.functions.AuronUDFWrapperContext;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * Flink UDF 调用上下文：按 Arrow C-Data 批协议在 native 与 Java 之间搬运一个 UDF 调用的输入输出。
 *
 * <p>契约：
 *
 * <ul>
 *   <li>函数实例不在本类创建或打开：{@link FlinkUdfInstance} 是唯一的共享实例来源，eval 时在实例锁内
 *       取得已 open 的函数并逐行调用；open/close 由生命周期统一管理。
 *   <li>输入批由 native 导出、本类 import 并在本类返回时释放；输出批在本类导出到调用方传入的 FFI
 *       结构后由调用方接管。两个方向的临时 {@link VectorSchemaRoot} 与 FFI 包装都在 eval 返回前关闭，
 *       不跨 eval 保留任何 Arrow 资源。
 *   <li>调用门控（RUNNING 检查、实例互斥、迟到 eval 拒绝）由生命周期门面承担，本类不做重复状态机。
 * </ul>
 */
public final class FlinkUDFWrapperContext implements AuronUDFWrapperContext {

    private final FlinkUdfInstance instance;
    private final Schema paramsSchema;
    private final Schema outputSchema;
    private final RowType paramRowType;
    private final RowType outputRowType;
    private final FlinkUDFHelper helper;
    private final List<Boolean> argumentLiterals;
    private final List<Boolean> argumentNulls;
    private final List<Object> argumentValues;
    private final DictionaryProvider dictionaryProvider = new DictionaryProvider.MapDictionaryProvider();
    private double lastStringBytesPerRow;

    FlinkUDFWrapperContext(
            FlinkUdfInstance instance,
            Schema paramsSchema,
            Schema outputSchema,
            RowType paramRowType,
            RowType outputRowType,
            FlinkUDFHelper helper,
            List<Boolean> argumentLiterals,
            List<Boolean> argumentNulls,
            List<Object> argumentValues) {
        this.instance = Objects.requireNonNull(instance, "instance");
        this.paramsSchema = Objects.requireNonNull(paramsSchema, "paramsSchema");
        this.outputSchema = Objects.requireNonNull(outputSchema, "outputSchema");
        this.paramRowType = Objects.requireNonNull(paramRowType, "paramRowType");
        this.outputRowType = Objects.requireNonNull(outputRowType, "outputRowType");
        this.helper = Objects.requireNonNull(helper, "helper");
        this.argumentLiterals = Objects.requireNonNull(argumentLiterals, "argumentLiterals");
        this.argumentNulls = Objects.requireNonNull(argumentNulls, "argumentNulls");
        this.argumentValues = Objects.requireNonNull(argumentValues, "argumentValues");
    }

    /**
     * 处理一个输入批并导出输出批。
     *
     * @param inputPtr native 导出的输入 ArrowArray FFI 结构地址
     * @param outputPtr 待填充的输出 ArrowArray FFI 结构地址，调用方在 eval 返回后接管
     */
    @Override
    public void eval(long inputPtr, long outputPtr) {
        try (VectorSchemaRoot paramsRoot = VectorSchemaRoot.create(paramsSchema, FlinkArrowUtils.getRootAllocator());
                VectorSchemaRoot outputRoot =
                        VectorSchemaRoot.create(outputSchema, FlinkArrowUtils.getRootAllocator())) {
            try (ArrowArray importArray = ArrowArray.wrap(inputPtr)) {
                Data.importIntoVectorSchemaRoot(
                        FlinkArrowUtils.getRootAllocator(),
                        importArray,
                        paramsRoot,
                        dictionaryProvider); // 这个函数调用的结果是，paramsRoot会被填入元数据信息
            }
            ScalarFunction function = sharedFunction();
            try (FlinkArrowReader arrowReader = FlinkArrowReader.create(paramsRoot, paramRowType)) {
                int rowCount = paramsRoot.getRowCount();
                FlinkArrowWriter arrowWriter = FlinkArrowWriter.create(
                        outputRoot, outputRowType, rowCount, 3 * estimateOutputBytesPerRow(paramsRoot));
                GenericRowData outputRow = new GenericRowData(outputRowType.getFieldCount());
                for (int i = 0; i < rowCount; i++) {
                    RowData row = arrowReader.read(i);
                    outputRow.setField(0, helper.evalUDF(function, row));
                    arrowWriter.write(outputRow);
                }
                arrowWriter.finish();
                updateLastStringBytesPerRow(outputRoot, rowCount);
            }
            try (ArrowArray exportArray = ArrowArray.wrap(outputPtr)) {
                Data.exportVectorSchemaRoot(
                        FlinkArrowUtils.getRootAllocator(), outputRoot, dictionaryProvider, exportArray);
            }
        }
    }

    private ScalarFunction sharedFunction() {
        try {
            return instance.invoke(function -> function);
        } catch (Exception failure) {
            throw new IllegalStateException("无法取得共享 UDF 函数实例", failure);
        }
    }

    private double estimateOutputBytesPerRow(VectorSchemaRoot paramsRoot) {
        int rowCount = paramsRoot.getRowCount();
        if (rowCount <= 0 || !isStringOutput()) {
            return 0.0;
        }
        long totalInputBytes = 0;
        for (FieldVector vector : paramsRoot.getFieldVectors()) {
            if (vector instanceof BaseVariableWidthVector) {
                totalInputBytes += ((BaseVariableWidthVector) vector).sizeOfValueBuffer();
            }
        }
        double estimated = totalInputBytes / (double) rowCount;
        return Math.max(8.0, Math.max(estimated, lastStringBytesPerRow));
    }

    private void updateLastStringBytesPerRow(VectorSchemaRoot outputRoot, int rowCount) {
        if (rowCount <= 0 || !isStringOutput()) {
            return;
        }
        FieldVector vector = outputRoot.getVector(0);
        if (vector instanceof BaseVariableWidthVector) {
            int totalBytes = ((BaseVariableWidthVector) vector).sizeOfValueBuffer();
            lastStringBytesPerRow = totalBytes / (double) rowCount;
        }
    }

    private boolean isStringOutput() {
        return outputRowType.getFieldCount() == 1
                && (outputRowType.getTypeAt(0) instanceof VarCharType
                        || outputRowType.getTypeAt(0) instanceof CharType);
    }
}
