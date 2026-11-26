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

import java.util.ArrayList;
import java.util.List;
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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

public final class MergedUDFWrapperContext implements FlinkRowLevelUdfContext {
    private final List<FlinkUdfToken> tokens;
    private final Schema inputSchema;
    private final Schema outputSchema;
    private final RowType inputRowType;
    private final RowType outputRowType;
    private final DictionaryProvider dictionaryProvider = new DictionaryProvider.MapDictionaryProvider();
    private double lastStringBytesPerRow;

    public MergedUDFWrapperContext(List<FlinkUdfToken> tokens) {
        FlinkRowLevelUdfContext startContext = UdfRegistry.lookup(tokens.get(0));
        FlinkRowLevelUdfContext endContext = UdfRegistry.lookup(tokens.get(tokens.size() - 1));
        this.tokens = tokens;
        FlinkUdfBoundaryMetadata inputMetadata = startContext.metadata();
        FlinkUdfBoundaryMetadata outputMetada = endContext.metadata();
        this.inputSchema = inputMetadata.inputSchema();
        this.inputRowType = inputMetadata.inputRowType();
        this.outputSchema = outputMetada.outputSchema();
        this.outputRowType = outputMetada.outputRowType();
    }

    @Override
    public void eval(long inputPtr, long outputPtr) {
        try (VectorSchemaRoot paramsRoot = VectorSchemaRoot.create(inputSchema, FlinkArrowUtils.getRootAllocator());
                VectorSchemaRoot outputRoot =
                        VectorSchemaRoot.create(outputSchema, FlinkArrowUtils.getRootAllocator())) {
            try (ArrowArray importArray = ArrowArray.wrap(inputPtr)) {
                Data.importIntoVectorSchemaRoot(
                        FlinkArrowUtils.getRootAllocator(),
                        importArray,
                        paramsRoot,
                        dictionaryProvider); // 这个函数调用的结果是，paramsRoot会被填入元数据信息
            }
            try (FlinkArrowReader arrowReader = FlinkArrowReader.create(paramsRoot, inputRowType)) {
                int rowCount = paramsRoot.getRowCount();
                FlinkArrowWriter arrowWriter = FlinkArrowWriter.create(
                        outputRoot, outputRowType, rowCount, estimateOutputBytesPerRow(paramsRoot));
                GenericRowData outputRow = new GenericRowData(outputRowType.getFieldCount());

                // 一次性获得所有组内udf实例的引用
                List<FlinkRowLevelUdfContext> contexts = new ArrayList<FlinkRowLevelUdfContext>(tokens.size());
                for (FlinkUdfToken token : tokens) {
                    FlinkRowLevelUdfContext context = UdfRegistry.lookup(token);
                    contexts.add(context);
                }

                for (int i = 0; i < rowCount; i++) {
                    RowData input = arrowReader.read(i);
                    Object output = new Object();
                    for (FlinkRowLevelUdfContext context : contexts) {
                        output = context.evalValue(input);
                        input = GenericRowData.of(output);
                    }
                    outputRow.setField(0, output);
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

    @Override
    public Object evalValue(RowData input) {
        Object output = new Object();
        for (FlinkUdfToken token : tokens) {
            FlinkRowLevelUdfContext context = UdfRegistry.lookup(token);
            output = context.evalValue(input);
            input = GenericRowData.of(output);
        }
        return output;
    }

    @Override
    public FlinkUdfBoundaryMetadata metadata() {
        return new FlinkUdfBoundaryMetadata(inputSchema, outputSchema, inputRowType, outputRowType);
    }
}
