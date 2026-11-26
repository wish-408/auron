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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.UUID;
import org.apache.auron.flink.arrow.FlinkArrowUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class MergedUDFWrapperContextTest {

    @Test
    void evalValueExecutesUdfChainInOrderAndReturnsFinalValue() {
        UUID owner = UUID.randomUUID();
        FlinkUdfToken firstToken = new FlinkUdfToken(owner, 0);
        FlinkUdfToken secondToken = new FlinkUdfToken(owner, 1);
        IntFunctionContext first = new IntFunctionContext(value -> value + 1);
        IntFunctionContext second = new IntFunctionContext(value -> value * 2);

        UdfRegistry.register(firstToken, first);
        UdfRegistry.register(secondToken, second);
        try {
            MergedUDFWrapperContext merged = new MergedUDFWrapperContext(Arrays.asList(firstToken, secondToken));

            Object result = merged.evalValue(GenericRowData.of(3));

            assertEquals(8, result);
        } finally {
            UdfRegistry.remove(firstToken);
            UdfRegistry.remove(secondToken);
        }
    }

    @Test
    void metadataUsesFirstInputAndLastOutput() {
        UUID owner = UUID.randomUUID();
        FlinkUdfToken firstToken = new FlinkUdfToken(owner, 0);
        FlinkUdfToken secondToken = new FlinkUdfToken(owner, 1);
        IntFunctionContext first = new IntFunctionContext(value -> value + 1);
        IntFunctionContext second = new IntFunctionContext(value -> value * 2);

        UdfRegistry.register(firstToken, first);
        UdfRegistry.register(secondToken, second);
        try {
            MergedUDFWrapperContext merged = new MergedUDFWrapperContext(Arrays.asList(firstToken, secondToken));
            FlinkRowLevelUdfContext.FlinkUdfBoundaryMetadata metadata = merged.metadata();

            assertEquals(first.metadata().inputSchema(), metadata.inputSchema());
            assertEquals(first.metadata().inputRowType(), metadata.inputRowType());
            assertEquals(second.metadata().outputSchema(), metadata.outputSchema());
            assertEquals(second.metadata().outputRowType(), metadata.outputRowType());
        } finally {
            UdfRegistry.remove(firstToken);
            UdfRegistry.remove(secondToken);
        }
    }

    private static final class IntFunctionContext implements FlinkRowLevelUdfContext {
        private interface EvalFunction {
            Object apply(int value);
        }

        private final EvalFunction function;
        private final FlinkUdfBoundaryMetadata metadata;

        private IntFunctionContext(EvalFunction function) {
            this.function = function;
            RowType rowType = RowType.of(new IntType(false));
            this.metadata = new FlinkUdfBoundaryMetadata(
                    FlinkArrowUtils.toArrowSchema(rowType), FlinkArrowUtils.toArrowSchema(rowType), rowType, rowType);
        }

        @Override
        public void eval(long inputPtr, long outputPtr) {
            throw new UnsupportedOperationException("Not used by this test");
        }

        @Override
        public Object evalValue(RowData input) {
            return function.apply(input.getInt(0));
        }

        @Override
        public FlinkUdfBoundaryMetadata metadata() {
            return metadata;
        }
    }
}
