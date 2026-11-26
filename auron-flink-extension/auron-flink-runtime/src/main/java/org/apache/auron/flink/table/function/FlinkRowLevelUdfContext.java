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

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.auron.functions.AuronUDFWrapperContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public interface FlinkRowLevelUdfContext extends AuronUDFWrapperContext {
    Object evalValue(RowData input);

    FlinkUdfBoundaryMetadata metadata();

    final class FlinkUdfBoundaryMetadata {
        private final Schema inputSchema;
        private final Schema outputSchema;
        private final RowType inputRowType;
        private final RowType outputRowType;

        FlinkUdfBoundaryMetadata(Schema inputSchema, Schema outputSchema, RowType inputRowType, RowType outputRowType) {
            this.inputSchema = inputSchema;
            this.outputSchema = outputSchema;
            this.inputRowType = inputRowType;
            this.outputRowType = outputRowType;
        }

        public Schema inputSchema() {
            return inputSchema;
        }

        public Schema outputSchema() {
            return outputSchema;
        }

        public RowType inputRowType() {
            return inputRowType;
        }

        public RowType outputRowType() {
            return outputRowType;
        }
    }
}
