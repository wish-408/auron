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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.logical.LogicalType;

/**
 * 调用约定接口，由 {@link FlinkUDFHelper} 生成的实现类承载。
 *
 * <p>契约：
 *
 * <ul>
 *   <li>{@code eval} 不持有函数实例：{@link ScalarFunction} 由调用方（owner 注入的共享实例）逐次传入，
 *       helper 实现只负责参数转换和方法调用。
 *   <li>{@code initDataFormatConverters} 在 context 初始化阶段按参数顺序逐个调用一次，之后不再变更；
 *       下标 {@code 0..paramCount-1} 为参数转换器，{@code paramCount} 为输出转换器。
 * </ul>
 */
public interface FlinkUDFHelperInterface<T> {

    /**
     * 求值一次 UDF 调用：从列式行视图提取参数并调用 {@code eval}，返回输出列的内部表示值。
     *
     * @param function 共享的 ScalarFunction 实例，由调用方保证已 open
     * @param input 参数行视图（例如 Arrow-backed 的 {@code AuronColumnarRowData}），字段为内部表示值
     * @return 输出列的内部表示值；输出列为空时返回 null
     */
    T eval(ScalarFunction function, RowData input);

    /**
     * 初始化指定下标的 DataFormatConverter。
     *
     * @param index 转换器下标；参数转换器为 {@code 0..paramCount-1}，输出转换器为 {@code paramCount}
     * @param logicalType 该下标对应的 Flink LogicalType
     */
    void initDataFormatConverters(int index, LogicalType logicalType);
}
