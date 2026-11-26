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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.runtime.generated.CompileUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.table.types.utils.LogicalTypeDataTypeConverter;

/**
 * Java 调用适配器：为共享的 {@link ScalarFunction} 生成逐参数转换和输出转换的调用代码，并持有编译产物。
 *
 * <p>与参考实现保持一致的约定：
 *
 * <ul>
 *   <li>参数侧：生成代码直接用 {@code converters[i].toExternal(input, i)} 从
 *       {@link RowData} 行视图取值，不再经过 {@code GenericRowData} 逐字段装箱。
 *   <li>输出侧：返回值已是输出内部表示类时直通；否则经
 *       {@link DataFormatConverters.DataFormatConverter#toInternal(Object)} 转换。
 *   <li>helper 不持有函数实例，{@code eval} 每次由调用方传入 owner 持有的共享实例；
 *       helper 自身也不再创建函数。
 * </ul>
 *
 * <p>对参考实现的修正：类名使用进程内递增序号而非毫秒时间戳，避免不可预测的类名膨胀；一次 context
 * 初始化只编译一次——跨多轮 native runtime 的重复编译由 token 化的 context 常驻消除。
 */
public final class FlinkUDFHelper {

    /**
     * 编译器注入点。默认实现使用 Flink 的 {@link CompileUtils}（运行时依赖 Flink 发行版自带的 Janino）；
     * 测试可注入替身以在不引入 Janino 的环境下验证生成源码与编译次数。
     */
    @FunctionalInterface
    public interface Compiler {
        Class<?> compile(ClassLoader classLoader, String className, String sourceCode) throws Exception;
    }

    private static final Compiler DEFAULT_COMPILER = CompileUtils::compile;
    private static final AtomicLong CLASS_SEQUENCE = new AtomicLong();

    private final FlinkUDFHelperInterface<?> function;

    /**
     * 以默认编译器构造 helper；见 {@link #FlinkUDFHelper(ScalarFunction, RowType, RowType, Compiler)}。
     */
    public FlinkUDFHelper(ScalarFunction function, RowType paramRowType, RowType outputRowType) throws Exception {
        this(function, paramRowType, outputRowType, DEFAULT_COMPILER);
    }

    /**
     * 生成、编译并初始化 UDF 调用代码。
     *
     * @param function 共享的 ScalarFunction，用于确定用户类加载器和生成代码中的强转目标
     * @param paramRowType 参数 RowType；决定每个实参的提取与转换方式
     * @param outputRowType 输出 RowType；必须恰好一列，决定输出转换的目标内部表示类
     * @param compiler 编译器；默认路径使用 {@link CompileUtils}
     */
    public FlinkUDFHelper(ScalarFunction function, RowType paramRowType, RowType outputRowType, Compiler compiler)
            throws Exception {
        Objects.requireNonNull(function, "function");
        Objects.requireNonNull(paramRowType, "paramRowType");
        Objects.requireNonNull(outputRowType, "outputRowType");
        Objects.requireNonNull(compiler, "compiler");
        if (outputRowType.getFieldCount() != 1) {
            throw new IllegalArgumentException("outputRowType 必须恰好一列: " + outputRowType.getFieldCount());
        }
        String className = "FlinkUDFHelper$" + CLASS_SEQUENCE.incrementAndGet();
        Class<?> compiled = compiler.compile(
                function.getClass().getClassLoader(),
                className,
                generateSourceCode(function, paramRowType, outputRowType, className));
        this.function = instantiate(compiled);
        for (int i = 0; i < paramRowType.getFieldCount(); i++) {
            this.function.initDataFormatConverters(i, paramRowType.getTypeAt(i));
        }
        this.function.initDataFormatConverters(paramRowType.getFieldCount(), outputRowType.getTypeAt(0));
    }

    /**
     * 求值一次 UDF 调用。
     *
     * @param function owner 持有的共享实例
     * @param input 参数行视图（内部表示）
     * @return 输出列的内部表示值；可能为 null
     */
    public Object evalUDF(ScalarFunction function, RowData input) {
        return this.function.eval(function, input);
    }

    /**
     * 返回使用默认编译器的调用工厂，owner 的 open 路径直接传入
     * {@link FlinkUdfLifecycle#openAndBind(List, FunctionContext, ClassLoader, ContextFactory)}。
     */
    public static FlinkUdfLifecycle.ContextFactory contextFactory() {
        return contextFactory(DEFAULT_COMPILER);
    }

    /**
     * 返回使用指定编译器的调用工厂；测试注入替身时使用。
     */
    public static FlinkUdfLifecycle.ContextFactory contextFactory(Compiler compiler) {
        return new DescriptorFactory(Objects.requireNonNull(compiler, "compiler"));
    }

    private static FlinkUDFHelperInterface<?> instantiate(Class<?> compiled) throws Exception {
        Object instance = compiled.getDeclaredConstructor().newInstance();
        if (!(instance instanceof FlinkUDFHelperInterface)) {
            throw new IllegalStateException("编译产物未实现 FlinkUDFHelperInterface: " + compiled.getName());
        }
        return (FlinkUDFHelperInterface<?>) instance;
    }

    private static String generateSourceCode(
            ScalarFunction function, RowType paramRowType, RowType outputRowType, String className) {
        List<String> arguments = new ArrayList<>();
        for (int i = 0; i < paramRowType.getFieldCount(); i++) {
            DataType dataType = LogicalTypeDataTypeConverter.toDataType(paramRowType.getTypeAt(i));
            arguments.add("("
                    + dataType.getConversionClass().getName()
                    + ") converters["
                    + i
                    + "].toExternal(input, "
                    + i
                    + ")");
        }
        int argumentCount = paramRowType.getFieldCount();
        String outputInternalClass = LogicalTypeUtils.toInternalConversionClass(outputRowType.getTypeAt(0))
                .getName();
        String converterType = DataFormatConverters.DataFormatConverter.class.getName();
        StringBuilder sourceCode = new StringBuilder();
        sourceCode
                .append("public class ")
                .append(className)
                .append(" implements ")
                .append(FlinkUDFHelperInterface.class.getName())
                .append("<java.lang.Object> {\n");
        sourceCode
                .append("    ")
                .append(converterType)
                .append("[] converters = new ")
                .append(converterType)
                .append('[')
                .append(argumentCount + 1)
                .append("];\n");
        sourceCode
                .append("    public java.lang.Object eval(")
                .append(ScalarFunction.class.getName())
                .append(" fun, ")
                .append(RowData.class.getName())
                .append(" input) {\n");
        sourceCode
                .append("        ")
                .append(function.getClass().getName())
                .append(" tmpFun = (")
                .append(function.getClass().getName())
                .append(") fun;\n");
        sourceCode
                .append("        java.lang.Object result = tmpFun.eval(")
                .append(String.join(", ", arguments))
                .append(");\n");
        sourceCode.append("        if (result == null) {\n");
        sourceCode.append("            return null;\n");
        sourceCode.append("        }\n");
        sourceCode
                .append("        if (result instanceof ")
                .append(outputInternalClass)
                .append(") {\n");
        sourceCode.append("            return result;\n");
        sourceCode.append("        }\n");
        sourceCode.append("        return converters[").append(argumentCount).append("].toInternal(result);\n");
        sourceCode.append("    }\n");
        sourceCode
                .append("    public void initDataFormatConverters(int index, ")
                .append(LogicalType.class.getName())
                .append(" logicalType) {\n");
        sourceCode
                .append("        converters[index] = ")
                .append(DataFormatConverters.class.getName())
                .append(".getConverterForDataType(")
                .append(LogicalTypeDataTypeConverter.class.getName())
                .append(".toDataType(logicalType));\n");
        sourceCode.append("    }\n");
        sourceCode.append("}\n");
        return sourceCode.toString();
    }

    private static final class DescriptorFactory implements FlinkUdfLifecycle.ContextFactory {
        private final Compiler compiler;

        private DescriptorFactory(Compiler compiler) {
            this.compiler = compiler;
        }

        @Override
        public ScalarFunction resolveFunction(FlinkUdfPayload payload, ClassLoader classLoader) throws Exception {
            try (DescriptorObjectInputStream in =
                    new DescriptorObjectInputStream(payload.getCallDescriptor(), classLoader)) {
                Object function = in.readObject();
                if (!(function instanceof ScalarFunction)) {
                    throw new IllegalArgumentException(
                            "调用描述首项不是 ScalarFunction: " + function.getClass().getName());
                }
                return (ScalarFunction) function;
            }
        }

        @Override
        public FlinkRowLevelUdfContext createContext(FlinkUdfPayload payload, FlinkUdfInstance instance)
                throws Exception {
            ClassLoader classLoader =
                    instance.prepare(function -> function.getClass().getClassLoader());
            try (DescriptorObjectInputStream in =
                    new DescriptorObjectInputStream(payload.getCallDescriptor(), classLoader)) {
                in.readObject();
                String paramsSchemaJson = requireType(in.readObject(), String.class, "paramsSchema");
                String outputSchemaJson = requireType(in.readObject(), String.class, "outputSchema");
                RowType paramRowType = requireType(in.readObject(), RowType.class, "paramRowType");
                RowType outputRowType = requireType(in.readObject(), RowType.class, "outputRowType");
                List<Boolean> argumentLiterals = readBooleanList(in.readObject(), "argumentLiterals");
                List<Boolean> argumentNulls = readBooleanList(in.readObject(), "argumentNulls");
                List<Object> argumentValues = readObjectList(in.readObject(), "argumentValues");
                FlinkUDFHelper helper = instance.prepare(
                        function -> new FlinkUDFHelper(function, paramRowType, outputRowType, compiler));
                return new FlinkUDFWrapperContext(
                        instance,
                        Schema.fromJSON(paramsSchemaJson),
                        Schema.fromJSON(outputSchemaJson),
                        paramRowType,
                        outputRowType,
                        helper,
                        argumentLiterals,
                        argumentNulls,
                        argumentValues);
            }
        }
    }

    private static <T> T requireType(Object value, Class<T> expected, String field) {
        if (!expected.isInstance(value)) {
            throw new IllegalArgumentException("调用描述字段 " + field + " 期望 " + expected.getName() + ": "
                    + value.getClass().getName());
        }
        return expected.cast(value);
    }

    @SuppressWarnings("unchecked")
    private static List<Boolean> readBooleanList(Object value, String field) {
        List<Boolean> list = (List<Boolean>) requireType(value, List.class, field);
        for (Boolean element : list) {
            if (element == null) {
                throw new IllegalArgumentException("调用描述字段 " + field + " 含 null 元素");
            }
        }
        return list;
    }

    private static List<Object> readObjectList(Object value, String field) {
        return (List<Object>) requireType(value, List.class, field);
    }

    private static final class DescriptorObjectInputStream extends ObjectInputStream {
        private final ClassLoader classLoader;

        private DescriptorObjectInputStream(byte[] descriptor, ClassLoader classLoader) throws IOException {
            super(new ByteArrayInputStream(descriptor));
            this.classLoader = classLoader;
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass descriptor) throws IOException, ClassNotFoundException {
            return Class.forName(descriptor.getName(), false, classLoader);
        }
    }
}
