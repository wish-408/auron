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
import java.util.function.BooleanSupplier;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

public final class FlinkUdfInstance {
    @FunctionalInterface
    public interface FunctionCall<T> {
        T call(ScalarFunction function) throws Exception;
    }

    private final String scope;
    private final String fieldName;
    private final ScalarFunction function;
    private final BooleanSupplier stopping;
    private final BooleanSupplier running;
    private boolean openAttempted;
    private boolean opened;
    private boolean closed;

    FlinkUdfInstance(
            String scope,
            String fieldName,
            ScalarFunction function,
            BooleanSupplier stopping,
            BooleanSupplier running) {
        this.scope = Objects.requireNonNull(scope, "scope");
        this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
        this.function = Objects.requireNonNull(function, "function");
        this.stopping = Objects.requireNonNull(stopping, "stopping");
        this.running = Objects.requireNonNull(running, "running");
    }

    public String getScope() {
        return scope;
    }

    public String getFieldName() {
        return fieldName;
    }

    public synchronized <T> T prepare(FunctionCall<T> call) throws Exception {
        checkNotStopped();
        if (openAttempted) {
            throw new IllegalStateException("UDF 已开始初始化，不能重新准备调用适配器");
        }
        return Objects.requireNonNull(call, "call").call(function);
    }

    public synchronized <T> T invoke(FunctionCall<T> call) throws Exception {
        checkCallable();
        return Objects.requireNonNull(call, "call").call(function);
    }

    synchronized void open(FunctionContext context) throws Exception {
        checkNotStopped();
        if (openAttempted) {
            throw new IllegalStateException("UDF 不允许重复 open");
        }
        openAttempted = true;
        function.open(Objects.requireNonNull(context, "context"));
        opened = true;
        checkNotStopped();
    }

    synchronized void eval(Runnable call) {
        checkCallable();
        call.run();
    }

    synchronized Throwable close(List<FlinkRowLevelUdfContext> contexts, Throwable failure) {
        if (closed) {
            return failure;
        }
        closed = true;
        for (FlinkRowLevelUdfContext context : contexts) {
            try {
                context.close();
            } catch (Throwable error) {
                failure = FlinkUdfLifecycle.aggregate(failure, error);
            }
        }
        try {
            function.close();
        } catch (Throwable error) {
            failure = FlinkUdfLifecycle.aggregate(failure, error);
        }
        return failure;
    }

    private void checkCallable() {
        checkNotStopped();
        if (!opened || !running.getAsBoolean()) {
            throw new IllegalStateException("UDF 尚未成功 open");
        }
    }

    private void checkNotStopped() {
        if (closed || stopping.getAsBoolean()) {
            throw new IllegalStateException("UDF 已停止或关闭: " + scope + "/" + fieldName);
        }
    }
}
