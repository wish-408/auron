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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.auron.functions.AuronUDFWrapperContext;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

public final class FlinkUdfLifecycle implements AutoCloseable {
    public enum State {
        OPENING,
        RUNNING,
        STOPPING,
        CLOSED
    }

    public interface ContextFactory {
        ScalarFunction resolveFunction(FlinkUdfPayload payload, ClassLoader classLoader) throws Exception;

        AuronUDFWrapperContext createContext(FlinkUdfPayload payload, FlinkUdfInstance instance) throws Exception;
    }

    private final UUID owner = UUID.randomUUID();
    private final AtomicReference<State> state = new AtomicReference<>(State.OPENING);
    private final Map<InstanceKey, FlinkUdfInstance> instances = new LinkedHashMap<>();
    private final Map<CallKey, ControlledContext> contexts = new LinkedHashMap<>();
    private final Map<FlinkUdfInstance, List<AuronUDFWrapperContext>> delegates = new LinkedHashMap<>();
    private final List<FlinkUdfToken> registeredTokens = new ArrayList<>();
    private boolean openAttempted;

    public UUID getOwner() {
        return owner;
    }

    public State getState() {
        return state.get();
    }

    public synchronized List<FlinkUdfToken> openAndBind(
            List<FlinkUdfPayload> payloads,
            FunctionContext functionContext,
            ClassLoader classLoader,
            ContextFactory factory)
            throws Exception {
        if (openAttempted || state.get() != State.OPENING) {
            throw new IllegalStateException("UDF 生命周期不允许重复 open 或停止后 open");
        }
        openAttempted = true;
        try {
            Objects.requireNonNull(payloads, "payloads");
            if (!payloads.isEmpty()) {
                Objects.requireNonNull(functionContext, "functionContext");
                Objects.requireNonNull(classLoader, "classLoader");
                Objects.requireNonNull(factory, "factory");
            }
            Map<String, Map<Long, FlinkUdfPayload>> registrations = new LinkedHashMap<>();
            Map<InstanceKey, FlinkUdfPayload> selected = new LinkedHashMap<>();
            Map<InstanceKey, Long> firstRegistrations = new LinkedHashMap<>();
            Map<CallKey, String> identifiers = new LinkedHashMap<>();
            for (FlinkUdfPayload payload : payloads) {
                Objects.requireNonNull(payload, "payload");
                Map<Long, FlinkUdfPayload> orders =
                        registrations.computeIfAbsent(payload.getScope(), ignored -> new LinkedHashMap<>());
                FlinkUdfPayload previous = orders.putIfAbsent(payload.getRegistrationOrder(), payload);
                if (previous != null
                        && (!previous.getFunctionIdentifier().equals(payload.getFunctionIdentifier())
                                || !Arrays.equals(previous.getCallDescriptor(), payload.getCallDescriptor()))) {
                    throw new IllegalArgumentException("同一 scope 的 registrationOrder 存在歧义");
                }
                CallKey callKey = new CallKey(payload);
                String identifier = identifiers.putIfAbsent(callKey, payload.getFunctionIdentifier());
                if (identifier != null && !identifier.equals(payload.getFunctionIdentifier())) {
                    throw new IllegalArgumentException("同一 scope 与调用描述对应不同 functionIdentifier");
                }
                InstanceKey key = new InstanceKey(payload);
                firstRegistrations.merge(key, payload.getRegistrationOrder(), Math::min);
                FlinkUdfPayload current = selected.get(key);
                if (current == null || payload.getRegistrationOrder() > current.getRegistrationOrder()) {
                    selected.put(key, payload);
                }
            }
            List<FlinkUdfPayload> ordered = new ArrayList<>(selected.values());
            ordered.sort(Comparator.comparingLong(payload -> firstRegistrations.get(new InstanceKey(payload))));
            IdentityHashMap<ScalarFunction, Boolean> functions = new IdentityHashMap<>();
            for (FlinkUdfPayload payload : ordered) {
                checkOpening();
                ScalarFunction function =
                        Objects.requireNonNull(factory.resolveFunction(payload, classLoader), "resolvedFunction");
                if (functions.put(function, Boolean.TRUE) != null) {
                    throw new IllegalArgumentException("不同 scope 或字段不能共享同一个 ScalarFunction 对象");
                }
                FlinkUdfInstance instance = new FlinkUdfInstance(
                        payload.getScope(),
                        payload.getFieldName(),
                        function,
                        this::isStopping,
                        () -> state.get() == State.RUNNING);
                instances.put(new InstanceKey(payload), instance);
                delegates.put(instance, new ArrayList<>());
            }
            List<FlinkUdfToken> result = new ArrayList<>(payloads.size());
            IdentityHashMap<AuronUDFWrapperContext, Boolean> allocatedContexts = new IdentityHashMap<>();
            for (FlinkUdfPayload payload : payloads) {
                checkOpening();
                CallKey key = new CallKey(payload);
                ControlledContext context = contexts.get(key);
                if (context == null) {
                    FlinkUdfInstance instance = instances.get(new InstanceKey(payload));
                    AuronUDFWrapperContext delegate =
                            Objects.requireNonNull(factory.createContext(payload, instance), "createdContext");
                    if (allocatedContexts.put(delegate, Boolean.TRUE) != null) {
                        throw new IllegalArgumentException("不同调用适配器不能复用同一个 context 对象");
                    }
                    delegates.get(instance).add(delegate);
                    context =
                            new ControlledContext(this, instance, delegate, new FlinkUdfToken(owner, contexts.size()));
                    contexts.put(key, context);
                }
                result.add(context.token);
            }
            for (FlinkUdfInstance instance : instances.values()) {
                checkOpening();
                instance.open(functionContext);
            }
            for (ControlledContext context : contexts.values()) {
                checkOpening();
                UdfRegistry.register(context.token, context);
                registeredTokens.add(context.token);
            }
            if (!state.compareAndSet(State.OPENING, State.RUNNING)) {
                throw new IllegalStateException("UDF 初始化期间已停止");
            }
            return Collections.unmodifiableList(result);
        } catch (Throwable failure) {
            stop();
            throw propagate(cleanup(failure));
        }
    }

    /**
     * 使用 {@link FlinkUDFHelper} 提供的默认调用工厂执行 openAndBind；owner 只需准备 payload、
     * FunctionContext 与用户代码 ClassLoader。
     *
     * <p>默认工厂的反序列化与 helper 编译依赖运行时 Flink 发行版自带的 Janino；需要注入替身编译器的
     * 调用方使用四参重载。
     */
    public synchronized List<FlinkUdfToken> openAndBind(
            List<FlinkUdfPayload> payloads, FunctionContext functionContext, ClassLoader classLoader) throws Exception {
        return openAndBind(payloads, functionContext, classLoader, FlinkUDFHelper.contextFactory());
    }

    public void stop() {
        state.updateAndGet(current -> current == State.CLOSED ? State.CLOSED : State.STOPPING);
    }

    @Override
    public void close() throws Exception {
        stop();
        synchronized (this) {
            if (state.get() == State.CLOSED) {
                return;
            }
            Throwable failure = cleanup(null);
            if (failure != null) {
                throw propagate(failure);
            }
        }
    }

    private Throwable cleanup(Throwable failure) {
        try {
            for (FlinkUdfInstance instance : instances.values()) {
                failure = instance.close(delegates.get(instance), failure);
            }
        } finally {
            for (FlinkUdfToken token : registeredTokens) {
                UdfRegistry.remove(token);
            }
            registeredTokens.clear();
            contexts.clear();
            delegates.clear();
            instances.clear();
            state.set(State.CLOSED);
        }
        return failure;
    }

    private boolean isStopping() {
        State current = state.get();
        return current == State.STOPPING || current == State.CLOSED;
    }

    private void checkOpening() {
        if (state.get() != State.OPENING) {
            throw new IllegalStateException("UDF 初始化期间已停止");
        }
    }

    static Throwable aggregate(Throwable failure, Throwable error) {
        if (failure == null) {
            return error;
        }
        if (failure != error) {
            failure.addSuppressed(error);
        }
        return failure;
    }

    private static Exception propagate(Throwable failure) {
        if (failure instanceof Exception) {
            return (Exception) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        return new RuntimeException(failure);
    }

    static final class ControlledContext implements AuronUDFWrapperContext {
        private final FlinkUdfLifecycle lifecycle;
        private final FlinkUdfInstance instance;
        private final AuronUDFWrapperContext delegate;
        private final FlinkUdfToken token;

        private ControlledContext(
                FlinkUdfLifecycle lifecycle,
                FlinkUdfInstance instance,
                AuronUDFWrapperContext delegate,
                FlinkUdfToken token) {
            this.lifecycle = lifecycle;
            this.instance = instance;
            this.delegate = delegate;
            this.token = token;
        }

        void checkRunning() {
            if (lifecycle.state.get() != State.RUNNING) {
                throw new IllegalStateException("UDF 生命周期尚未运行或已停止");
            }
        }

        @Override
        public void open(String resourceId) {
            throw new IllegalStateException("UDF context 只能由生命周期初始化");
        }

        @Override
        public void eval(long inputPtr, long outputPtr) {
            checkRunning();
            instance.eval(() -> {
                checkRunning();
                delegate.eval(inputPtr, outputPtr);
            });
        }

        @Override
        public void close() {
            throw new IllegalStateException("UDF context 只能由生命周期关闭");
        }
    }

    private static final class InstanceKey {
        private final String scope;
        private final String fieldName;

        private InstanceKey(FlinkUdfPayload payload) {
            scope = payload.getScope();
            fieldName = payload.getFieldName();
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof InstanceKey)) {
                return false;
            }
            InstanceKey key = (InstanceKey) other;
            return scope.equals(key.scope) && fieldName.equals(key.fieldName);
        }

        @Override
        public int hashCode() {
            return 31 * scope.hashCode() + fieldName.hashCode();
        }
    }

    private static final class CallKey {
        private final String scope;
        private final byte[] descriptor;

        private CallKey(FlinkUdfPayload payload) {
            scope = payload.getScope();
            descriptor = payload.getCallDescriptor().clone();
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof CallKey)) {
                return false;
            }
            CallKey key = (CallKey) other;
            return scope.equals(key.scope) && Arrays.equals(descriptor, key.descriptor);
        }

        @Override
        public int hashCode() {
            return 31 * scope.hashCode() + Arrays.hashCode(descriptor);
        }
    }
}
