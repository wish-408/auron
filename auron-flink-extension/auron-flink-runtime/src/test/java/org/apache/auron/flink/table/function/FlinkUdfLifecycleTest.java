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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.junit.jupiter.api.Test;

class FlinkUdfLifecycleTest {
    private static final ClassLoader LOADER = FlinkUdfLifecycleTest.class.getClassLoader();
    private static final FunctionContext FUNCTION_CONTEXT = new FunctionContext(null, LOADER, new Configuration());

    @Test
    void deduplicatesExactDescriptorsAndSelectsLastNativeRegistration() throws Exception {
        RecordingFactory factory = new RecordingFactory();
        FlinkUdfPayload last = payload("calc", 9, "pkg.Function", 9);
        FlinkUdfPayload first = payload("calc", 1, "pkg.Function", 1);
        FlinkUdfPayload duplicate = payload("calc", 7, "pkg.Function", 9);
        try (FlinkUdfLifecycle lifecycle = new FlinkUdfLifecycle()) {
            List<FlinkUdfToken> tokens = bind(lifecycle, factory, first, last, duplicate, last);
            assertEquals(tokens.get(1), tokens.get(2));
            assertEquals(tokens.get(1), tokens.get(3));
            assertNotEquals(tokens.get(0), tokens.get(1));
            assertEquals(Collections.singletonList(last), factory.resolved);
            assertEquals("function_pkg$Function", factory.instances.get(0).getFieldName());
            assertSame(factory.instances.get(0), factory.instances.get(1));
            assertEquals(1, factory.functions.get(0).opens);
            assertSame(UdfRegistry.lookup(tokens.get(0)), UdfRegistry.lookup(tokens.get(0)));
            UdfRegistry.lookup(tokens.get(0)).eval(0, 0);
            UdfRegistry.lookup(tokens.get(1)).eval(0, 0);
            assertEquals(2, factory.functions.get(0).calls.get());
            List<FlinkUdfToken> rebound = bind(lifecycle, factory, first);
            assertEquals(tokens.get(0), rebound.get(0));
            assertEquals(2, factory.contexts.size());
            assertEquals(1, factory.functions.get(0).opens);
        }
        assertEquals(1, factory.functions.get(0).closes);
        assertEquals(1, factory.contexts.get(0).closes);
        assertEquals(1, factory.contexts.get(1).closes);
    }

    @Test
    void bindRejectsFreshLifecycleBeforeOpenAndBind() throws Exception {
        FlinkUdfLifecycle lifecycle = new FlinkUdfLifecycle();
        try {
            assertThrows(IllegalStateException.class, () -> lifecycle.bind(new RecordingGroupContext()));
            assertEquals(FlinkUdfLifecycle.State.OPENING, lifecycle.getState());
        } finally {
            lifecycle.close();
        }
    }

    @Test
    void bindRejectsClosedLifecycle() throws Exception {
        FlinkUdfLifecycle lifecycle = new FlinkUdfLifecycle();
        lifecycle.close();

        assertThrows(IllegalStateException.class, () -> lifecycle.bind(new RecordingGroupContext()));
    }

    @Test
    void bindAllocatesDistinctGroupTokenAfterChildTokensAndWrapsGroupContext() throws Exception {
        RecordingFactory factory = new RecordingFactory();
        FlinkUdfLifecycle lifecycle = new FlinkUdfLifecycle();
        try {
            List<FlinkUdfToken> childTokens = bind(lifecycle, factory, payload("s", 0, "F", 1));
            RecordingGroupContext context = new RecordingGroupContext();

            FlinkUdfToken groupToken = lifecycle.bind(context);

            assertNotEquals(childTokens.get(0), groupToken);
            assertEquals(1L, groupToken.getContextId());
            UdfRegistry.lookup(groupToken).eval(0, 0);
            assertEquals(1, context.evalCalls.get());
        } finally {
            lifecycle.close();
        }
    }

    @Test
    void firstFieldRegistrationOrdersLifecycleWhileLastRegistrationSelectsCopy() throws Exception {
        List<String> trace = new ArrayList<>();
        FlinkUdfPayload firstA = payload("s", 0, "A", 0);
        FlinkUdfPayload b = payload("s", 1, "B", 1);
        FlinkUdfPayload lastA = payload("s", 2, "A", 2);
        FlinkUdfLifecycle.ContextFactory factory = new FlinkUdfLifecycle.ContextFactory() {
            @Override
            public ScalarFunction resolveFunction(FlinkUdfPayload payload, ClassLoader classLoader) {
                String copy = payload.getFunctionIdentifier() + payload.getRegistrationOrder();
                trace.add("resolve " + copy);
                return new ScalarFunction() {
                    @Override
                    public void open(FunctionContext context) {
                        trace.add("open " + copy);
                    }

                    @Override
                    public void close() {
                        trace.add("close " + copy);
                    }
                };
            }

            @Override
            public FlinkRowLevelUdfContext createContext(FlinkUdfPayload payload, FlinkUdfInstance instance) {
                return new TestContext(instance, null);
            }
        };
        try (FlinkUdfLifecycle lifecycle = new FlinkUdfLifecycle()) {
            lifecycle.openAndBind(Arrays.asList(lastA, b, firstA), FUNCTION_CONTEXT, LOADER, factory);
            assertEquals(Arrays.asList("resolve A2", "resolve B1", "open A2", "open B1"), trace);
        }
        assertEquals(Arrays.asList("resolve A2", "resolve B1", "open A2", "open B1", "close A2", "close B1"), trace);
    }

    @Test
    void scopesAndOwnersAreIsolatedAndHashCollisionsDoNotMerge() throws Exception {
        RecordingFactory factory = new RecordingFactory();
        FlinkUdfPayload a = new FlinkUdfPayload("a", 0, "pkg.F", new byte[] {0, 31});
        FlinkUdfPayload collision = new FlinkUdfPayload("a", 1, "pkg.F", new byte[] {1, 0});
        FlinkUdfPayload b = new FlinkUdfPayload("b", 0, "pkg.F", new byte[] {0, 31});
        assertEquals(Arrays.hashCode(a.getCallDescriptor()), Arrays.hashCode(collision.getCallDescriptor()));
        try (FlinkUdfLifecycle first = new FlinkUdfLifecycle();
                FlinkUdfLifecycle second = new FlinkUdfLifecycle()) {
            List<FlinkUdfToken> tokens = bind(first, factory, a, collision, b);
            List<FlinkUdfToken> other = bind(second, factory, a);
            assertNotEquals(tokens.get(0), tokens.get(1));
            assertNotEquals(tokens.get(0), tokens.get(2));
            assertNotEquals(tokens.get(0), other.get(0));
            assertNotSame(factory.instances.get(0), factory.instances.get(2));
            assertNotSame(factory.functions.get(0), factory.functions.get(1));
            assertNotEquals(first.getOwner(), second.getOwner());
        }
        for (TestFunction function : factory.functions) {
            assertEquals(1, function.opens);
            assertEquals(1, function.closes);
        }
    }

    @Test
    void rejectsAmbiguousOrdersAndInconsistentIdentifiersBeforeAllocation() {
        RecordingFactory factory = new RecordingFactory();
        FlinkUdfLifecycle orders = new FlinkUdfLifecycle();
        assertThrows(
                IllegalArgumentException.class,
                () -> bind(orders, factory, payload("s", 1, "F", 1), payload("s", 1, "F", 2)));
        assertEquals(FlinkUdfLifecycle.State.CLOSED, orders.getState());
        FlinkUdfLifecycle identifiers = new FlinkUdfLifecycle();
        assertThrows(
                IllegalArgumentException.class,
                () -> bind(identifiers, factory, payload("s", 1, "F", 1), payload("s", 2, "G", 1)));
        assertTrue(factory.functions.isEmpty());
    }

    @Test
    void sharedInstanceSerializesDifferentContexts() throws Exception {
        RecordingFactory factory = new RecordingFactory();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        factory.evalEntered = entered;
        factory.evalRelease = release;
        ExecutorService executor = Executors.newFixedThreadPool(2);
        FlinkUdfLifecycle lifecycle = new FlinkUdfLifecycle();
        try {
            List<FlinkUdfToken> tokens = bind(lifecycle, factory, payload("s", 0, "F", 1), payload("s", 1, "F", 2));
            FlinkRowLevelUdfContext first = UdfRegistry.lookup(tokens.get(0));
            FlinkRowLevelUdfContext second = UdfRegistry.lookup(tokens.get(1));
            Future<?> running = executor.submit(() -> first.eval(0, 0));
            await(entered);
            CountDownLatch attempted = new CountDownLatch(1);
            Future<?> waiting = executor.submit(() -> {
                attempted.countDown();
                second.eval(0, 0);
            });
            await(attempted);
            assertThrows(TimeoutException.class, () -> waiting.get(100, TimeUnit.MILLISECONDS));
            assertEquals(1, factory.functions.get(0).calls.get());
            release.countDown();
            running.get(5, TimeUnit.SECONDS);
            waiting.get(5, TimeUnit.SECONDS);
            assertEquals(1, factory.functions.get(0).maxConcurrent.get());
        } finally {
            release.countDown();
            lifecycle.close();
            executor.shutdownNow();
        }
    }

    @Test
    void stopReturnsDuringEvalRejectsLateReferencesAndCloseWaits() throws Exception {
        RecordingFactory factory = new RecordingFactory();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        factory.evalEntered = entered;
        factory.evalRelease = release;
        ExecutorService executor = Executors.newFixedThreadPool(3);
        FlinkUdfLifecycle lifecycle = new FlinkUdfLifecycle();
        try {
            FlinkUdfToken token =
                    bind(lifecycle, factory, payload("s", 0, "F", 1)).get(0);
            FlinkRowLevelUdfContext retained = UdfRegistry.lookup(token);
            Future<?> evaluation = executor.submit(() -> retained.eval(0, 0));
            await(entered);
            executor.submit(lifecycle::stop).get(5, TimeUnit.SECONDS);
            assertEquals(FlinkUdfLifecycle.State.STOPPING, lifecycle.getState());
            assertThrows(IllegalStateException.class, () -> UdfRegistry.lookup(token));
            assertThrows(IllegalStateException.class, () -> retained.eval(0, 0));
            CountDownLatch closeStarted = new CountDownLatch(1);
            Future<?> closing = executor.submit(() -> {
                closeStarted.countDown();
                lifecycle.close();
                return null;
            });
            await(closeStarted);
            assertThrows(TimeoutException.class, () -> closing.get(100, TimeUnit.MILLISECONDS));
            assertEquals(0, factory.contexts.get(0).closes);
            assertEquals(0, factory.functions.get(0).closes);
            release.countDown();
            evaluation.get(5, TimeUnit.SECONDS);
            closing.get(5, TimeUnit.SECONDS);
            lifecycle.close();
            assertEquals(1, factory.contexts.get(0).closes);
            assertEquals(1, factory.functions.get(0).closes);
            assertThrows(IllegalStateException.class, () -> UdfRegistry.lookup(token));
            assertThrows(IllegalStateException.class, () -> retained.eval(0, 0));
        } finally {
            release.countDown();
            lifecycle.close();
            executor.shutdownNow();
        }
    }

    @Test
    void stopDuringOpenCannotRepublishAndCloseCanMarkStoppingBeforeOwnerLock() throws Exception {
        RecordingFactory factory = new RecordingFactory();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        factory.openEntered = entered;
        factory.openRelease = release;
        ExecutorService executor = Executors.newFixedThreadPool(2);
        FlinkUdfLifecycle lifecycle = new FlinkUdfLifecycle();
        try {
            Future<?> opening = executor.submit(() -> bind(lifecycle, factory, payload("s", 0, "F", 1)));
            await(entered);
            executor.submit(lifecycle::stop).get(5, TimeUnit.SECONDS);
            Future<?> closing = executor.submit(() -> {
                lifecycle.close();
                return null;
            });
            assertEquals(FlinkUdfLifecycle.State.STOPPING, lifecycle.getState());
            release.countDown();
            ExecutionException failure = assertThrows(ExecutionException.class, () -> opening.get(5, TimeUnit.SECONDS));
            assertInstanceOf(IllegalStateException.class, failure.getCause());
            closing.get(5, TimeUnit.SECONDS);
            assertEquals(FlinkUdfLifecycle.State.CLOSED, lifecycle.getState());
            assertEquals(1, factory.functions.get(0).closes);
            assertThrows(
                    IllegalStateException.class, () -> UdfRegistry.lookup(new FlinkUdfToken(lifecycle.getOwner(), 0)));
        } finally {
            release.countDown();
            lifecycle.close();
            executor.shutdownNow();
        }
    }

    @Test
    void contextFactoryFailureClosesAllAllocatedFunctionsAndContexts() throws Exception {
        RecordingFactory factory = new RecordingFactory();
        factory.failContextAt = 1;
        factory.functionCloseFailure = new Exception("function close");
        factory.contextCloseFailure = new IllegalStateException("context close");
        FlinkUdfLifecycle lifecycle = new FlinkUdfLifecycle();
        Exception failure = assertThrows(
                Exception.class, () -> bind(lifecycle, factory, payload("s", 0, "F", 1), payload("s", 1, "G", 2)));
        assertSame(factory.factoryFailure, failure);
        assertEquals(3, failure.getSuppressed().length);
        assertEquals(0, factory.functions.get(0).opens);
        for (TestFunction function : factory.functions) {
            assertEquals(1, function.closes);
        }
        assertEquals(1, factory.contexts.get(0).closes);
        assertEquals(FlinkUdfLifecycle.State.CLOSED, lifecycle.getState());
        lifecycle.close();
        assertThrows(IllegalStateException.class, () -> bind(lifecycle, factory, payload("s", 0, "F", 1)));
    }

    @Test
    void resolveFailureClosesPreviouslyAllocatedFunction() throws Exception {
        RecordingFactory factory = new RecordingFactory();
        factory.failResolveAt = 1;
        FlinkUdfLifecycle lifecycle = new FlinkUdfLifecycle();
        assertSame(
                factory.factoryFailure,
                assertThrows(
                        Exception.class,
                        () -> bind(lifecycle, factory, payload("s", 0, "F", 1), payload("s", 1, "G", 2))));
        assertEquals(0, factory.functions.get(0).opens);
        assertEquals(1, factory.functions.get(0).closes);
        assertTrue(factory.contexts.isEmpty());
        lifecycle.close();
    }

    @Test
    void openFailurePreservesOriginalAndClosesEvenUnopenedFunctions() throws Exception {
        RecordingFactory factory = new RecordingFactory();
        factory.failOpenAt = 1;
        factory.functionCloseFailure = new Exception("function close");
        factory.contextCloseFailure = new IllegalStateException("context close");
        FlinkUdfLifecycle lifecycle = new FlinkUdfLifecycle();
        Exception failure = assertThrows(
                Exception.class,
                () -> bind(
                        lifecycle, factory, payload("s", 0, "F", 1), payload("s", 1, "G", 2), payload("s", 2, "H", 3)));
        assertSame(factory.openFailure, failure);
        assertEquals(6, failure.getSuppressed().length);
        assertEquals(1, factory.functions.get(0).opens);
        assertEquals(1, factory.functions.get(1).opens);
        assertEquals(0, factory.functions.get(2).opens);
        for (TestFunction function : factory.functions) {
            assertEquals(1, function.closes);
        }
        for (TestContext context : factory.contexts) {
            assertEquals(1, context.closes);
        }
        assertThrows(IllegalStateException.class, () -> UdfRegistry.lookup(new FlinkUdfToken(lifecycle.getOwner(), 0)));
        lifecycle.close();
    }

    @Test
    void closeFailureAggregatesContinuesRemovesRegistryAndIsIdempotent() throws Exception {
        RecordingFactory factory = new RecordingFactory();
        factory.contextCloseFailure = new IllegalStateException("context close");
        factory.functionCloseFailure = new Exception("function close");
        FlinkUdfLifecycle lifecycle = new FlinkUdfLifecycle();
        List<FlinkUdfToken> tokens = bind(lifecycle, factory, payload("s", 0, "F", 1), payload("s", 1, "G", 2));
        Exception failure = assertThrows(Exception.class, lifecycle::close);
        assertSame(factory.contextCloseFailure, failure);
        assertEquals(2, failure.getSuppressed().length);
        assertEquals(FlinkUdfLifecycle.State.CLOSED, lifecycle.getState());
        lifecycle.close();
        for (FlinkUdfToken token : tokens) {
            assertThrows(IllegalStateException.class, () -> UdfRegistry.lookup(token));
        }
        for (TestFunction function : factory.functions) {
            assertEquals(1, function.closes);
        }
        for (TestContext context : factory.contexts) {
            assertEquals(1, context.closes);
        }
    }

    @Test
    void registrationFailureRollsBackOnlyItsOwnPublishedEntries() throws Exception {
        RecordingFactory factory = new RecordingFactory();
        FlinkUdfLifecycle lifecycle = new FlinkUdfLifecycle();
        FlinkUdfToken first = new FlinkUdfToken(lifecycle.getOwner(), 0);
        FlinkUdfToken collision = new FlinkUdfToken(lifecycle.getOwner(), 1);
        FlinkRowLevelUdfContext occupied = new TestContext(null, null);
        UdfRegistry.register(collision, occupied);
        try {
            assertThrows(
                    IllegalStateException.class,
                    () -> bind(lifecycle, factory, payload("s", 0, "F", 1), payload("s", 1, "F", 2)));
            assertThrows(IllegalStateException.class, () -> UdfRegistry.lookup(first));
            assertSame(occupied, UdfRegistry.lookup(collision));
            assertEquals(1, factory.functions.get(0).closes);
            assertEquals(1, factory.contexts.get(0).closes);
            assertEquals(1, factory.contexts.get(1).closes);
            lifecycle.close();
        } finally {
            UdfRegistry.remove(collision);
        }
    }

    @Test
    void registryRejectsDuplicatesAndLookupDoesNotRemove() {
        FlinkUdfToken token = new FlinkUdfToken(java.util.UUID.randomUUID(), 0);
        FlinkRowLevelUdfContext context = new TestContext(null, null);
        UdfRegistry.register(token, context);
        try {
            assertThrows(IllegalStateException.class, () -> UdfRegistry.register(token, new TestContext(null, null)));
            assertSame(context, UdfRegistry.lookup(token));
            assertSame(context, UdfRegistry.lookup(token.registryKey()));
        } finally {
            UdfRegistry.remove(token);
        }
        assertThrows(IllegalStateException.class, () -> UdfRegistry.lookup(token));
    }

    @Test
    void instanceApiCannotPrepareAgainOrInvokeBeforePublicationOrAfterStop() throws Exception {
        RecordingFactory factory = new RecordingFactory();
        FlinkUdfLifecycle lifecycle = new FlinkUdfLifecycle();
        try {
            bind(lifecycle, factory, payload("s", 0, "F", 1));
            FlinkUdfInstance instance = factory.instances.get(0);
            assertThrows(IllegalStateException.class, () -> instance.prepare(function -> null));
            assertEquals(
                    7,
                    instance.invoke(function -> ((TestFunction) function).eval(7))
                            .intValue());
            lifecycle.stop();
            assertThrows(IllegalStateException.class, () -> instance.invoke(function -> null));
        } finally {
            lifecycle.close();
        }
    }

    @Test
    void nullFunctionContextFailsBeforeAnyAllocation() throws Exception {
        RecordingFactory factory = new RecordingFactory();
        FlinkUdfLifecycle lifecycle = new FlinkUdfLifecycle();
        assertThrows(
                NullPointerException.class,
                () -> lifecycle.openAndBind(Collections.singletonList(payload("s", 0, "F", 1)), null, LOADER, factory));
        assertTrue(factory.functions.isEmpty());
        assertEquals(FlinkUdfLifecycle.State.CLOSED, lifecycle.getState());
        lifecycle.close();
    }

    @Test
    void closeAloneStopsAnInProgressOpenBeforeWaitingForOwnerLock() throws Exception {
        RecordingFactory factory = new RecordingFactory();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        factory.openEntered = entered;
        factory.openRelease = release;
        ExecutorService executor = Executors.newFixedThreadPool(2);
        FlinkUdfLifecycle lifecycle = new FlinkUdfLifecycle();
        try {
            Future<?> opening = executor.submit(() -> bind(lifecycle, factory, payload("s", 0, "F", 1)));
            await(entered);
            CountDownLatch closeStarted = new CountDownLatch(1);
            Future<?> closing = executor.submit(() -> {
                closeStarted.countDown();
                lifecycle.close();
                return null;
            });
            await(closeStarted);
            assertThrows(TimeoutException.class, () -> closing.get(100, TimeUnit.MILLISECONDS));
            assertEquals(FlinkUdfLifecycle.State.STOPPING, lifecycle.getState());
            release.countDown();
            ExecutionException failure = assertThrows(ExecutionException.class, () -> opening.get(5, TimeUnit.SECONDS));
            assertInstanceOf(IllegalStateException.class, failure.getCause());
            closing.get(5, TimeUnit.SECONDS);
            assertEquals(1, factory.functions.get(0).closes);
            assertEquals(1, factory.contexts.get(0).closes);
            assertEquals(FlinkUdfLifecycle.State.CLOSED, lifecycle.getState());
        } finally {
            release.countDown();
            lifecycle.close();
            executor.shutdownNow();
        }
    }

    @Test
    void emptyPlanDoesNotNeedFactoryOrRegisterAndCannotReopen() throws Exception {
        FlinkUdfLifecycle lifecycle = new FlinkUdfLifecycle();
        assertTrue(
                lifecycle.openAndBind(Collections.emptyList(), null, null, null).isEmpty());
        assertEquals(FlinkUdfLifecycle.State.RUNNING, lifecycle.getState());
        assertThrows(IllegalStateException.class, () -> UdfRegistry.lookup(new FlinkUdfToken(lifecycle.getOwner(), 0)));
        lifecycle.close();
        lifecycle.close();
        assertEquals(FlinkUdfLifecycle.State.CLOSED, lifecycle.getState());
        assertThrows(
                IllegalStateException.class, () -> lifecycle.openAndBind(Collections.emptyList(), null, null, null));
        FlinkUdfLifecycle stopped = new FlinkUdfLifecycle();
        stopped.stop();
        assertThrows(IllegalStateException.class, () -> stopped.openAndBind(Collections.emptyList(), null, null, null));
        stopped.close();
    }

    private static FlinkUdfPayload payload(String scope, long order, String identifier, int descriptor) {
        return new FlinkUdfPayload(scope, order, identifier, new byte[] {(byte) descriptor});
    }

    private static List<FlinkUdfToken> bind(
            FlinkUdfLifecycle lifecycle, RecordingFactory factory, FlinkUdfPayload... payloads) throws Exception {
        return lifecycle.openAndBind(Arrays.asList(payloads), FUNCTION_CONTEXT, LOADER, factory);
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        assertTrue(latch.await(5, TimeUnit.SECONDS), "等待并发测试事件超时");
    }

    public static final class TestFunction extends ScalarFunction {
        private final AtomicInteger calls = new AtomicInteger();
        private final AtomicInteger concurrent = new AtomicInteger();
        private final AtomicInteger maxConcurrent = new AtomicInteger();
        private int opens;
        private int closes;
        private Exception openFailure;
        private Exception closeFailure;
        private CountDownLatch openEntered;
        private CountDownLatch openRelease;
        private CountDownLatch evalEntered;
        private CountDownLatch evalRelease;

        @Override
        public void open(FunctionContext context) throws Exception {
            assertSame(FUNCTION_CONTEXT, context);
            opens++;
            if (openEntered != null) {
                openEntered.countDown();
                await(openRelease);
            }
            if (openFailure != null) {
                throw openFailure;
            }
        }

        public Integer eval(Integer value) throws InterruptedException {
            int active = concurrent.incrementAndGet();
            maxConcurrent.accumulateAndGet(active, Math::max);
            try {
                calls.incrementAndGet();
                if (evalEntered != null) {
                    evalEntered.countDown();
                    await(evalRelease);
                }
                return value;
            } finally {
                concurrent.decrementAndGet();
            }
        }

        @Override
        public void close() throws Exception {
            assertEquals(0, concurrent.get());
            closes++;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    private static final class TestContext implements FlinkRowLevelUdfContext {
        private final FlinkUdfInstance instance;
        private final RuntimeException closeFailure;
        private int closes;

        private TestContext(FlinkUdfInstance instance, RuntimeException closeFailure) {
            this.instance = instance;
            this.closeFailure = closeFailure;
        }

        @Override
        public void eval(long inputPtr, long outputPtr) {
            try {
                instance.invoke(function -> ((TestFunction) function).eval((int) inputPtr));
            } catch (RuntimeException error) {
                throw error;
            } catch (Exception error) {
                throw new IllegalStateException(error);
            }
        }

        @Override
        public Object evalValue(RowData input) {
            return null;
        }

        @Override
        public FlinkUdfBoundaryMetadata metadata() {
            return new FlinkUdfBoundaryMetadata(null, null, null, null);
        }

        @Override
        public void close() {
            closes++;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    private static final class RecordingGroupContext implements FlinkRowLevelUdfContext {
        private final AtomicInteger evalCalls = new AtomicInteger();

        @Override
        public void eval(long inputPtr, long outputPtr) {
            evalCalls.incrementAndGet();
        }

        @Override
        public Object evalValue(RowData input) {
            evalCalls.incrementAndGet();
            return null;
        }

        @Override
        public FlinkUdfBoundaryMetadata metadata() {
            return new FlinkUdfBoundaryMetadata(null, null, null, null);
        }
    }

    private static final class RecordingFactory implements FlinkUdfLifecycle.ContextFactory {
        private final List<FlinkUdfPayload> resolved = new ArrayList<>();
        private final List<TestFunction> functions = new ArrayList<>();
        private final List<FlinkUdfInstance> instances = new ArrayList<>();
        private final List<TestContext> contexts = new ArrayList<>();
        private final Exception factoryFailure = new Exception("factory");
        private final Exception openFailure = new Exception("open");
        private int failResolveAt = -1;
        private int failContextAt = -1;
        private int failOpenAt = -1;
        private Exception functionCloseFailure;
        private RuntimeException contextCloseFailure;
        private CountDownLatch openEntered;
        private CountDownLatch openRelease;
        private CountDownLatch evalEntered;
        private CountDownLatch evalRelease;

        @Override
        public ScalarFunction resolveFunction(FlinkUdfPayload payload, ClassLoader classLoader) throws Exception {
            assertSame(LOADER, classLoader);
            if (functions.size() == failResolveAt) {
                throw factoryFailure;
            }
            TestFunction function = new TestFunction();
            function.openFailure = functions.size() == failOpenAt ? openFailure : null;
            function.closeFailure = functionCloseFailure;
            function.openEntered = openEntered;
            function.openRelease = openRelease;
            function.evalEntered = evalEntered;
            function.evalRelease = evalRelease;
            functions.add(function);
            resolved.add(payload);
            return function;
        }

        @Override
        public FlinkRowLevelUdfContext createContext(FlinkUdfPayload payload, FlinkUdfInstance instance)
                throws Exception {
            if (contexts.size() == failContextAt) {
                throw factoryFailure;
            }
            assertThrows(IllegalStateException.class, () -> instance.invoke(function -> null));
            instance.prepare(function -> {
                assertInstanceOf(TestFunction.class, function);
                return null;
            });
            TestContext context = new TestContext(instance, contextCloseFailure);
            instances.add(instance);
            contexts.add(context);
            return context;
        }
    }
}
