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
package org.apache.auron.flink.table.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.auron.flink.table.runtime.UdfInvocationTraceFixture.Event;
import org.apache.auron.flink.table.runtime.UdfInvocationTraceFixture.Phase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.util.FlinkUserCodeClassLoaders;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.MutableURLClassLoader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class UdfInvocationTraceFixtureTest {

    private UUID traceId;

    @BeforeEach
    public void createTrace() {
        traceId = UdfInvocationTraceFixture.newTrace();
    }

    @AfterEach
    public void removeTrace() {
        UdfInvocationTraceFixture.removeTrace(traceId);
    }

    @Test
    public void testNullableOverloadsAndConfigurationWithManualLifecycle() throws Exception {
        UdfInvocationTraceFixture function = new UdfInvocationTraceFixture(traceId, 7);
        assertThat(function.isDeterministic()).isFalse();
        function.open(context());
        assertThat(function.eval((Integer) null)).isNull();
        assertThat(function.eval(2)).isEqualTo(9);
        assertThat(function.eval((String) null)).isNull();
        assertThat(function.eval("value")).isEqualTo("value:7");
        function.close();

        List<Event> events = UdfInvocationTraceFixture.snapshot(traceId);
        assertThat(events)
                .extracting(Event::getPhase)
                .containsExactly(Phase.OPEN, Phase.EVAL, Phase.EVAL, Phase.EVAL, Phase.EVAL, Phase.CLOSE);
        assertThat(events).extracting(Event::getInvocationSequence).containsExactly(1L, 2L, 3L, 4L, 5L, 6L);
        assertThat(events)
                .extracting(Event::getRunId)
                .containsOnly(events.get(0).getRunId());
        assertThat(events)
                .extracting(Event::getThreadId)
                .containsOnly(Thread.currentThread().getId());
        assertThat(events.get(0).getSharedTraceClass()).isTrue();
        assertThat(events.get(0).getArgument()).isNull();
        assertThat(events.get(5).getResult()).isNull();
        assertThat(events.subList(1, 5))
                .extracting(Event::getArgumentType)
                .containsExactly(Integer.class, Integer.class, String.class, String.class);
        assertThat(events.subList(1, 5)).extracting(Event::getArgument).containsExactly(null, 2, null, "value");
        assertThat(events.subList(1, 5)).extracting(Event::getResult).containsExactly(null, 9, null, "value:7");
    }

    @Test
    public void testStateAcrossTwoManualInvocationBatches() throws Exception {
        UdfInvocationTraceFixture function = new UdfInvocationTraceFixture(traceId, 3);
        function.open(context());
        for (Integer value : Arrays.asList(1, null)) {
            function.eval(value);
        }
        List<Event> firstBatch = UdfInvocationTraceFixture.snapshot(traceId);
        assertThat(firstBatch).extracting(Event::getInvocationSequence).containsExactly(1L, 2L, 3L);
        for (Integer value : Arrays.asList(2, 4)) {
            function.eval(value);
        }
        function.close();

        List<Event> events = UdfInvocationTraceFixture.snapshot(traceId);
        assertThat(firstBatch).hasSize(3);
        assertThat(events)
                .extracting(Event::getPhase)
                .containsExactly(Phase.OPEN, Phase.EVAL, Phase.EVAL, Phase.EVAL, Phase.EVAL, Phase.CLOSE);
        assertThat(events).extracting(Event::getInvocationSequence).containsExactly(1L, 2L, 3L, 4L, 5L, 6L);
        assertThat(events)
                .extracting(Event::getRunId)
                .containsOnly(firstBatch.get(0).getRunId());
        assertThat(events.subList(1, 5)).extracting(Event::getArgument).containsExactly(1, null, 2, 4);
        assertThat(events.subList(1, 5)).extracting(Event::getResult).containsExactly(4, null, 5, 7);
    }

    @Test
    public void testSerializedCopiesHaveIndependentManualLifecycleState() throws Exception {
        UdfInvocationTraceFixture original = new UdfInvocationTraceFixture(traceId, 11);
        original.open(context());
        assertThat(original.eval(1)).isEqualTo(12);
        byte[] serialized = InstantiationUtil.serializeObject(original);
        UdfInvocationTraceFixture first =
                InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
        UdfInvocationTraceFixture second =
                InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
        first.open(context());
        second.open(context());
        assertThat(first.eval(2)).isEqualTo(13);
        assertThat(second.eval(3)).isEqualTo(14);
        assertThat(original.eval(4)).isEqualTo(15);
        first.close();
        second.close();
        original.close();

        List<Event> events = UdfInvocationTraceFixture.snapshot(traceId);
        assertThat(events.stream().map(Event::getRunId).distinct().count()).isEqualTo(3);
        List<List<Event>> runs = new ArrayList<>(
                events.stream().collect(Collectors.groupingBy(Event::getRunId)).values());
        assertThat(runs).allSatisfy(run -> {
            assertThat(run.get(0).getPhase()).isEqualTo(Phase.OPEN);
            assertThat(run.get(run.size() - 1).getPhase()).isEqualTo(Phase.CLOSE);
            for (int i = 0; i < run.size(); i++) {
                assertThat(run.get(i).getInvocationSequence()).isEqualTo(i + 1L);
            }
        });
        assertThat(events.stream().filter(event -> event.getPhase() == Phase.EVAL))
                .extracting(Event::getArgument)
                .containsExactly(1, 2, 3, 4);
        assertThat(events.stream().filter(event -> event.getPhase() == Phase.EVAL))
                .extracting(Event::getResult)
                .containsExactly(12, 13, 14, 15);
        assertThat(runs).extracting(List::size).containsExactlyInAnyOrder(4, 3, 3);
    }

    @Test
    public void testConcurrentManualInvocationsRemainOrdered() throws Exception {
        UdfInvocationTraceFixture function = new UdfInvocationTraceFixture(traceId, 5);
        function.open(context());
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int worker = 0; worker < 2; worker++) {
                futures.add(executor.submit(() -> {
                    for (int value = 0; value < 20; value++) {
                        assertThat(function.eval(value)).isEqualTo(value + 5);
                    }
                }));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        } finally {
            executor.shutdownNow();
            function.close();
        }
        List<Event> events = UdfInvocationTraceFixture.snapshot(traceId);
        assertThat(events).hasSize(42);
        assertThat(events.get(0).getPhase()).isEqualTo(Phase.OPEN);
        assertThat(events.get(41).getPhase()).isEqualTo(Phase.CLOSE);
        assertThat(events.subList(1, 41)).allSatisfy(event -> {
            assertThat(event.getPhase()).isEqualTo(Phase.EVAL);
            assertThat(event.getThreadId()).isNotEqualTo(Thread.currentThread().getId());
            assertThat(event.getResult()).isEqualTo((Integer) event.getArgument() + 5);
        });
        assertThat(events)
                .extracting(Event::getRunId)
                .containsOnly(events.get(0).getRunId());
        for (int i = 0; i < events.size(); i++) {
            assertThat(events.get(i).getInvocationSequence()).isEqualTo(i + 1L);
        }
    }

    @Test
    public void testLocalChildFirstClassLoaderSharesStaticTrace() throws Exception {
        ClassLoader parent = getClass().getClassLoader();
        try (MutableURLClassLoader loader =
                FlinkUserCodeClassLoaders.childFirst(new URL[0], parent, new String[0], null, true)) {
            assertThat(loader.loadClass(UdfInvocationTraceFixture.class.getName()))
                    .isSameAs(UdfInvocationTraceFixture.class);
            UdfInvocationTraceFixture function = InstantiationUtil.deserializeObject(
                    InstantiationUtil.serializeObject(new UdfInvocationTraceFixture(traceId, 2)), loader);
            function.open(new FunctionContext(null, loader, new Configuration()));
            assertThat(function.eval(1)).isEqualTo(3);
            function.close();
        }
        List<Event> events = UdfInvocationTraceFixture.snapshot(traceId);
        assertThat(events).extracting(Event::getPhase).containsExactly(Phase.OPEN, Phase.EVAL, Phase.CLOSE);
        assertThat(events.get(0).getSharedTraceClass()).isTrue();
    }

    private FunctionContext context() {
        return new FunctionContext(null, getClass().getClassLoader(), new Configuration());
    }
}
