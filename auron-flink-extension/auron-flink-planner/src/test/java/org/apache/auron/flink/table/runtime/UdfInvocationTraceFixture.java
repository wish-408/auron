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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

public class UdfInvocationTraceFixture extends ScalarFunction {

    private static final long serialVersionUID = 1L;
    private static final ConcurrentMap<UUID, ConcurrentLinkedQueue<Event>> TRACES = new ConcurrentHashMap<>();

    private final UUID traceId;
    private final int addend;
    private transient UUID runId;
    private transient long invocationSequence;

    public UdfInvocationTraceFixture(UUID traceId, int addend) {
        this.traceId = Objects.requireNonNull(traceId);
        this.addend = addend;
    }

    public static UUID newTrace() {
        UUID traceId = UUID.randomUUID();
        TRACES.put(traceId, new ConcurrentLinkedQueue<>());
        return traceId;
    }

    public static List<Event> snapshot(UUID traceId) {
        ConcurrentLinkedQueue<Event> events = TRACES.get(traceId);
        return events == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(events));
    }

    public static void removeTrace(UUID traceId) {
        TRACES.remove(traceId);
    }

    @Override
    public synchronized void open(FunctionContext context) throws Exception {
        Objects.requireNonNull(context);
        super.open(context);
        boolean sharedTraceClass =
                context.getUserCodeClassLoader().loadClass(getClass().getName()) == getClass();
        record(Phase.OPEN, null, null, null, sharedTraceClass);
    }

    public synchronized Integer eval(Integer value) {
        Integer result = value == null ? null : value + addend;
        record(Phase.EVAL, Integer.class, value, result, null);
        return result;
    }

    public synchronized String eval(String value) {
        String result = value == null ? null : value + ":" + addend;
        record(Phase.EVAL, String.class, value, result, null);
        return result;
    }

    @Override
    public synchronized void close() throws Exception {
        super.close();
        record(Phase.CLOSE, null, null, null, null);
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }

    private void record(Phase phase, Class<?> argumentType, Object argument, Object result, Boolean sharedTraceClass) {
        if (runId == null) {
            runId = UUID.randomUUID();
        }
        Event event = new Event(
                runId,
                phase,
                Thread.currentThread().getId(),
                argumentType,
                argument,
                result,
                ++invocationSequence,
                sharedTraceClass);
        ConcurrentLinkedQueue<Event> events = TRACES.get(traceId);
        if (events != null) {
            events.add(event);
        }
    }

    public enum Phase {
        OPEN,
        EVAL,
        CLOSE
    }

    public static final class Event {
        private final UUID runId;
        private final Phase phase;
        private final long threadId;
        private final Class<?> argumentType;
        private final Object argument;
        private final Object result;
        private final long invocationSequence;
        private final Boolean sharedTraceClass;

        private Event(
                UUID runId,
                Phase phase,
                long threadId,
                Class<?> argumentType,
                Object argument,
                Object result,
                long invocationSequence,
                Boolean sharedTraceClass) {
            this.runId = runId;
            this.phase = phase;
            this.threadId = threadId;
            this.argumentType = argumentType;
            this.argument = argument;
            this.result = result;
            this.invocationSequence = invocationSequence;
            this.sharedTraceClass = sharedTraceClass;
        }

        public UUID getRunId() {
            return runId;
        }

        public Phase getPhase() {
            return phase;
        }

        public long getThreadId() {
            return threadId;
        }

        public Class<?> getArgumentType() {
            return argumentType;
        }

        public Object getArgument() {
            return argument;
        }

        public Object getResult() {
            return result;
        }

        public long getInvocationSequence() {
            return invocationSequence;
        }

        public Boolean getSharedTraceClass() {
            return sharedTraceClass;
        }
    }
}
