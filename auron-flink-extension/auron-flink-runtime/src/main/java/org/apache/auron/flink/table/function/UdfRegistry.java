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

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public final class UdfRegistry {
    private static final ConcurrentHashMap<String, FlinkRowLevelUdfContext> CONTEXTS = new ConcurrentHashMap<>();

    private UdfRegistry() {}

    public static void register(FlinkUdfToken token, FlinkRowLevelUdfContext context) {
        register(Objects.requireNonNull(token, "token").registryKey(), context);
    }

    public static void register(String key, FlinkRowLevelUdfContext context) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(context, "context");
        if (CONTEXTS.putIfAbsent(key, context) != null) {
            throw new IllegalStateException("UDF token 已注册: " + key);
        }
    }

    public static FlinkRowLevelUdfContext lookup(FlinkUdfToken token) {
        return lookup(Objects.requireNonNull(token, "token").registryKey());
    }

    public static FlinkRowLevelUdfContext lookup(String key) {
        FlinkRowLevelUdfContext context = CONTEXTS.get(Objects.requireNonNull(key, "key"));
        if (context == null) {
            throw new IllegalStateException("UDF token 未注册或已移除: " + key);
        }
        if (context instanceof FlinkUdfLifecycle.ControlledContext) {
            ((FlinkUdfLifecycle.ControlledContext) context).checkRunning();
        }
        return context;
    }

    public static void remove(FlinkUdfToken token) {
        remove(Objects.requireNonNull(token, "token").registryKey());
    }

    public static void remove(String key) {
        CONTEXTS.remove(Objects.requireNonNull(key, "key"));
    }
}
