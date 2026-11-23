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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.UUID;

public final class FlinkUdfToken {

    private static final int MAGIC = 0x46554454;
    private static final int VERSION = 1;
    private static final int ENCODED_BYTES = 32;

    private final UUID owner;
    private final long contextId;

    public FlinkUdfToken(UUID owner, long contextId) {
        this.owner = Objects.requireNonNull(owner, "owner");
        if (contextId < 0) {
            throw new IllegalArgumentException("contextId 必须非负");
        }
        this.contextId = contextId;
    }

    public UUID getOwner() {
        return owner;
    }

    public long getContextId() {
        return contextId;
    }

    public byte[] encode() {
        return ByteBuffer.allocate(ENCODED_BYTES)
                .putInt(MAGIC)
                .putInt(VERSION)
                .putLong(owner.getMostSignificantBits())
                .putLong(owner.getLeastSignificantBits())
                .putLong(contextId)
                .array();
    }

    public static FlinkUdfToken decode(ByteBuffer input) {
        ByteBuffer buffer = Objects.requireNonNull(input, "input").duplicate().order(ByteOrder.BIG_ENDIAN);
        if (buffer.remaining() != ENCODED_BYTES) {
            throw new IllegalArgumentException("token 长度无效");
        }
        if (buffer.getInt() != MAGIC || buffer.getInt() != VERSION) {
            throw new IllegalArgumentException("token 标记或版本无效");
        }
        UUID owner = new UUID(buffer.getLong(), buffer.getLong());
        return new FlinkUdfToken(owner, buffer.getLong());
    }

    public String registryKey() {
        return "flink-udf:" + owner + ":" + contextId;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof FlinkUdfToken)) {
            return false;
        }
        FlinkUdfToken that = (FlinkUdfToken) other;
        return contextId == that.contextId && owner.equals(that.owner);
    }

    @Override
    public int hashCode() {
        return Objects.hash(owner, contextId);
    }
}
