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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashSet;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class FlinkUdfTokenTest {

    @Test
    public void testRoundTripAndValueEquality() {
        for (UUID owner : new UUID[] {new UUID(0, 0), new UUID(-1, -1), new UUID(Long.MIN_VALUE, Long.MAX_VALUE)}) {
            for (long id : new long[] {0, 1, Long.MAX_VALUE}) {
                FlinkUdfToken token = new FlinkUdfToken(owner, id);
                byte[] encoded = token.encode();
                assertEquals(32, encoded.length);
                FlinkUdfToken decoded = FlinkUdfToken.decode(ByteBuffer.wrap(encoded));
                assertEquals(owner, decoded.getOwner());
                assertEquals(id, decoded.getContextId());
                assertEquals(token, decoded);
                assertEquals(token.hashCode(), decoded.hashCode());
                assertEquals(token.registryKey(), decoded.registryKey());
                assertArrayEquals(encoded, decoded.encode());
                assertEquals(1, new HashSet<>(Arrays.asList(token, decoded)).size());
            }
        }
    }

    @Test
    public void testRegistryNamespace() {
        UUID firstOwner = new UUID(1, 2);
        UUID secondOwner = new UUID(1, 3);
        FlinkUdfToken first = new FlinkUdfToken(firstOwner, 12);
        FlinkUdfToken differentOwner = new FlinkUdfToken(secondOwner, 12);
        FlinkUdfToken differentId = new FlinkUdfToken(firstOwner, 123);
        assertEquals("flink-udf:" + firstOwner + ":12", first.registryKey());
        assertEquals(
                3,
                new HashSet<>(Arrays.asList(
                                first.registryKey(), differentOwner.registryKey(), differentId.registryKey()))
                        .size());
        assertNotEquals(first.registryKey(), "12");
        assertNotEquals(first, differentOwner);
        assertNotEquals(first, differentId);
        assertEquals(first, first);
        assertNotEquals(first, null);
        assertNotEquals(first, first.registryKey());
    }

    @Test
    public void testImmutability() {
        FlinkUdfToken token = new FlinkUdfToken(new UUID(1, 2), 3);
        byte[] original = token.encode();
        byte[] encoded = token.encode();
        FlinkUdfToken decoded = FlinkUdfToken.decode(ByteBuffer.wrap(encoded));
        int hash = token.hashCode();
        Arrays.fill(encoded, (byte) 0);
        assertArrayEquals(original, token.encode());
        assertEquals(token, decoded);
        assertEquals(hash, token.hashCode());
        assertArrayEquals(original, decoded.encode());
    }

    @Test
    public void testBufferWindowAndState() {
        FlinkUdfToken token = new FlinkUdfToken(new UUID(1, 2), 3);
        byte[] encoded = token.encode();
        for (boolean direct : new boolean[] {false, true}) {
            ByteBuffer storage = direct ? ByteBuffer.allocateDirect(42) : ByteBuffer.allocate(42);
            storage.position(3);
            storage.put(encoded).putInt(123);
            storage.limit(35).position(3);
            assertWindowUnchanged(storage.order(ByteOrder.LITTLE_ENDIAN), token);
            assertWindowUnchanged(storage.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN), token);
            assertWindowUnchanged(storage.slice().order(ByteOrder.LITTLE_ENDIAN), token);
        }
    }

    @Test
    public void testConstructorValidation() {
        assertThrows(NullPointerException.class, () -> new FlinkUdfToken(null, 0));
        assertThrows(IllegalArgumentException.class, () -> new FlinkUdfToken(new UUID(0, 0), -1));
        assertThrows(IllegalArgumentException.class, () -> new FlinkUdfToken(new UUID(0, 0), Long.MIN_VALUE));
        assertThrows(NullPointerException.class, () -> FlinkUdfToken.decode(null));
    }

    @Test
    public void testInvalidFraming() {
        byte[] valid = new FlinkUdfToken(new UUID(1, 2), 3).encode();
        for (int length = 0; length < valid.length; length++) {
            assertInvalid(Arrays.copyOf(valid, length));
        }
        assertInvalid(Arrays.copyOf(valid, valid.length + 1));
        assertInvalid(Arrays.copyOf(valid, valid.length * 2));
        for (int offset : new int[] {0, 4}) {
            for (int value : new int[] {0, -1, 2}) {
                byte[] invalid = valid.clone();
                ByteBuffer.wrap(invalid).putInt(offset, value);
                assertInvalid(invalid);
            }
        }
        byte[] negativeId = valid.clone();
        ByteBuffer.wrap(negativeId).putLong(24, -1);
        assertInvalid(negativeId);
        assertInvalid(new FlinkUdfPayload("s", 0, "i", new byte[2]).encode());
    }

    private static void assertWindowUnchanged(ByteBuffer buffer, FlinkUdfToken expected) {
        int position = buffer.position();
        int limit = buffer.limit();
        ByteOrder order = buffer.order();
        buffer.mark();
        assertEquals(expected, FlinkUdfToken.decode(buffer));
        assertEquals(position, buffer.position());
        assertEquals(limit, buffer.limit());
        assertEquals(order, buffer.order());
        buffer.reset();
        assertEquals(position, buffer.position());
    }

    private static void assertInvalid(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(bytes.length + 4).order(ByteOrder.LITTLE_ENDIAN);
        buffer.position(2);
        buffer.put(bytes);
        buffer.limit(bytes.length + 2).position(2).mark();
        assertThrows(IllegalArgumentException.class, () -> FlinkUdfToken.decode(buffer));
        assertEquals(2, buffer.position());
        assertEquals(bytes.length + 2, buffer.limit());
        assertEquals(ByteOrder.LITTLE_ENDIAN, buffer.order());
        buffer.reset();
        assertEquals(2, buffer.position());
    }
}
