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

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class FlinkUdfPayloadTest {

    @Test
    public void testRoundTripPreservesOriginalDescriptor() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            for (Object item : new Object[] {"function", "method", new String[] {"INT"}, "INT", true, false, 1, null}) {
                output.writeObject(item);
            }
        }
        byte[] descriptor = bytes.toByteArray();
        FlinkUdfPayload payload = new FlinkUdfPayload("作业范围", Long.MAX_VALUE, "catalog.db.函数", descriptor);
        FlinkUdfPayload decoded = FlinkUdfPayload.decode(ByteBuffer.wrap(payload.encode()));
        assertEquals("作业范围", decoded.getScope());
        assertEquals(Long.MAX_VALUE, decoded.getRegistrationOrder());
        assertEquals("catalog.db.函数", decoded.getFunctionIdentifier());
        assertEquals("function_catalog$db$函数", decoded.getFieldName());
        assertArrayEquals(descriptor, decoded.getCallDescriptor());
        assertEquals(payload, decoded);
        assertEquals(payload.hashCode(), decoded.hashCode());
        assertArrayEquals(payload.encode(), decoded.encode());
        assertArrayEquals(
                new byte[0],
                FlinkUdfPayload.decode(ByteBuffer.wrap(new FlinkUdfPayload("s", 0, "i", new byte[0]).encode()))
                        .getCallDescriptor());
    }

    @Test
    public void testDefensiveCopies() {
        byte[] descriptor = {1, 2, 3};
        FlinkUdfPayload payload = new FlinkUdfPayload("s", 0, "i", descriptor);
        byte[] encoded = payload.encode();
        int hash = payload.hashCode();
        descriptor[0] = 9;
        payload.getCallDescriptor()[1] = 9;
        payload.encode()[0] = 0;
        assertArrayEquals(new byte[] {1, 2, 3}, payload.getCallDescriptor());
        assertArrayEquals(encoded, payload.encode());
        assertEquals(hash, payload.hashCode());
        FlinkUdfPayload decoded = FlinkUdfPayload.decode(ByteBuffer.wrap(encoded));
        Arrays.fill(encoded, (byte) 0);
        assertEquals(payload, decoded);
    }

    @Test
    public void testContentIdentityDoesNotUseOnlyHash() {
        byte[] first = {0, 31};
        byte[] second = {1, 0};
        assertEquals(Arrays.hashCode(first), Arrays.hashCode(second));
        FlinkUdfPayload payload = new FlinkUdfPayload("s", 0, "i", first);
        assertEquals(payload, payload);
        assertNotEquals(payload, new FlinkUdfPayload("s", 0, "i", second));
        assertNotEquals(payload, new FlinkUdfPayload("other", 0, "i", first));
        assertNotEquals(payload, new FlinkUdfPayload("s", 1, "i", first));
        assertNotEquals(payload, new FlinkUdfPayload("s", 0, "other", first));
        assertNotEquals(payload, null);
        assertNotEquals(payload, "s");
    }

    @Test
    public void testBufferWindowAndState() {
        FlinkUdfPayload payload = new FlinkUdfPayload("scope", 7, "c.f", new byte[] {1, 2});
        byte[] encoded = payload.encode();
        for (boolean direct : new boolean[] {false, true}) {
            ByteBuffer storage =
                    direct ? ByteBuffer.allocateDirect(encoded.length + 10) : ByteBuffer.allocate(encoded.length + 10);
            storage.position(3);
            storage.put(encoded).putInt(123);
            storage.limit(3 + encoded.length).position(3);
            assertWindowUnchanged(storage.order(ByteOrder.LITTLE_ENDIAN), payload);
            assertWindowUnchanged(storage.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN), payload);
            assertWindowUnchanged(storage.slice().order(ByteOrder.LITTLE_ENDIAN), payload);
        }
    }

    @Test
    public void testConstructorValidation() {
        assertThrows(NullPointerException.class, () -> new FlinkUdfPayload(null, 0, "i", new byte[0]));
        assertThrows(NullPointerException.class, () -> new FlinkUdfPayload("s", 0, null, new byte[0]));
        assertThrows(NullPointerException.class, () -> new FlinkUdfPayload("s", 0, "i", null));
        assertThrows(IllegalArgumentException.class, () -> new FlinkUdfPayload("", 0, "i", new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> new FlinkUdfPayload("s", 0, "", new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> new FlinkUdfPayload("s", -1, "i", new byte[0]));
        String highSurrogate = String.valueOf((char) 0xD800);
        String lowSurrogate = String.valueOf((char) 0xDC00);
        assertThrows(IllegalArgumentException.class, () -> new FlinkUdfPayload(highSurrogate, 0, "i", new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> new FlinkUdfPayload("s", 0, lowSurrogate, new byte[0]));
        assertThrows(NullPointerException.class, () -> FlinkUdfPayload.decode(null));
    }

    @Test
    public void testLengthLimits() {
        char[] chars = new char[64 * 1024];
        Arrays.fill(chars, 'a');
        String maximum = new String(chars);
        byte[] descriptor = new byte[16 * 1024 * 1024];
        FlinkUdfPayload payload = new FlinkUdfPayload(maximum, 0, maximum, descriptor);
        assertEquals(payload, FlinkUdfPayload.decode(ByteBuffer.wrap(payload.encode())));
        assertThrows(IllegalArgumentException.class, () -> new FlinkUdfPayload(maximum + "a", 0, "i", new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> new FlinkUdfPayload("s", 0, maximum + "a", new byte[0]));
        Arrays.fill(chars, '中');
        String oversizedUtf8 = new String(chars);
        assertThrows(IllegalArgumentException.class, () -> new FlinkUdfPayload(oversizedUtf8, 0, "i", new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> new FlinkUdfPayload("s", 0, oversizedUtf8, new byte[0]));
        assertThrows(
                IllegalArgumentException.class,
                () -> new FlinkUdfPayload("s", 0, "i", new byte[descriptor.length + 1]));
        assertInvalid(new byte[28 + 2 * 64 * 1024 + descriptor.length + 1]);
    }

    @Test
    public void testInvalidUtf8AndEmptyStrings() {
        for (byte[] invalid : new byte[][] {
            {(byte) 0x80},
            {(byte) 0xC0, (byte) 0xAF},
            {(byte) 0xE2, (byte) 0x82},
            {(byte) 0xED, (byte) 0xA0, (byte) 0x80},
            {(byte) 0xF4, (byte) 0x90, (byte) 0x80, (byte) 0x80},
            {}
        }) {
            assertInvalid(wire(invalid, new byte[] {'i'}, new byte[0]));
            assertInvalid(wire(new byte[] {'s'}, invalid, new byte[0]));
        }
    }

    @Test
    public void testInvalidFraming() {
        byte[] valid = new FlinkUdfPayload("s", 0, "i", new byte[] {1, 2}).encode();
        for (int length = 0; length < valid.length; length++) {
            assertInvalid(Arrays.copyOf(valid, length));
        }
        assertInvalid(Arrays.copyOf(valid, valid.length + 1));
        for (int offset : new int[] {0, 4}) {
            byte[] invalid = valid.clone();
            ByteBuffer.wrap(invalid).putInt(offset, 0);
            assertInvalid(invalid);
        }
        byte[] negativeOrder = valid.clone();
        ByteBuffer.wrap(negativeOrder).putLong(8, -1);
        assertInvalid(negativeOrder);
        for (int offset : new int[] {16, 21, 26}) {
            for (int length : new int[] {-1, Integer.MAX_VALUE, 64 * 1024 + 1, 16 * 1024 * 1024 + 1}) {
                byte[] invalid = valid.clone();
                ByteBuffer.wrap(invalid).putInt(offset, length);
                assertInvalid(invalid);
            }
        }
        byte[] unsupportedVersion = valid.clone();
        ByteBuffer.wrap(unsupportedVersion).putInt(4, 2);
        assertInvalid(unsupportedVersion);
    }

    private static byte[] wire(byte[] scope, byte[] identifier, byte[] descriptor) {
        byte[] encoded = new FlinkUdfPayload("s", 0, "i", new byte[0]).encode();
        return ByteBuffer.allocate(28 + scope.length + identifier.length + descriptor.length)
                .put(encoded, 0, 16)
                .putInt(scope.length)
                .put(scope)
                .putInt(identifier.length)
                .put(identifier)
                .putInt(descriptor.length)
                .put(descriptor)
                .array();
    }

    private static void assertWindowUnchanged(ByteBuffer buffer, FlinkUdfPayload expected) {
        int position = buffer.position();
        int limit = buffer.limit();
        ByteOrder order = buffer.order();
        buffer.mark();
        assertEquals(expected, FlinkUdfPayload.decode(buffer));
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
        assertThrows(IllegalArgumentException.class, () -> FlinkUdfPayload.decode(buffer));
        assertEquals(2, buffer.position());
        assertEquals(bytes.length + 2, buffer.limit());
        assertEquals(ByteOrder.LITTLE_ENDIAN, buffer.order());
        buffer.reset();
        assertEquals(2, buffer.position());
    }
}
