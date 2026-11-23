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
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

public final class FlinkUdfPayload {

    private static final int MAGIC = 0x46554450;
    private static final int VERSION = 1;
    private static final int MAX_STRING_BYTES = 64 * 1024;
    private static final int MAX_DESCRIPTOR_BYTES = 16 * 1024 * 1024;
    private static final int HEADER_BYTES = 28;
    private static final int MAX_PAYLOAD_BYTES = HEADER_BYTES + 2 * MAX_STRING_BYTES + MAX_DESCRIPTOR_BYTES;

    private final String scope;
    private final long registrationOrder;
    private final String functionIdentifier;
    private final byte[] callDescriptor;

    public FlinkUdfPayload(String scope, long registrationOrder, String functionIdentifier, byte[] callDescriptor) {
        encodeString(scope);
        encodeString(functionIdentifier);
        if (registrationOrder < 0) {
            throw new IllegalArgumentException("registrationOrder 必须非负");
        }
        Objects.requireNonNull(callDescriptor, "callDescriptor");
        if (callDescriptor.length > MAX_DESCRIPTOR_BYTES) {
            throw new IllegalArgumentException("callDescriptor 超出长度上限");
        }
        this.scope = scope;
        this.registrationOrder = registrationOrder;
        this.functionIdentifier = functionIdentifier;
        this.callDescriptor = callDescriptor.clone();
    }

    public String getScope() {
        return scope;
    }

    public long getRegistrationOrder() {
        return registrationOrder;
    }

    public String getFunctionIdentifier() {
        return functionIdentifier;
    }

    public byte[] getCallDescriptor() {
        return callDescriptor.clone();
    }

    public String getFieldName() {
        return "function_" + functionIdentifier.replace('.', '$');
    }

    public byte[] encode() {
        byte[] scopeBytes = encodeString(scope);
        byte[] identifierBytes = encodeString(functionIdentifier);
        ByteBuffer buffer =
                ByteBuffer.allocate(HEADER_BYTES + scopeBytes.length + identifierBytes.length + callDescriptor.length);
        buffer.putInt(MAGIC).putInt(VERSION).putLong(registrationOrder);
        buffer.putInt(scopeBytes.length).put(scopeBytes);
        buffer.putInt(identifierBytes.length).put(identifierBytes);
        buffer.putInt(callDescriptor.length).put(callDescriptor);
        return buffer.array();
    }

    public static FlinkUdfPayload decode(ByteBuffer input) {
        ByteBuffer buffer = Objects.requireNonNull(input, "input").duplicate().order(ByteOrder.BIG_ENDIAN);
        if (buffer.remaining() < HEADER_BYTES || buffer.remaining() > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("payload 长度无效");
        }
        if (buffer.getInt() != MAGIC || buffer.getInt() != VERSION) {
            throw new IllegalArgumentException("payload 标记或版本无效");
        }
        long registrationOrder = buffer.getLong();
        if (registrationOrder < 0) {
            throw new IllegalArgumentException("registrationOrder 必须非负");
        }
        String scope = decodeString(readBytes(buffer, MAX_STRING_BYTES));
        String identifier = decodeString(readBytes(buffer, MAX_STRING_BYTES));
        byte[] descriptor = readBytes(buffer, MAX_DESCRIPTOR_BYTES);
        if (buffer.hasRemaining()) {
            throw new IllegalArgumentException("payload 包含尾随字节");
        }
        return new FlinkUdfPayload(scope, registrationOrder, identifier, descriptor);
    }

    private static byte[] encodeString(String value) {
        Objects.requireNonNull(value, "value");
        if (value.isEmpty() || value.length() > MAX_STRING_BYTES) {
            throw new IllegalArgumentException("字符串长度无效");
        }
        try {
            ByteBuffer bytes = StandardCharsets.UTF_8
                    .newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            if (bytes.remaining() > MAX_STRING_BYTES) {
                throw new IllegalArgumentException("UTF8 字符串超出长度上限");
            }
            byte[] result = new byte[bytes.remaining()];
            bytes.get(result);
            return result;
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("字符串无法编码为严格 UTF8", e);
        }
    }

    private static String decodeString(byte[] bytes) {
        try {
            return StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("字符串不是严格 UTF8", e);
        }
    }

    private static byte[] readBytes(ByteBuffer buffer, int maximum) {
        if (buffer.remaining() < Integer.BYTES) {
            throw new IllegalArgumentException("payload 长度字段被截断");
        }
        int length = buffer.getInt();
        if (length < 0 || length > maximum || length > buffer.remaining()) {
            throw new IllegalArgumentException("payload 字段长度无效");
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof FlinkUdfPayload)) {
            return false;
        }
        FlinkUdfPayload that = (FlinkUdfPayload) other;
        return registrationOrder == that.registrationOrder
                && scope.equals(that.scope)
                && functionIdentifier.equals(that.functionIdentifier)
                && Arrays.equals(callDescriptor, that.callDescriptor);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(scope, registrationOrder, functionIdentifier) + Arrays.hashCode(callDescriptor);
    }
}
