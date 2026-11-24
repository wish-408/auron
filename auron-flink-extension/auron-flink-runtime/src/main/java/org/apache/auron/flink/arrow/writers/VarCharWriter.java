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
package org.apache.auron.flink.arrow.writers;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import org.apache.arrow.vector.VarCharVector;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;

/**
 * {@link ArrowFieldWriter} for variable-length strings ({@link VarCharVector}).
 *
 * <p>Use {@link #forRow(VarCharVector)} when writing from {@link RowData} and {@link
 * #forArray(VarCharVector)} when writing from {@link ArrayData}.
 *
 * @param <T> the input data type
 */
public abstract class VarCharWriter<T> extends ArrowFieldWriter<T> {

    private static final MethodHandle STRING_VALUE_HANDLE = stringFieldHandle("value");
    private static final MethodHandle STRING_CODER_HANDLE = stringFieldHandle("coder");

    /** Creates a VarCharWriter that reads from {@link RowData}. */
    public static VarCharWriter<RowData> forRow(VarCharVector varCharVector) {
        return new VarCharWriterForRow(varCharVector);
    }

    /** Creates a VarCharWriter that reads from {@link ArrayData}. */
    public static VarCharWriter<ArrayData> forArray(VarCharVector varCharVector) {
        return new VarCharWriterForArray(varCharVector);
    }

    private VarCharWriter(VarCharVector varCharVector) {
        super(varCharVector);
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract StringData readString(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        VarCharVector vector = (VarCharVector) getValueVector();
        if (isNullAt(in, ordinal)) {
            vector.setNull(getCount());
        } else {
            writeString(vector, readString(in, ordinal));
        }
    }

    private void writeString(VarCharVector vector, StringData value) {
        // if (value instanceof BinaryStringData) {
        //     BinaryStringData binary = (BinaryStringData) value;
        //     binary.ensureMaterialized();
        //     MemorySegment[] segments = binary.getSegments();
        //     if (segments != null
        //             && segments.length == 1
        //             && binary.getOffset() >= 0
        //             && binary.getSizeInBytes() >= 0
        //             && binary.getOffset() + binary.getSizeInBytes() <= segments[0].size()) {
        //         ByteBuffer buffer = segmentBuffer(segments[0]);
        //         if (buffer != null) {
        //             vector.setSafe(getCount(), buffer, binary.getOffset(), binary.getSizeInBytes());
        //             return;
        //         }
        //     }
        // }
        // byte[] bytes = value.toBytes();
        // vector.setSafe(getCount(), bytes);

        // [wsh_dev] 直接把String的数据边解码边写入到Native Memory
        if (value instanceof BinaryStringData) {
            String javaString = ((BinaryStringData) value).getJavaObject();

            int rowId = getCount();
            if (javaString != null) {
                // 专门路径：
                // 1. 先保证 Arrow data buffer 容量足够
                // 2. 拿到 vector 的 data buffer
                // 3. 把 javaString 直接 UTF-8 encode 进去
                // 4. 手动设置 offset / validity / lastSet

                vector.setIndexDefined(rowId); // 先保证 offset/validity 容量，并把当前行标记为有效
                vector.fillEmpties(rowId); // 把前面因 null 造成的 hole 补齐
                int startOffset = vector.getStartOffset(rowId); // 获取将要写入的数据会从什么偏移量开始写

                int charLength = javaString.length(); // 字符数
                boolean ascii = isAscii(javaString);
                int byteLength = ascii ? charLength : 3 * charLength; // 纯ASCII字符串占用的字节数=字符数，否则预留字符数*3的空间
                long requiredCapacity = (long) startOffset + byteLength;
                if (requiredCapacity > vector.getDataBuffer().capacity()) { // 需要扩容
                    vector.reallocDataBuffer(requiredCapacity);
                }
                // 获取将要写入数据的buffer
                ByteBuffer buffer = vector.getDataBuffer().nioBuffer(startOffset, byteLength);
                // 实际编码转换后写入的字节数
                byteLength = writeUtf8(javaString, buffer, ascii);

                // 手动更新value的长度
                vector.setValueLengthSafe(rowId, byteLength);
                return;
            }

            // javaString == null 时，优先走原来的 materialized 路径
            BinaryStringData binary = (BinaryStringData) value;
            binary.ensureMaterialized();
            MemorySegment[] segments = binary.getSegments();
            if (segments != null
                    && segments.length == 1
                    && binary.getOffset() >= 0
                    && binary.getSizeInBytes() >= 0
                    && binary.getOffset() + binary.getSizeInBytes() <= segments[0].size()) {
                ByteBuffer buffer = segmentBuffer(segments[0]);
                if (buffer != null) {
                    vector.setSafe(rowId, buffer, binary.getOffset(), binary.getSizeInBytes());
                    return;
                }
            }
        }
        // 非 BinaryStringData 或没有 javaString 时，走 fallback
        byte[] bytes = value.toBytes();
        vector.setSafe(getCount(), bytes);
    }

    // 将String对象的数据按照UTF-8编码拷贝到native memory
    private static int writeUtf8(String value, ByteBuffer buffer, boolean ascii) {
        int length = value.length();

        if (STRING_VALUE_HANDLE != null) {
            try {
                if (ascii) { // ASCII fast path
                    byte[] bytes = (byte[]) STRING_VALUE_HANDLE.invokeExact(value);
                    buffer.put(bytes, 0, length);
                    return length;
                }
            } catch (Throwable ignored) {
                // fall back to standard UTF-8 encoding
            }
        }

        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        buffer.put(bytes, 0, bytes.length);
        return bytes.length;
    }

    /**
     * 判断 String 内容是否全是 ASCII。
     *
     * <p>这里直接读取 Java 11 Compact Strings 的内部存储：
     * coder=0 表示 Latin1，内部 byte[] 每个字节对应一个字符；
     * coder=1 不可能是纯ASCII字符串
     *
     * <p>ByteBuffer.wrap(data) 只是包装数组，不会发生数据拷贝。
     */
    private static boolean isAscii(String value) {
        if (STRING_VALUE_HANDLE != null && STRING_CODER_HANDLE != null) {
            try {
                byte[] data = (byte[]) STRING_VALUE_HANDLE.invokeExact(value);
                byte coder = (byte) STRING_CODER_HANDLE.invokeExact(value);
                ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.nativeOrder());

                long utf8HighBitMask = 0x8080808080808080L;

                if (coder == 0) {
                    // Latin1：只要任意 byte >= 0x80，就不是 ASCII
                    int start = 0;
                    int end = data.length;

                    while (start + 32 <= end) {
                        long w0 = buffer.getLong(start);
                        long w1 = buffer.getLong(start + 8);
                        long w2 = buffer.getLong(start + 16);
                        long w3 = buffer.getLong(start + 24);

                        if (((w0 | w1 | w2 | w3) & utf8HighBitMask) != 0L) {
                            return false;
                        }
                        start += 32;
                    }

                    while (start + Long.BYTES <= end) {
                        long word = buffer.getLong(start);
                        if ((word & utf8HighBitMask) != 0L) {
                            return false;
                        }
                        start += Long.BYTES;
                    }

                    while (start < end) {
                        if ((data[start] & 0x80) != 0) {
                            return false;
                        }
                        start++;
                    }

                    return true;
                }
                // 理论上 Java 11 只会出现 coder=0 / coder=1
                return false;
            } catch (Throwable e) {
                throw new IllegalStateException("Failed to inspect String internals", e);
            }
        }
        return false;
    }

    private static MethodHandle stringFieldHandle(String name) {
        try {
            Field field = String.class.getDeclaredField(name);
            field.setAccessible(true);
            return MethodHandles.lookup().unreflectGetter(field);
        } catch (ReflectiveOperationException | RuntimeException e) {
            return null;
        }
    }

    private static ByteBuffer segmentBuffer(MemorySegment segment) {
        if (segment.isOffHeap()) {
            ByteBuffer buffer = segment.getOffHeapBuffer();
            return buffer == null ? null : buffer.duplicate();
        }
        try {
            return segment.wrap(0, segment.size());
        } catch (UnsupportedOperationException ignored) {
            return null;
        }
    }

    // ---- Inner classes for RowData and ArrayData ----

    public static final class VarCharWriterForRow extends VarCharWriter<RowData> {
        private VarCharWriterForRow(VarCharVector varCharVector) {
            super(varCharVector);
        }

        @Override
        boolean isNullAt(RowData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        StringData readString(RowData in, int ordinal) {
            return in.getString(ordinal);
        }
    }

    public static final class VarCharWriterForArray extends VarCharWriter<ArrayData> {
        private VarCharWriterForArray(VarCharVector varCharVector) {
            super(varCharVector);
        }

        @Override
        boolean isNullAt(ArrayData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        StringData readString(ArrayData in, int ordinal) {
            return in.getString(ordinal);
        }
    }
}
