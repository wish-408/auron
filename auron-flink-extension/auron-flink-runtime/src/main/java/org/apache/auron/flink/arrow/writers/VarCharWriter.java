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

import java.nio.ByteBuffer;
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
        if (value instanceof BinaryStringData) {
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
                    vector.setSafe(getCount(), buffer, binary.getOffset(), binary.getSizeInBytes());
                    return;
                }
            }
        }
        byte[] bytes = value.toBytes();
        vector.setSafe(getCount(), bytes);
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
