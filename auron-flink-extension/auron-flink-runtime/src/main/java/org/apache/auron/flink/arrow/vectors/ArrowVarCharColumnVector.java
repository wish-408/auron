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
package org.apache.auron.flink.arrow.vectors;

import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.flink.table.data.columnar.vector.BytesColumnVector;
import org.apache.flink.util.Preconditions;

/**
 * A Flink {@link BytesColumnVector} backed by an Arrow {@link VarCharVector}.
 *
 * <p>String values are materialized once per batch into a single contiguous {@code byte[]}, and
 * each row is then served as an offset/length slice of that array. This avoids the per-row
 * {@code byte[]} allocation and copy performed by {@link VarCharVector#get(int)} and keeps the
 * slice copy-free: callers that wrap the slice (e.g. {@code StringData.fromBytes}) share the same
 * batch array instead of duplicating every value.
 */
public final class ArrowVarCharColumnVector implements BytesColumnVector {

    private final VarCharVector vector;
    private byte[] materialized;
    private int[] starts;
    private int[] lengths;

    /**
     * Creates a new wrapper around the given Arrow {@link VarCharVector}.
     *
     * @param vector the Arrow vector to wrap, must not be null
     */
    public ArrowVarCharColumnVector(VarCharVector vector) {
        this.vector = Preconditions.checkNotNull(vector);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isNullAt(int i) {
        return vector.isNull(i);
    }

    /** {@inheritDoc} */
    @Override
    public Bytes getBytes(int i) {
        // [wsh_dev] 替换成每个数据逐个拷贝而不是一次性拷贝后取切片
        // materializeIfNeeded();
        // return new Bytes(materialized, starts[i], lengths[i]);

        if (vector.isNull(i)) {
            return new Bytes(new byte[0], 0, 0);
        }
        int start = vector.getStartOffset(i);
        int end = vector.getEndOffset(i);
        int length = end - start;
        byte[] data = new byte[length];
        // 把arrow中的数据拷贝到堆上的byte[]
        vector.getDataBuffer().getBytes(start, data, 0, length);

        return new Bytes(data, 0, length);
    }

    /**
     * Copies the whole string column into one {@code byte[]} the first time a value is read. All
     * rows of the current batch share that array; rows only reference their slice by offset/length.
     */
    private void materializeIfNeeded() {
        if (materialized != null) {
            return;
        }
        int valueCount = vector.getValueCount();
        NullableVarCharHolder holder = new NullableVarCharHolder();
        int totalBytes = 0;
        for (int i = 0; i < valueCount; i++) {
            vector.get(i, holder);
            if (holder.isSet != 0) {
                totalBytes += holder.end - holder.start;
            }
        }
        byte[] all = new byte[totalBytes];
        int[] starts = new int[valueCount];
        int[] lengths = new int[valueCount];
        int position = 0;
        for (int i = 0; i < valueCount; i++) {
            vector.get(i, holder);
            if (holder.isSet != 0) {
                int length = holder.end - holder.start;
                holder.buffer.getBytes(holder.start, all, position, length);
                starts[i] = position;
                lengths[i] = length;
                position += length;
            } else {
                starts[i] = position;
                lengths[i] = 0;
            }
        }
        this.materialized = all;
        this.starts = starts;
        this.lengths = lengths;
    }
}
