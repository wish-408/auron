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
package org.apache.auron.flink.table.data;

import static org.apache.flink.util.Preconditions.checkArgument;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.*;
import org.apache.flink.table.data.binary.TypedSetters;
import org.apache.flink.table.data.columnar.vector.BytesColumnVector;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.flink.types.RowKind;

/**
 * A Columnar {@link RowData} implementation that supports Auron System Columns.
 */
@Internal
public class AuronColumnarRowData implements RowData, TypedSetters {

    private RowKind rowKind;
    private VectorizedColumnBatch vectorizedColumnBatch;
    private int rowId;
    private int dataColStartIndex = 0;

    private static final MethodHandle INTERNAL_STRING_CONSTRUCTOR = internalStringConstructor();

    /** 获取 Java 11 的包私有 String(byte[], byte) 构造器 */
    private static MethodHandle internalStringConstructor() {
        try {
            Constructor<String> constructor = String.class.getDeclaredConstructor(byte[].class, byte.class);
            constructor.setAccessible(true);
            return MethodHandles.lookup().unreflectConstructor(constructor);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to access String internal constructor", e);
        }
    }

    public AuronColumnarRowData() {
        this.rowKind = RowKind.INSERT;
    }

    public AuronColumnarRowData(VectorizedColumnBatch vectorizedColumnBatch) {
        this(vectorizedColumnBatch, 0);
    }

    public AuronColumnarRowData(VectorizedColumnBatch vectorizedColumnBatch, int rowId) {
        this.rowKind = RowKind.INSERT;
        this.vectorizedColumnBatch = vectorizedColumnBatch;
        this.rowId = rowId;
    }

    public void setVectorizedColumnBatch(VectorizedColumnBatch vectorizedColumnBatch) {
        this.vectorizedColumnBatch = vectorizedColumnBatch;
        this.rowId = 0;
    }

    public void setDataColStartIndex(int dataColStartIndex) {
        checkArgument(dataColStartIndex >= 0, "dataColStartIndex should be greater than or equal to 0");
        this.dataColStartIndex = dataColStartIndex;
    }

    public int getDataColStartIndex() {
        return this.dataColStartIndex;
    }

    public void setRowId(int rowId) {
        this.rowId = rowId;
    }

    public RowKind getRowKind() {
        return this.rowKind;
    }

    public void setRowKind(RowKind kind) {
        this.rowKind = kind;
    }

    public int getArity() {
        return this.vectorizedColumnBatch.getArity() - dataColStartIndex;
    }

    public boolean isNullAt(int pos) {
        return this.vectorizedColumnBatch.isNullAt(this.rowId, pos + dataColStartIndex);
    }

    public boolean getBoolean(int pos) {
        return this.vectorizedColumnBatch.getBoolean(this.rowId, pos + dataColStartIndex);
    }

    public byte getByte(int pos) {
        return this.vectorizedColumnBatch.getByte(this.rowId, pos + dataColStartIndex);
    }

    public short getShort(int pos) {
        return this.vectorizedColumnBatch.getShort(this.rowId, pos + dataColStartIndex);
    }

    public int getInt(int pos) {
        return this.vectorizedColumnBatch.getInt(this.rowId, pos + dataColStartIndex);
    }

    public long getLong(int pos) {
        return this.vectorizedColumnBatch.getLong(this.rowId, pos + dataColStartIndex);
    }

    public float getFloat(int pos) {
        return this.vectorizedColumnBatch.getFloat(this.rowId, pos + dataColStartIndex);
    }

    public double getDouble(int pos) {
        return this.vectorizedColumnBatch.getDouble(this.rowId, pos + dataColStartIndex);
    }

    public StringData getString(int pos) {
        // 把(rowId, pos)的数据从native memory拷贝到堆上的byte
        // 注意，需要rust侧保证写入native memory的字符串已经是java的LATIN1编码
        BytesColumnVector.Bytes byteArray =
                this.vectorizedColumnBatch.getByteArray(this.rowId, pos + dataColStartIndex);
        try {
            // 零拷贝构造String对象，直接把byteArray的data传给String
            String value = (String) INTERNAL_STRING_CONSTRUCTOR.invokeExact(byteArray.data, (byte) 0);
            return StringData.fromString(value);
        } catch (Throwable ignoredException) {
            String value = new String(byteArray.data);
            return StringData.fromString(value);
        }
        // String value = new String(byteArray.data); // 会多一次从byteArray.data到String内部缓冲区的拷贝
        // return StringData.fromString(value);
        // [wsh_dev] 此处可通过 (byteArray.data, byteArray.offset, byteArray.len)构建一个String对象，返回缓存了String对象的StringData
        // 后续调用udf时会把StringData变成String，缓存了之后可以直接返回String而不用再构建新的String
        // return StringData.fromBytes(byteArray.data, byteArray.offset, byteArray.len);
    }

    public DecimalData getDecimal(int pos, int precision, int scale) {
        return this.vectorizedColumnBatch.getDecimal(this.rowId, pos + dataColStartIndex, precision, scale);
    }

    public TimestampData getTimestamp(int pos, int precision) {
        return this.vectorizedColumnBatch.getTimestamp(this.rowId, pos + dataColStartIndex, precision);
    }

    public <T> RawValueData<T> getRawValue(int pos) {
        throw new UnsupportedOperationException("RawValueData is not supported.");
    }

    public byte[] getBinary(int pos) {
        BytesColumnVector.Bytes byteArray =
                this.vectorizedColumnBatch.getByteArray(this.rowId, pos + dataColStartIndex);
        if (byteArray.len == byteArray.data.length) {
            return byteArray.data;
        } else {
            byte[] ret = new byte[byteArray.len];
            System.arraycopy(byteArray.data, byteArray.offset, ret, 0, byteArray.len);
            return ret;
        }
    }

    public RowData getRow(int pos, int numFields) {
        return this.vectorizedColumnBatch.getRow(this.rowId, pos + dataColStartIndex);
    }

    public ArrayData getArray(int pos) {
        return this.vectorizedColumnBatch.getArray(this.rowId, pos + dataColStartIndex);
    }

    public MapData getMap(int pos) {
        return this.vectorizedColumnBatch.getMap(this.rowId, pos + dataColStartIndex);
    }

    public void setNullAt(int pos) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    public void setBoolean(int pos, boolean value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    public void setByte(int pos, byte value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    public void setShort(int pos, short value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    public void setInt(int pos, int value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    public void setLong(int pos, long value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    public void setFloat(int pos, float value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    public void setDouble(int pos, double value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    public void setDecimal(int pos, DecimalData value, int precision) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    public void setTimestamp(int pos, TimestampData value, int precision) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    public boolean equals(Object o) {
        throw new UnsupportedOperationException(
                "AuronColumnarRowData do not support equals, please compare fields one by one!");
    }

    public int hashCode() {
        throw new UnsupportedOperationException(
                "AuronColumnarRowData do not support hashCode, please hash fields one by one!");
    }
}
