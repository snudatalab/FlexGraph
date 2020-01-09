/*
 * Copyright 2018 SNU Data Mining Lab.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package flexgraph.types;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.hadoop.io.Writable;
import flexgraph.Constants;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A writable class that represents an array of (long, double) pairs.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class LongDoublePairArrayWritable implements Writable {
    private long[] longs;
    private double[] doubles;

    private int begin;
    private int end;

    public LongDoublePairArrayWritable() {
        begin = end = 0;
        longs = new long[Constants.InitArrayWritableSize];
        doubles = new double[Constants.InitArrayWritableSize];
    }

    public LongDoublePairArrayWritable(final long[] longs, final double[] doubles) {
        this.longs = longs;
        this.doubles = doubles;
        begin = 0;
        end = longs.length;
    }

    public final int size() {
        return end - begin;
    }

    public final long[] getLongs() {
        return longs;
    }

    public final double[] getDoubles() {
        return doubles;
    }

    public final int begin() {
        return begin;
    }

    public final int end() {
        return end;
    }

    public final long getLong(final int i) {
        return longs[begin + i];
    }

    public final double getDouble(final int i) {
        return doubles[begin + i];
    }

    public final void set(final long longValue, final double doubleValue) {
        longs[0] = longValue;
        doubles[0] = doubleValue;
        begin = 0;
        end = 1;
    }

    public final void set(final long[] longs, final double[] doubles, final int begin, final int end) {
        this.longs = longs;
        this.doubles = doubles;
        this.begin = begin;
        this.end = end;
    }

    public final void set(final LongArrayList longs, final DoubleArrayList doubles) {
        this.longs = longs.elements();
        this.doubles = doubles.elements();
        this.begin = 0;
        this.end = longs.size();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        final int size = size();
        if (size > 1) {
            out.writeLong(longs[begin] | Long.MIN_VALUE);
            out.writeDouble(doubles[begin]);
            out.writeInt(size - 1);
            for (int i = begin + 1; i < end; ++i) {
                out.writeLong(longs[i]);
                out.writeDouble(doubles[i]);
            }
        } else {
            out.writeLong(longs[begin]);
            out.writeDouble(doubles[begin]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        final long headLong = in.readLong();
        final double headDouble = in.readDouble();
        if ((headLong & Long.MIN_VALUE) != 0) {
            end = in.readInt() + 1;
            if (end > longs.length) {
                longs = new long[end];
                doubles = new double[end];
            }
            begin = 0;
            longs[0] = headLong & Long.MAX_VALUE;
            doubles[0] = headDouble;
            for (int i = 1; i < end; ++i) {
                longs[i] = in.readLong();
                doubles[i] = in.readDouble();
            }
        } else {
            longs[0] = headLong;
            doubles[0] = headDouble;
            begin = 0;
            end = 1;
        }
    }
}
