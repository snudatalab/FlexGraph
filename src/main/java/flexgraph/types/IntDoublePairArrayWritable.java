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
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.hadoop.io.Writable;
import flexgraph.Constants;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A writable class that represents an array of (integer, double) pairs.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class IntDoublePairArrayWritable implements Writable {
    private int[] ints;
    private double[] doubles;

    private int begin;
    private int end;

    public IntDoublePairArrayWritable() {
        begin = end = 0;
        ints = new int[Constants.InitArrayWritableSize];
        doubles = new double[Constants.InitArrayWritableSize];
    }

    public IntDoublePairArrayWritable(final int[] ints, final double[] doubles) {
        this.ints = ints;
        this.doubles = doubles;
        begin = 0;
        end = ints.length;
    }

    public final int size() {
        return end - begin;
    }

    public final int[] getInts() {
        return ints;
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

    public final int getInt(final int i) {
        return ints[begin + i];
    }

    public final double getDouble(final int i) {
        return doubles[begin + i];
    }

    public final void set(final int intValue, final double doubleValue) {
        ints[0] = intValue;
        doubles[0] = doubleValue;
        begin = 0;
        end = 1;
    }

    public final void set(final int[] ints, final double[] doubles, final int begin, final int end) {
        this.ints = ints;
        this.doubles = doubles;
        this.begin = begin;
        this.end = end;
    }

    public final void set(final IntArrayList ints, final DoubleArrayList doubles) {
        this.ints = ints.elements();
        this.doubles = doubles.elements();
        this.begin = 0;
        this.end = ints.size();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        final int size = size();
        if (size > 1) {
            out.writeInt(ints[begin] | Integer.MIN_VALUE);
            out.writeDouble(doubles[begin]);
            out.writeInt(size - 1);
            for (int i = begin + 1; i < end; ++i) {
                out.writeInt(ints[i]);
                out.writeDouble(doubles[i]);
            }
        } else {
            out.writeInt(ints[begin]);
            out.writeDouble(doubles[begin]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        final int headInt = in.readInt();
        final double headDouble = in.readDouble();
        if ((headInt & Integer.MIN_VALUE) != 0) {
            end = in.readInt() + 1;
            if (end > ints.length) {
                ints = new int[end];
                doubles = new double[end];
            }
            begin = 0;
            ints[0] = headInt & Integer.MAX_VALUE;
            doubles[0] = headDouble;
            for (int i = 1; i < end; ++i) {
                ints[i] = in.readInt();
                doubles[i] = in.readDouble();
            }
        } else {
            ints[0] = headInt;
            doubles[0] = headDouble;
            begin = 0;
            end = 1;
        }
    }
}
