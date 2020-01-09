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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.hadoop.io.Writable;
import flexgraph.Constants;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A writable class that represents an integer array.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class IntArrayWritable implements Writable {
    private int[] values;
    private int begin;
    private int end;

    public IntArrayWritable() {
        begin = end = 0;
        values = new int[Constants.InitArrayWritableSize];
    }

    public IntArrayWritable(final int value) {
        begin = 0;
        end = 1;
        values = new int[Constants.InitArrayWritableSize];
        values[0] = value;
    }

    public IntArrayWritable(final int... values) {
        begin = 0;
        end = values.length;
        this.values = values;
    }

    public final int size() {
        return end - begin;
    }

    public final int[] getValues() {
        return values;
    }

    public final int begin() {
        return begin;
    }

    public final int end() {
        return end;
    }

    public final int get(final int i) {
        return values[begin + i];
    }

    public final void set(final int value) {
        this.values[0] = value;
        begin = 0;
        end = 1;
    }

    public final void set(final int[] values) {
        this.values = values;
        begin = 0;
        end = values.length;
    }

    public final void set(final int[] values, final int begin, final int end) {
        this.values = values;
        this.begin = begin;
        this.end = end;
    }

    public final void set(final IntArrayList values) {
        this.values = values.elements();
        this.begin = 0;
        this.end = values.size();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        final int size = size();
        if (size > 1) {
            out.writeInt(values[begin] | Integer.MIN_VALUE);
            out.writeInt(size - 1);
            for (int i = begin + 1; i < end; ++i) {
                out.writeInt(values[i]);
            }
        } else {
            out.writeInt(values[begin]);
        }
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        final int head = in.readInt();
        if ((head & Integer.MIN_VALUE) != 0) {
            end = in.readInt() + 1;
            if (end > values.length) {
                values = new int[end];
            }
            begin = 0;
            values[0] = head & Integer.MAX_VALUE;
            for (int i = 1; i < end; ++i) {
                values[i] = in.readInt();
            }
        } else {
            values[0] = head;
            begin = 0;
            end = 1;
        }
    }
}
