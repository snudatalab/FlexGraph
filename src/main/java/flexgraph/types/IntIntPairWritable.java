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

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import flexgraph.utils.TypeUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A writable class that represents an (integer, integer) pair.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class IntIntPairWritable implements WritableComparable<IntIntPairWritable> {
    private int first;
    private int second;

    public IntIntPairWritable() {
        this(-1, -1);
    }

    public IntIntPairWritable(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(final int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(final int second) {
        this.second = second;
    }

    public void set(final int first, final int second) {
        setFirst(first);
        setSecond(second);
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeInt(second);
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        first = in.readInt();
        second = in.readInt();
    }

    @Override
    public int compareTo(final IntIntPairWritable other) {
        return first < other.first ? -1 : first > other.first ? 1 : 0;
    }

    static {
        WritableComparator.define(IntIntPairWritable.class, new TypeUtils.IntComparator(IntIntPairWritable.class));
    }
}
