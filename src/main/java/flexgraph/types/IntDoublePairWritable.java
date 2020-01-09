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
 * A writable class that represents an (integer, double) pair.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class IntDoublePairWritable implements WritableComparable<IntDoublePairWritable> {
    private int intValue;
    private double doubleValue;

    public IntDoublePairWritable() {
    }

    public IntDoublePairWritable(final int intValue, final double doubleValue) {
        set(intValue, doubleValue);
    }

    public int getInt() {
        return intValue;
    }

    public double getDouble() {
        return doubleValue;
    }

    public void set(final int intValue, final double doubleValue) {
        this.intValue = intValue;
        this.doubleValue = doubleValue;
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeInt(intValue);
        out.writeDouble(doubleValue);
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        intValue = in.readInt();
        doubleValue = in.readDouble();
    }

    @Override
    public int compareTo(final IntDoublePairWritable other) {
        return intValue < other.intValue ? -1 : intValue > other.intValue ? 1 : 0;
    }

    static {
        WritableComparator.define(
                IntDoublePairWritable.class, new TypeUtils.IntComparator(IntDoublePairWritable.class));
    }
}
