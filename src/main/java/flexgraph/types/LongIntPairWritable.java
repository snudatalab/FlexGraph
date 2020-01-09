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
 * A writable class that represents an (long, integer) pair.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class LongIntPairWritable implements WritableComparable<LongIntPairWritable> {
    private long id;
    private int value;

    public LongIntPairWritable() {
        id = -1;
        value = -1;
    }

    public LongIntPairWritable(long id, int value) {
        this.id = id;
        this.value = value;
    }

    public final long getId() {
        return id;
    }

    public final void setId(final long id) {
        this.id = id;
    }

    public final int getValue() {
        return value;
    }

    public final void setValue(final int value) {
        this.value = value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        out.writeInt(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        value = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LongIntPairWritable other = (LongIntPairWritable) o;

        return id == other.id && value == other.value;
    }

    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + value;
        return result;
    }

    @Override
    public int compareTo(LongIntPairWritable other) {
        return id < other.id ? -1 : id > other.id ? 1 : 0;
    }

    static {
        WritableComparator.define(LongIntPairWritable.class, new TypeUtils.LongComparator(LongIntPairWritable.class));
    }
}
