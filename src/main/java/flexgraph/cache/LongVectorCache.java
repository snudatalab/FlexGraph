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
package flexgraph.cache;

import org.apache.hadoop.io.LongWritable;
import flexgraph.utils.IDUtils;

import java.util.Arrays;

/**
 * A VectorCache implementation storing long-typed values.
 *
 * @see flexgraph.cache.VectorCache
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public final class LongVectorCache extends BaseVectorCache<LongWritable> {
    private final long[] values;
    private final long zeroValue;
    private final LongWritable value = new LongWritable();

    public LongVectorCache(
            final int numBlocks, final int blockId, final long numVertices, final LongWritable zeroValue) {
        super(numBlocks, blockId, numVertices);

        this.values = new long[size];
        this.zeroValue = zeroValue.get();
        clear();
    }

    @Override
    public final LongWritable get(final int i) {
        value.set(values[i]);
        return value;
    }

    @Override
    public final void put(final int i, final LongWritable value) {
        super.put(i, value);
        values[i] = value.get();
    }

    @Override
    public final void addTo(final int i, final LongWritable value) {
        super.addTo(i, value);
        values[i] += value.get();
    }

    @Override
    public final void clear() {
        super.clear();
        Arrays.fill(values, zeroValue);
    }

    @Override
    public final VectorIterator<LongWritable> iterator(final boolean onlyNonZero, final BitSet filterSet) {
        return new VectorIterator<LongWritable>() {
            private int pos = 0;
            private int id;
            private final LongWritable value = new LongWritable();

            @Override
            public final boolean next() {
                while (pos < size && IDUtils.localToGlobal(numBlocks, blockId, pos) < numVertices) {
                    if (contains(pos) &&
                            (filterSet == null || filterSet.contains(pos)) &&
                            (!onlyNonZero || values[pos] != zeroValue)) {
                        id = pos;
                        value.set(values[pos]);
                        ++pos;
                        return true;
                    }
                    ++pos;
                }
                return false;
            }

            @Override
            public final int currentId() {
                return id;
            }

            @Override
            public final LongWritable currentValue() {
                return value;
            }
        };
    }
}
