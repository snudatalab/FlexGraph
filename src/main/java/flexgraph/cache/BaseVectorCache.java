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

import org.apache.hadoop.io.Writable;

/**
 * A base implementation for VectorCache interface.
 *
 * @see flexgraph.cache.VectorCache
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public abstract class BaseVectorCache<V extends Writable> implements VectorCache<V> {
    private BitSet containsSet;
    protected int size;
    protected int numBlocks;
    protected long numVertices;
    protected int blockId;

    public BaseVectorCache(final int numBlocks, final int blockId, final long numVertices) {
        this.numBlocks = numBlocks;
        this.numVertices = numVertices;
        this.blockId = blockId;
        this.size = (int) Math.ceil(numVertices / (double) numBlocks);
        this.containsSet = new BitSet(size);
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean contains(final int i) {
        return containsSet.contains(i);
    }

    @Override
    public void put(int i, V value) {
        containsSet.add(i);
    }

    @Override
    public void addTo(int i, V value) {
        containsSet.add(i);
    }

    @Override
    public void clear() {
        containsSet.clear();
    }
}
