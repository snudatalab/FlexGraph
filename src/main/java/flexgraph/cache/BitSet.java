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

import java.util.Arrays;

/**
 * A set implementation based on bitwise operations.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public final class BitSet {
    private static final long[] MASKS;
    private final long[] bits;
    private final int cardinality;

    /**
     * Creates a bitset of the given cardinality.
     */
    public BitSet(final int cardinality) {
        final int adjustedSize = (int) Math.ceil(cardinality / 64.0);
        this.cardinality = cardinality;
        this.bits = new long[adjustedSize];
    }

    /**
     * Returns the cardinality of this bitset.
     *
     * @return the cardinality
     */
    public final int getCardinality() {
        return cardinality;
    }

    /**
     * Returns the size of backed array.
     *
     * In general, the size of array is larger than cardinality / 8.
     *
     * @return the size of backed array
     */
    public final int getArraySize() {
        return bits.length;
    }

    /**
     * Checks whether this bitset contains the given id or not.
     *
     * @return true if the given id is in this bitset, otherwise false
     */
    public final boolean contains(final int id) {
        final int pos = id / 64;
        final long mask = MASKS[id % 64];

        return (bits[pos] & mask) != 0;
    }

    /**
     * Adds the given id to this bitset.
     */
    public final void add(final int id) {
        final int pos = id / 64;
        final long mask = MASKS[id % 64];

        bits[pos] |= mask;
    }

    /**
     * Removes the given id from this bitset.
     */
    public final void remove(final int id) {
        final int pos = id / 64;
        final long mask = MASKS[id % 64];

        bits[pos] &= ~mask;
    }

    /**
     * Clear all elements in this bitset.
     */
    public final void clear() {
        Arrays.fill(bits, 0L);
    }

    static {
        MASKS = new long[64];
        for (int i = 0; i < 64; ++i) {
            MASKS[i] = 1L << i;
        }
    }
}
