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
 * An interface for vector cache.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public interface VectorCache<V extends Writable> {
    /**
     * Returns the size of this vector cache.
     *
     * @return the size of cache
     */
    int size();

    /**
     * Returns the i-th value of this vector.
     *
     * @param i index
     *
     * @return the i-th value of this vector
     */
    V get(final int i);

    /**
     * Checks whether the i-th value of this vector equals zero or not.
     *
     * @param i index
     *
     * @return true if the i-th value of this vector is not zero, otherwise false
     */
    boolean contains(final int i);

    /**
     * Sets the given value to the i-th value of this vector.
     *
     * Note that we consider the i-th value is not zero even the given value is zero.
     *
     * @param i index
     * @param value the value to set
     */
    void put(final int i, final V value);

    /**
     * Adds the given value to the i-th value of this vector.
     *
     * @param i index
     * @param value the value to be added
     */
    void addTo(final int i, final V value);

    /**
     * Clears all the values in this vector.
     */
    void clear();

    /**
     * Returns a vector iterator that traverses all values in this vector.
     *
     * @param onlyNonZero if this value is true, the returned iterator traverses
     *                    only nonzero elements.
     * @param filterSet if this set is given, only indices in the set are traversed.
     */
    VectorIterator<V> iterator(boolean onlyNonZero, final BitSet filterSet);
}
