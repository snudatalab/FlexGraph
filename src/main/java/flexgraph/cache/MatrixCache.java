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
import flexgraph.types.IntIntPairWritable;

import java.io.IOException;

/**
 * An interface for matrix caches.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public interface MatrixCache<V extends Writable> {
    /**
     * Puts a column with multiple row values into this cache.
     *
     * @param col a integer pair that consists of column index,
     *            and number of nonzero elements in the column.
     * @param rows row elements
     */
    void put(final IntIntPairWritable col, final V rows) throws IOException;

    /**
     * Returns an iterator that traverses all the elements in this cache.
     *
     * @return An MatrixIterator
     */
    MatrixIterator<V> iterator() throws IOException;

    /**
     * Freezes this matrix cache.
     *
     * After calling this method, calling put() method is ignored.
     */
    void freeze();
}
