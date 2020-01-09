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

import java.io.IOException;

/**
 * An interface for matrix iterator.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public interface MatrixIterator<V extends Writable> extends AutoCloseable {
    /**
     * Moves this iterator to next element.
     *
     * @return false if there is no next element, otherwise true
     */
    boolean next() throws IOException;

    /**
     * Returns column index of current element.
     *
     * @return column index
     */
    int currentCol();

    /**
     * Returns the number of nonzero elements in current column.
     *
     * @return the number of nonzero elements
     */
    int numNonzeros();

    /**
     * Returns the row values in current column.
     *
     * @return the container with row values in current column.
     */
    V currentRows();
}
