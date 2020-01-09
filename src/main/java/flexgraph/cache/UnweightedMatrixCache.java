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

import org.apache.hadoop.conf.Configuration;
import flexgraph.types.IntArrayWritable;
import flexgraph.types.IntIntPairWritable;

import java.io.IOException;

/**
 * A MatrixCache implementation for binary-valued matrix.
 *
 * Note that this cache stores only nonzero elements because the matrices from real-world graphs are extremely sparse.
 *
 * @see flexgraph.cache.MatrixCache
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class UnweightedMatrixCache extends BaseMatrixCache<IntArrayWritable> {
    public UnweightedMatrixCache(
            final String blockName, final Configuration conf, final int numCols, final int numRows) throws IOException {
        super(blockName, conf, numCols, numRows, 0);
    }

    @Override
    protected void putToMemory(final IntIntPairWritable col, final IntArrayWritable rows) {
        colList.add(col.getFirst());
        degrees.add(col.getSecond());
        rowList.addElements(rowList.size(), rows.getValues(), rows.begin(), rows.size());
        rowSizes.add(rowList.size());
    }

    @Override
    public MatrixIterator<IntArrayWritable> iterator() throws IOException {
        return new BaseMatrixIterator<IntArrayWritable>(path, conf) {
            private int readCol = 0;
            private int readRow = 0;
            private final IntIntPairWritable col = new IntIntPairWritable();
            private final IntArrayWritable rows = new IntArrayWritable();

            @Override
            public boolean next() throws IOException {
                if (in == null && readCol < colList.size()) {
                    // read from memory
                    col.set(colList.getInt(readCol), degrees.getInt(readCol));
                    rows.set(rowList.elements(), readRow, rowSizes.getInt(readCol));
                    ++readCol;
                    readRow += rows.size();
                    return true;
                } else if (in != null && !in.eof()) {
                    // read from disk
                    col.readFields(dataIn);
                    rows.readFields(dataIn);
                    return true;
                }
                return false;
            }

            @Override
            public int currentCol() {
                return col.getFirst();
            }

            @Override
            public int numNonzeros() {
                return col.getSecond();
            }

            @Override
            public IntArrayWritable currentRows() {
                return rows;
            }
        };
    }
}
