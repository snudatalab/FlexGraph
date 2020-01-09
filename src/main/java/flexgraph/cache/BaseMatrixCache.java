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

import com.esotericsoftware.kryo.io.KryoDataOutput;
import com.esotericsoftware.kryo.io.Output;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import flexgraph.Constants;
import flexgraph.types.IntIntPairWritable;
import flexgraph.utils.IOUtils;
import flexgraph.utils.MemoryUtils;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A base implementation of MatrixCache interface.
 *
 * @see flexgraph.cache.MatrixCache
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public abstract class BaseMatrixCache<M extends Writable> implements MatrixCache<M> {
    private static final Logger LOG = LoggerFactory.getLogger(BaseMatrixCache.class);

    protected final Configuration conf;

    protected final IntArrayList colList;
    protected final IntArrayList rowList;
    protected final IntArrayList rowSizes;
    protected final IntArrayList degrees;

    private final boolean memoryFrozen;

    protected final Path path;
    private Output out;
    private KryoDataOutput dataOut;
    private Compressor compressor;

    protected final IntIntPairWritable col = new IntIntPairWritable();

    protected BaseMatrixCache(
            final String blockName, final Configuration conf, final int numCols, final int numRows,
            final int sizePerValue) throws IOException {
        // TODO: calling GC manually may cause the performance degrade.
        MemoryUtils.callGCMultipleTimes();
        this.conf = conf;

        final long expectedSize = 12 * (long) numCols + (4 + sizePerValue) * (long) numRows;
        final long availableMemory = (long) ((
                MemoryUtils.availableMemoryBytes() - Constants.SizeOfMemoryBarrier) * 0.6);
        final String storedType;
        if (expectedSize < availableMemory) {
            storedType = "memory";
            memoryFrozen = false;

            colList = new IntArrayList(numCols);
            rowList = new IntArrayList(numRows);
            rowSizes = new IntArrayList(numCols);
            degrees = new IntArrayList(numCols);

            path = null;
            out = null;
            dataOut = null;
            compressor = null;
        } else {
            storedType = "disk";
            memoryFrozen = true;

            colList = null;
            rowList = null;
            rowSizes = null;
            degrees = null;

            path = IOUtils.getLocalTemporaryPath(blockName);

            final LocalFileSystem localFS = FileSystem.getLocal(conf);
            final CompressionCodec codec = IOUtils.getPreferredCompressionCodec(conf);
            final OutputStream rawStream = localFS.create(path, (short) 1);
            compressor = CodecPool.getCompressor(codec);
            out = new Output(codec.createOutputStream(rawStream, compressor));
            dataOut = new KryoDataOutput(out);
        }
        LOG.info("Matrix block {} is stored in {}. (expectedSize: {} MiB, availableMemory: {} MiB",
                 blockName, storedType, MemoryUtils.bytesToMebibytes(expectedSize),
                 MemoryUtils.bytesToMebibytes(availableMemory));
    }

    @Override
    public void put(final IntIntPairWritable col, final M rows) throws IOException {
        if (!memoryFrozen) {
            // store to memory
            putToMemory(col, rows);
        } else {
            // store to disk
            col.write(dataOut);
            rows.write(dataOut);
        }
    }

    protected abstract void putToMemory(final IntIntPairWritable col, final M rows);

    @Override
    public void freeze() {
        if (out != null) {
            out.close();
            CodecPool.returnCompressor(compressor);

            out = null;
            dataOut = null;
            compressor = null;
        }
    }
}
