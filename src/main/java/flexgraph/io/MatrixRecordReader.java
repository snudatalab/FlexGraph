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
package flexgraph.io;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.KryoDataInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import flexgraph.types.IntIntPairWritable;
import flexgraph.utils.IOUtils;

import java.io.IOException;

/**
 * A record reader for pre-partitioned sub-matrices.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class MatrixRecordReader<V extends Writable> extends RecordReader<IntIntPairWritable, V> {
    private static final Logger LOG = LoggerFactory.getLogger(MatrixRecordReader.class);
    private Input in;
    private KryoDataInput dataIn;
    private Decompressor decompressor;

    private final IntIntPairWritable key = new IntIntPairWritable();
    private final V value;

    public MatrixRecordReader(final Class<V> valueClass, final Configuration conf) {
        this.value = ReflectionUtils.newInstance(valueClass, conf);
    }

    @Override
    public void initialize(final InputSplit split, final TaskAttemptContext context)
            throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();
        final Path path = ((FileSplit) split).getPath();
        final FileSystem fs = path.getFileSystem(conf);
        final CompressionCodec codec = IOUtils.getPreferredCompressionCodec(conf);
        // We manually retrieve decompressor from codec pool because Hadoop 2.7 has a memory leakage bug.
        decompressor = CodecPool.getDecompressor(codec);

        LOG.info("Initialize input stream: {}", path.toString());
        in = new Input(codec.createInputStream(fs.open(path), decompressor));
        dataIn = new KryoDataInput(in);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (in.eof()) return false;

        key.readFields(dataIn);
        value.readFields(dataIn);
        return true;
    }

    @Override
    public IntIntPairWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();

            // We manually return the decompressor
            CodecPool.returnDecompressor(decompressor);
            decompressor = null;
            dataIn = null;
            in = null;
        }
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0.99f;
    }
}
