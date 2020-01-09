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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import flexgraph.utils.IOUtils;

import java.io.IOException;
import java.io.InputStream;

/**
 * A record reader for vector blocks.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class VectorRecordReader<V extends Writable> extends RecordReader<IntWritable, V> {
    private final Input in;
    private final KryoDataInput dataIn;
    private final Decompressor decompressor;

    private final IntWritable key = new IntWritable();
    private final V value;

    public VectorRecordReader(
            final InputStream in, final TaskAttemptContext context, final Class<V> valueClass) throws IOException {
        final CompressionCodec codec = IOUtils.getPreferredCompressionCodec(context);
        this.decompressor = CodecPool.getDecompressor(codec);

        this.in = new Input(codec.createInputStream(in, decompressor));
        this.dataIn = new KryoDataInput(this.in);
        this.value = ReflectionUtils.newInstance(valueClass, context.getConfiguration());
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (in.eof()) return false;

        key.readFields(dataIn);
        value.readFields(dataIn);
        return true;
    }

    @Override
    public IntWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0.99f;
    }

    @Override
    public void close() throws IOException {
        in.close();
        CodecPool.returnDecompressor(decompressor);
    }
}
