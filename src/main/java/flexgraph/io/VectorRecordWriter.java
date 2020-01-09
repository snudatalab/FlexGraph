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

import com.esotericsoftware.kryo.io.KryoDataOutput;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import flexgraph.utils.IOUtils;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A record writer for vector blocks.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class VectorRecordWriter<V extends Writable> extends RecordWriter<IntWritable, V> {
    private final Output out;
    private final KryoDataOutput dataOut;
    private final Compressor compressor;

    public VectorRecordWriter(final OutputStream out, final TaskAttemptContext context) throws IOException {
        final CompressionCodec codec = IOUtils.getPreferredCompressionCodec(context);
        this.compressor = CodecPool.getCompressor(codec);

        this.out = new Output(codec.createOutputStream(out, compressor));
        this.dataOut = new KryoDataOutput(this.out);
    }

    @Override
    public void write(final IntWritable key, final V value) throws IOException, InterruptedException {
        key.write(dataOut);
        value.write(dataOut);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        out.close();
        CodecPool.returnCompressor(compressor);
    }
}
