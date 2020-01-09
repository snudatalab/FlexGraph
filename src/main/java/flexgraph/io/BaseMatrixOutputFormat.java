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

import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import flexgraph.Constants;
import flexgraph.utils.IOUtils;

import java.io.IOException;

/**
 * A base implementation of matrix output format in distributed file system.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public abstract class BaseMatrixOutputFormat<V> extends FileOutputFormat<IntWritable, V> {
    protected static class OutputConfig {
        public final Output[] sparseOutput;
        public final Output[] denseOutput;
        public final int denseThreshold;

        public OutputConfig(final Output[] sparseOutput, final Output[] denseOutput, final int denseThreshold) {
            this.sparseOutput = sparseOutput;
            this.denseOutput = denseOutput;
            this.denseThreshold = denseThreshold;
        }
    }

    protected OutputConfig getOutputConfig(final TaskAttemptContext context) throws IOException {
        final Configuration conf = context.getConfiguration();
        final int workerId = context.getTaskAttemptID().getTaskID().getId();
        final int parallelism = context.getNumReduceTasks();
        final int denseThreshold = conf.getInt(Constants.DenseThreshold, Constants.DefaultDenseThreshold);

        final Path rootPath = getOutputPath(context);
        final FileSystem fs = rootPath.getFileSystem(conf);
        final CompressionCodec codec = IOUtils.getPreferredCompressionCodec(context);
        final int bufSize = conf.getInt("io.file.buffer.size", 4096);

        final Output[] sparseOutput = new Output[parallelism];
        final Output[] denseOutput = new Output[parallelism];

        for (int i = 0; i < parallelism; ++i) {
            final int blockId = workerId + i * parallelism;
            final Path sparsePath = new Path(rootPath, String.format("sparse-%05d", blockId));
            final Path densePath = new Path(rootPath, String.format("dense-%05d", blockId));

            sparseOutput[i] = new Output(codec.createOutputStream(
                    fs.create(sparsePath, true, bufSize, (short) 1, fs.getDefaultBlockSize(sparsePath), context)));
            denseOutput[i] = new Output(codec.createOutputStream(
                    fs.create(densePath, true, bufSize, (short) 1, fs.getDefaultBlockSize(densePath), context)));
        }

        return new OutputConfig(sparseOutput, denseOutput, denseThreshold);
    }
}
