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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import flexgraph.types.WeightedColumnEntry;

import java.io.IOException;

/**
 * A matrix output format implementation for double-valued, pre-partitioned sub-matrices.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class WeightedMatrixOutputFormat extends BaseMatrixOutputFormat<WeightedColumnEntry> {
    @Override
    public RecordWriter<IntWritable, WeightedColumnEntry> getRecordWriter(final TaskAttemptContext context)
            throws IOException, InterruptedException {
        final OutputConfig config = getOutputConfig(context);
        return new MultiBlockWriter(
                config.sparseOutput, config.denseOutput, config.denseThreshold, config.denseOutput.length);
    }

    public static class MultiBlockWriter extends RecordWriter<IntWritable, WeightedColumnEntry> {
        private final int denseThreshold;
        private final int numBlocks;
        private final Output[] sparseOutput;
        private final Output[] denseOutput;

        MultiBlockWriter(
                final Output[] sparseOutput,
                final Output[] denseOutput,
                final int denseThreshold,
                final int numBlocks) {
            this.sparseOutput = sparseOutput;
            this.denseOutput = denseOutput;
            this.denseThreshold = denseThreshold;
            this.numBlocks = numBlocks;
        }

        @Override
        public void write(final IntWritable blockId, final WeightedColumnEntry colEntry)
                throws IOException, InterruptedException {
            final int subBlockId = blockId.get();
            final int degree = colEntry.numEntries;
            Output out;
            if (denseThreshold < 0 || degree < denseThreshold) {
                out = sparseOutput[subBlockId];
            } else {
                out = denseOutput[subBlockId];
            }

            // write column
            out.writeInt(colEntry.col);
            out.writeInt(degree);

            // write rows
            final int size = colEntry.rows.size();
            if (size == 1) {
                out.writeInt(colEntry.rows.getInt(0));
                out.writeDouble(colEntry.weights.getDouble(0));
            } else {
                out.writeInt(colEntry.rows.getInt(0) | Integer.MIN_VALUE);
                out.writeDouble(colEntry.weights.getDouble(0));
                out.writeInt(size - 1);
                for (int i = 1; i < size; ++i) {
                    out.writeInt(colEntry.rows.getInt(i));
                    out.writeDouble(colEntry.weights.getDouble(i));
                }
            }
        }

        @Override
        public void close(final TaskAttemptContext context) throws IOException, InterruptedException {
            for (int i = 0; i < numBlocks; ++i) {
                sparseOutput[i].close();
                denseOutput[i].close();
            }
        }
    }
}
