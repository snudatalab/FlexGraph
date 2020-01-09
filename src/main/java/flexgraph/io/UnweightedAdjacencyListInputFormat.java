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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import flexgraph.Constants;
import flexgraph.types.LongArrayWritable;

import java.io.IOException;

/**
 * An input format for adjacency list formatted, unweighted graph.
 *
 * Each line in input graph should be formatted as follows:
 * `SRCID<TAB>DEGREE DST1 DST2 ... DSTN`
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class UnweightedAdjacencyListInputFormat extends FileInputFormat<LongWritable, LongArrayWritable> {
    @Override
    public RecordReader<LongWritable, LongArrayWritable> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();

        return new AdjacencyListRecordReader(
                conf.get(Constants.AdjacencyListVertexDelimiter, "\t"),
                conf.get(Constants.AdjacencyListEdgeDelimiter, " "));
    }

    public static class AdjacencyListRecordReader extends RecordReader<LongWritable, LongArrayWritable> {
        private final String vertexDelimiter;
        private final String edgeDelimiter;
        private final LineRecordReader reader = new LineRecordReader();

        private final LongWritable vertex = new LongWritable();
        private final LongArrayWritable edges = new LongArrayWritable();

        public AdjacencyListRecordReader(String vertexDelimiter, String edgeDelimiter) {
            this.vertexDelimiter = vertexDelimiter;
            this.edgeDelimiter = edgeDelimiter;
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            reader.initialize(split, context);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!reader.nextKeyValue()) return false;

            final Text line = reader.getCurrentValue();
            final String lineStr = line.toString();
            final int vertexLimit = line.find(vertexDelimiter);

            // vertex id
            vertex.set(Long.parseLong(lineStr.substring(0, vertexLimit)));

            // number of neighbors
            final int lengthLimit = line.find(edgeDelimiter, vertexLimit + 1);
            int length;
            if (lengthLimit == -1) length = 0;
            else length = Integer.parseInt(lineStr.substring(vertexLimit + 1, lengthLimit));

            switch (length) {
                case 0:
                    return nextKeyValue();
                case 1:
                    edges.set(Long.parseLong(lineStr.substring(lengthLimit + 1)));
                    break;
                default:
                    long[] values;
                    if (edges.getValues().length < length) {
                        values = new long[length];
                    } else {
                        values = edges.getValues();
                    }
                    
                    int begin = lengthLimit + 1, end = line.find(edgeDelimiter, begin);
                    for (int i = 0; end != -1; ++i) {
                        values[i] = Long.parseLong(lineStr.substring(begin, end));
                        begin = end + 1;
                        end = line.find(edgeDelimiter, begin);
                    }
                    values[length - 1] = Long.parseLong(lineStr.substring(begin));
                    edges.set(values, 0, length);
                    break;
            }

            return true;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return vertex;
        }

        @Override
        public LongArrayWritable getCurrentValue() throws IOException, InterruptedException {
            return edges;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return reader.getProgress();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }
}
