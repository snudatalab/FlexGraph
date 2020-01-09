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
import flexgraph.types.LongDoublePairArrayWritable;

import java.io.IOException;

/**
 * An input format for adjacency list formatted, weighted graph in distributed file system.
 *
 * Each line in the input graph should be formatted as follows:
 * `SRCID<TAB>DEGREE DST1 WEIGHT1 DST2 WEIGHT2 ... DSTN WEIGHTN`
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class WeightedAdjacencyListInputFormat extends FileInputFormat<LongWritable, LongDoublePairArrayWritable> {
    @Override
    public RecordReader<LongWritable, LongDoublePairArrayWritable> createRecordReader(
            final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();

        return new AdjacencyListRecordReader(
                conf.get(Constants.AdjacencyListVertexDelimiter, "\t"),
                conf.get(Constants.AdjacencyListEdgeDelimiter, " "));
    }

    public static class AdjacencyListRecordReader extends RecordReader<LongWritable, LongDoublePairArrayWritable> {
        private final String vertexDelimiter;
        private final String edgeDelimiter;
        private final LineRecordReader reader = new LineRecordReader();

        private final LongWritable vertex = new LongWritable();
        private final LongDoublePairArrayWritable edges = new LongDoublePairArrayWritable();

        public AdjacencyListRecordReader(String vertexDelimiter, String edgeDelimiter) {
            this.vertexDelimiter = vertexDelimiter;
            this.edgeDelimiter = edgeDelimiter;
        }

        @Override
        public void initialize(final InputSplit split, final TaskAttemptContext context)
                throws IOException, InterruptedException {
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
                    final int dstLimit = line.find(edgeDelimiter, lengthLimit + 1);
                    final long dst = Long.parseLong(lineStr.substring(lengthLimit + 1, dstLimit));
                    final double weight = Double.parseDouble(lineStr.substring(dstLimit + 1));
                    edges.set(dst, weight);
                    return true;
                default:
                    long[] dsts;
                    double[] weights;
                    if (edges.getLongs().length < length) {
                        dsts = new long[length];
                        weights = new double[length];
                    } else {
                        dsts = edges.getLongs();
                        weights = edges.getDoubles();
                    }

                    int begin = lengthLimit + 1;
                    int mid = line.find(edgeDelimiter, begin);
                    int end = line.find(edgeDelimiter, mid + 1);
                    int last = length - 1;
                    for (int i = 0; i < last; ++i) {
                        dsts[i] = Long.valueOf(lineStr.substring(begin, mid));
                        weights[i] = Double.valueOf(lineStr.substring(mid + 1, end));
                        begin = end + 1;
                        mid = line.find(edgeDelimiter, begin);
                        end = line.find(edgeDelimiter, mid + 1);
                    }
                    dsts[last] = Long.valueOf(lineStr.substring(begin, mid));
                    weights[last] = Double.valueOf(lineStr.substring(mid + 1));
                    edges.set(dsts, weights, 0, length);
                    break;
            }

            return true;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return vertex;
        }

        @Override
        public LongDoublePairArrayWritable getCurrentValue() throws IOException, InterruptedException {
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
