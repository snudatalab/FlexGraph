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
 * An input format for edge list formatted, weighted graph in distributed system.
 *
 * Each line in the input graph should be formatted as follows:
 * `SRCID<TAB>DSTID<TAB>WEIGHT`
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class WeightedEdgeListInputFormat extends FileInputFormat<LongWritable, LongDoublePairArrayWritable> {
    @Override
    public RecordReader<LongWritable, LongDoublePairArrayWritable> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        final String delimiter = context.getConfiguration().get(Constants.EdgeListDelimiter, "\t");
        return new EdgeRecordReader(delimiter);
    }

    public static class EdgeRecordReader extends RecordReader<LongWritable, LongDoublePairArrayWritable> {
        private final LongWritable vertex = new LongWritable();
        private final LongDoublePairArrayWritable edges = new LongDoublePairArrayWritable(new long[1], new double[1]);
        private final LineRecordReader reader = new LineRecordReader();
        private final String fieldDelimiter;

        public EdgeRecordReader(String fieldDelimiter) {
            this.fieldDelimiter = fieldDelimiter;
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
            final int split1 = line.find(fieldDelimiter);
            final int split2 = line.find(fieldDelimiter, split1 + 1);

            vertex.set(Long.parseLong(lineStr.substring(0, split1)));
            edges.getLongs()[0] = Long.parseLong(lineStr.substring(split1 + 1, split2));
            edges.getDoubles()[0] = Double.parseDouble(lineStr.substring(split2 + 1));

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
