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
package flexgraph.prepartitioning;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import flexgraph.Constants;
import flexgraph.io.WeightedEdgeListInputFormat;
import flexgraph.io.WeightedMatrixOutputFormat;
import flexgraph.types.LongDoublePairArrayWritable;
import flexgraph.types.LongIntPairWritable;
import flexgraph.types.WeightedColumnEntry;
import flexgraph.utils.IDUtils;

import java.io.IOException;

/**
 * Performs pre-partitioning step on weighted graph.
 * - Parameters
 *   - input: the path to input graph
 *   - output: the path to store pre-partitioned matrices
 *   - undirected: whether the input graph is undirected graph or not 
 *   - inputformat: edgelist / adjlist
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class WeightedGraphPrepartitioning extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        final int exitCode = ToolRunner.run(new WeightedGraphPrepartitioning(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 4) return -1;

        final Path input = new Path(args[0]);
        final Path output = new Path(args[1]);
        final boolean undirected = args[2].toLowerCase().equals("undirected");
        final String inputFormat = args[3].toLowerCase();

        final Job job = Job.getInstance(
                getConf(), String.format("FlexGraph - WeightedGraphPrepartitioning (%s)", input.toString()));
        job.setJarByClass(WeightedGraphPrepartitioning.class);

        // input
        FileInputFormat.setInputPaths(job, input);
        if (inputFormat.equals("edgelist")) job.setInputFormatClass(WeightedEdgeListInputFormat.class);
        // else job.setInputFormatClass(UnweightedAdjacencyListInputFormat.class);

        // output
        FileOutputFormat.setOutputPath(job, output);
        job.setOutputFormatClass(WeightedMatrixOutputFormat.class);

        // mapper
        if (undirected) job.setMapperClass(UndirectedMapper.class);
        else job.setMapperClass(DirectedMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongDoublePairArrayWritable.class);

        // partitioner
        job.setPartitionerClass(PrepartitioningPartitioner.class);

        // combiner
        job.setCombinerClass(PrepartitioningCombiner.class);

        // reducer
        job.setReducerClass(PrepartitioningReducer.class);
        job.setOutputKeyClass(LongIntPairWritable.class);
        job.setOutputValueClass(LongDoublePairArrayWritable.class);

        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static class DirectedMapper
            extends Mapper<LongWritable, LongDoublePairArrayWritable, LongWritable, LongDoublePairArrayWritable> {
        @Override
        protected void map(LongWritable key, LongDoublePairArrayWritable value, Context context)
                throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class UndirectedMapper
            extends Mapper<LongWritable, LongDoublePairArrayWritable, LongWritable, LongDoublePairArrayWritable> {
        private final LongWritable vertex = new LongWritable();
        private final LongDoublePairArrayWritable bundle = new LongDoublePairArrayWritable(new long[1], new double[1]);

        @Override
        protected void map(LongWritable key, LongDoublePairArrayWritable value, Context context)
                throws IOException, InterruptedException {
            // directed
            context.write(key, value);

            // reverse directed
            bundle.getLongs()[0] = key.get();
            for (int i = 0, size = value.size(); i < size; ++i) {
                vertex.set(value.getLong(i));
                bundle.getDoubles()[0] = value.getDouble(0);
                context.write(vertex, bundle);
            }
        }
    }

    public static class PrepartitioningCombiner
            extends Reducer<LongWritable, LongDoublePairArrayWritable, LongWritable, LongDoublePairArrayWritable> {
        private final LongDoublePairArrayWritable bundle = new LongDoublePairArrayWritable();
        private final LongArrayList edges = new LongArrayList(Constants.InitArrayWritableSize);
        private final DoubleArrayList weights = new DoubleArrayList(Constants.InitArrayWritableSize);

        @Override
        protected void reduce(LongWritable key, Iterable<LongDoublePairArrayWritable> values, Context context)
                throws IOException, InterruptedException {
            edges.clear();
            weights.clear();

            for (LongDoublePairArrayWritable bundle : values) {
                edges.addElements(edges.size(), bundle.getLongs(), bundle.begin(), bundle.end());
                weights.addElements(weights.size(), bundle.getDoubles(), bundle.begin(), bundle.end());
            }

            bundle.set(edges, weights);
            context.write(key, bundle);
        }
    }

    public static class PrepartitioningReducer
            extends GraphPrepartitioningReducer<LongDoublePairArrayWritable, WeightedColumnEntry> {
        private final IntWritable blockId = new IntWritable();
        private final WeightedColumnEntry columnEntry = new WeightedColumnEntry();
        private IntArrayList[] intBlocks;
        private DoubleArrayList[] doubleBlocks;
        private int denseThreshold;
        private final IntWritable localId = new IntWritable();

        @Override
        protected void setup(final Context context) throws IOException, InterruptedException {
            super.setup(context);

            intBlocks = new IntArrayList[numBlocks];
            doubleBlocks = new DoubleArrayList[numBlocks];

            for (int i = 0; i < numBlocks; ++i) {
                intBlocks[i] = new IntArrayList();
                doubleBlocks[i] = new DoubleArrayList();
            }

            denseThreshold = context.getConfiguration().getInt(
                    Constants.DenseThreshold, Constants.DefaultDenseThreshold);
        }

        @Override
        protected void reduce(
                final LongWritable vertex, final Iterable<LongDoublePairArrayWritable> values, final Context context)
                throws IOException, InterruptedException {
            for (int i = 0; i < numBlocks; ++i) {
                intBlocks[i].clear();
                doubleBlocks[i].clear();
            }

            // collect all entries
            maxVertex = Math.max(maxVertex, vertex.get());
            for (final LongDoublePairArrayWritable bundle : values) {
                for (int i = 0, size = bundle.size(); i < size; ++i) {
                    final long dst = bundle.getLong(i);
                    final double weight = bundle.getDouble(i);
                    final int blockId = (int) (dst % numBlocks);
                    intBlocks[blockId].add(IDUtils.globalToLocal(numBlocks, dst));
                    doubleBlocks[blockId].add(weight);
                    maxVertex = Math.max(maxVertex, dst);
                }
            }

            // count number of non-zeros
            int numEntries = 0;
            for (int i = 0; i < numBlocks; ++i) {
                numEntries += intBlocks[i].size();
            }
            columnEntry.numEntries = numEntries;

            // output the block entries
            columnEntry.col = IDUtils.globalToLocal(numBlocks, vertex.get());
            for (int i = 0; i < numBlocks; ++i) {
                if (intBlocks[i].size() == 0) continue;
                blockId.set(i);
                columnEntry.rows = intBlocks[i];
                columnEntry.weights = doubleBlocks[i];
                context.write(blockId, columnEntry);
            }

            if (denseThreshold >= 0 && columnEntry.numEntries >= denseThreshold) {
                localId.set(IDUtils.globalToLocal(numBlocks, vertex.get()));
                dvWriter.write(localId, NullWritable.get());
                for (int i = 0; i < numBlocks; ++i) {
                    numDenseRows[i] += intBlocks[i].size();
                    if (intBlocks[i].size() > 0) {
                        ++numDenseCols[i];
                    }
                }
            } else {
                for (int i = 0; i < numBlocks; ++i) {
                    numSparseRows[i] += intBlocks[i].size();
                    if (intBlocks[i].size() > 0) {
                        ++numSparseCols[i];
                    }
                }
            }
        }
    }
}
