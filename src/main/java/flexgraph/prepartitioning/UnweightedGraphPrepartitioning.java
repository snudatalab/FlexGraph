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
import flexgraph.io.UnweightedAdjacencyListInputFormat;
import flexgraph.io.UnweightedEdgeListInputFormat;
import flexgraph.io.UnweightedMatrixOutputFormat;
import flexgraph.types.ColumnEntry;
import flexgraph.types.IntArrayWritable;
import flexgraph.types.IntIntPairWritable;
import flexgraph.types.LongArrayWritable;
import flexgraph.utils.IDUtils;

import java.io.IOException;

/**
 * Performs pre-partitioning step given a unweighted graph.
 *
 * - Parameters
 *   - input: the path to input graph
 *   - output: the path to store pre-partitioned matrices
 *   - undirected/transposed: the input graph is undirected graph or transposed graph
 *   - inputformat: edgelist / adjlist
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class UnweightedGraphPrepartitioning extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        final int exitCode = ToolRunner.run(new UnweightedGraphPrepartitioning(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 4) return -1;

        final Path input = new Path(args[0]);
        final Path output = new Path(args[1]);
        final boolean undirected = args[2].equalsIgnoreCase("undirected");
        final boolean transposed = args[2].equalsIgnoreCase("transposed");
        final String inputFormat = args[3].toLowerCase();

        final Job job = Job.getInstance(
                getConf(), String.format("FlexGraph - UnweightedGraphPrepartitioning (%s)", input.toString()));
        job.setJarByClass(UnweightedGraphPrepartitioning.class);

        // input
        FileInputFormat.setInputPaths(job, input);
        if (inputFormat.equals("edgelist")) job.setInputFormatClass(UnweightedEdgeListInputFormat.class);
        else job.setInputFormatClass(UnweightedAdjacencyListInputFormat.class);

        // output
        FileOutputFormat.setOutputPath(job, output);
        job.setOutputFormatClass(UnweightedMatrixOutputFormat.class);

        // mapper
        if (undirected) job.setMapperClass(UndirectedMapper.class);
        else if (transposed) job.setMapperClass(TransposedMapper.class);
        else job.setMapperClass(DirectedMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongArrayWritable.class);

        // partitioner
        job.setPartitionerClass(PrepartitioningPartitioner.class);

        // combiner
        job.setCombinerClass(PrepartitioningCombiner.class);

        // reducer
        job.setReducerClass(PrepartitioningReducer.class);
        job.setOutputKeyClass(IntIntPairWritable.class);
        job.setOutputValueClass(IntArrayWritable.class);

        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static class DirectedMapper
            extends Mapper<LongWritable, LongArrayWritable, LongWritable, LongArrayWritable> {
        @Override
        protected void map(LongWritable key, LongArrayWritable value, Context context)
                throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class TransposedMapper
            extends Mapper<LongWritable, LongArrayWritable, LongWritable, LongArrayWritable> {
        private final LongWritable vertex = new LongWritable();
        private final LongArrayWritable bundle = new LongArrayWritable(new long[1]);

        @Override
        protected void map(final LongWritable key, final LongArrayWritable value, final Context context)
                throws IOException, InterruptedException {
            bundle.getValues()[0] = key.get();
            for (int i = 0, size = value.size(); i < size; ++i) {
                vertex.set(value.get(i));
                context.write(vertex, bundle);
            }
        }
    }

    public static class UndirectedMapper
            extends Mapper<LongWritable, LongArrayWritable, LongWritable, LongArrayWritable> {
        private final LongWritable vertex = new LongWritable();
        private final LongArrayWritable bundle = new LongArrayWritable(new long[1]);

        @Override
        protected void map(LongWritable key, LongArrayWritable value, Context context)
                throws IOException, InterruptedException {
            // directed
            context.write(key, value);

            // reverse directed
            bundle.getValues()[0] = key.get();
            for (int i = 0, size = value.size(); i < size; ++i) {
                vertex.set(value.get(i));
                context.write(vertex, bundle);
            }
        }
    }

    public static class PrepartitioningCombiner
            extends Reducer<LongWritable, LongArrayWritable, LongWritable, LongArrayWritable> {
        private final LongArrayWritable bundle = new LongArrayWritable();
        private final LongArrayList edges = new LongArrayList(Constants.InitArrayWritableSize);

        @Override
        protected void reduce(LongWritable key, Iterable<LongArrayWritable> values, Context context)
                throws IOException, InterruptedException {
            edges.clear();

            for (LongArrayWritable bundle : values) {
                edges.addElements(edges.size(), bundle.getValues(), bundle.begin(), bundle.size());
            }

            bundle.set(edges);
            context.write(key, bundle);
        }
    }

    public static class PrepartitioningReducer extends GraphPrepartitioningReducer<LongArrayWritable, ColumnEntry> {
        private final IntWritable blockId = new IntWritable();
        private final ColumnEntry columnEntry = new ColumnEntry();
        private IntArrayList[] blocks;
        private int denseThreshold;
        private final IntWritable localId = new IntWritable();

        @Override
        protected void setup(final Context context) throws IOException, InterruptedException {
            super.setup(context);

            // initialize block information
            blocks = new IntArrayList[numBlocks];
            for (int i = 0; i < numBlocks; ++i) {
                blocks[i] = new IntArrayList();
            }

            // retrieve dense threshold
            denseThreshold = context.getConfiguration().getInt(
                    Constants.DenseThreshold, Constants.DefaultDenseThreshold);
        }

        @Override
        protected void reduce(
                final LongWritable vertex, final Iterable<LongArrayWritable> values, final Context context)
                throws IOException, InterruptedException {
            for (int i = 0; i < numBlocks; ++i) {
                blocks[i].clear();
            }

            // collect all entries
            maxVertex = Math.max(maxVertex, vertex.get());
            for (LongArrayWritable bundle : values) {
                for (int i = 0, size = bundle.size(); i < size; ++i) {
                    final long dst = bundle.get(i);
                    maxVertex = Math.max(maxVertex, dst);
                    final int blockId = (int) (dst % numBlocks);
                    blocks[blockId].add(IDUtils.globalToLocal(numBlocks, dst));
                }
            }

            // count number of non-zeros
            int numEntries = 0;
            for (int i = 0; i < numBlocks; ++i) {
                numEntries += blocks[i].size();
            }
            columnEntry.numEntries = numEntries;

            // output the block entries
            columnEntry.col = IDUtils.globalToLocal(numBlocks, vertex.get());
            for (int i = 0; i < numBlocks; ++i) {
                if (blocks[i].size() == 0) continue;
                blockId.set(i);
                columnEntry.rows = blocks[i];
                context.write(blockId, columnEntry);
            }

            if (denseThreshold >= 0 && columnEntry.numEntries >= denseThreshold) {
                localId.set(IDUtils.globalToLocal(numBlocks, vertex.get()));
                dvWriter.write(localId, NullWritable.get());
                for (int i = 0; i < numBlocks; ++i) {
                    numDenseRows[i] += blocks[i].size();
                    if (blocks[i].size() > 0) {
                        ++numDenseCols[i];
                    }
                }
            } else {
                for (int i = 0; i < numBlocks; ++i) {
                    numSparseRows[i] += blocks[i].size();
                    if (blocks[i].size() > 0) {
                        ++numSparseCols[i];
                    }
                }
            }
        }
    }
}
