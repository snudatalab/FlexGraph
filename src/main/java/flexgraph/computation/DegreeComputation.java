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
package flexgraph.computation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import flexgraph.io.UnweightedAdjacencyListInputFormat;
import flexgraph.io.UnweightedEdgeListInputFormat;
import flexgraph.io.WeightedEdgeListInputFormat;
import flexgraph.types.LongArrayWritable;
import flexgraph.types.LongDoublePairArrayWritable;

import java.io.IOException;

/**
 * Computes a degree distribution to determine threshold.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class DegreeComputation extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new DegreeComputation(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 6) return -1;

        final Path input = new Path(args[0]);
        final Path output = new Path(args[1]);
        final boolean unweighted = "unweighted".equals(args[2]);
        final boolean edgelist = "edgelist".equals(args[3]);
        final String direction = args[4];
        final int parallelism = Integer.parseInt(args[5]);

        final Job job = Job.getInstance(getConf(), String.format("FlexGraph - DegreeComputation (%s)", input.toString()));
        job.setJarByClass(DegreeComputation.class);

        final Configuration conf = job.getConfiguration();
        conf.setInt("mapreduce.task.timeout", 0);

        // input
        FileInputFormat.setInputPaths(job, input);
        if (unweighted) {
            if (edgelist) job.setInputFormatClass(UnweightedEdgeListInputFormat.class);
            else job.setInputFormatClass(UnweightedAdjacencyListInputFormat.class);
        } else {
            if (edgelist) job.setInputFormatClass(WeightedEdgeListInputFormat.class);
            // TODO: Add an input format class for weighted and adjacency list-formatted graphs
        }

        // output
        FileOutputFormat.setOutputPath(job, output);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // mapper
        if (unweighted) {
            if ("in".equals(direction)) job.setMapperClass(UnweightedInDegreeMapper.class);
            else if ("out".equals(direction)) job.setMapperClass(UnweightedOutDegreeMapper.class);
            else job.setMapperClass(UnweightedDegreeMapper.class);
        } else {
            if ("in".equals(direction)) job.setMapperClass(WeightedInDegreeMapper.class);
            else if ("out".equals(direction)) job.setMapperClass(WeightedOutDegreeMapper.class);
            else job.setMapperClass(WeightedDegreeMapper.class);
        }
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        // combiner
        job.setCombinerClass(DegreeReducer.class);

        // reducer
        job.setReducerClass(DegreeReducer.class);
        job.setNumReduceTasks(parallelism);

        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static class UnweightedOutDegreeMapper
            extends Mapper<LongWritable, LongArrayWritable, LongWritable, IntWritable> {
        private final IntWritable degree = new IntWritable();

        @Override
        protected void map(final LongWritable key, final LongArrayWritable value, final Context context)
                throws IOException, InterruptedException {
            degree.set(value.size());
            context.write(key, degree);
        }
    }

    public static class UnweightedInDegreeMapper
            extends Mapper<LongWritable, LongArrayWritable, LongWritable, IntWritable> {
        private final LongWritable vertex = new LongWritable();
        private final IntWritable degree = new IntWritable(1);

        @Override
        protected void map(final LongWritable key, final LongArrayWritable value, final Context context)
                throws IOException, InterruptedException {
            for (int i = 0, size = value.size(); i < size; ++i) {
                vertex.set(value.get(i));
                context.write(vertex, degree);
            }
        }
    }

    public static class UnweightedDegreeMapper
            extends Mapper<LongWritable, LongArrayWritable, LongWritable, IntWritable> {
        private final LongWritable vertex = new LongWritable();
        private final IntWritable degree = new IntWritable(1);

        @Override
        protected void map(final LongWritable key, final LongArrayWritable value, final Context context)
                throws IOException, InterruptedException {
            // in-degree
            for (int i = 0, size = value.size(); i < size; ++i) {
                vertex.set(value.get(i));
                context.write(vertex, degree);
            }

            // out-degree
            degree.set(value.size());
            context.write(key, degree);
        }
    }

    public static class WeightedOutDegreeMapper
            extends Mapper<LongWritable, LongDoublePairArrayWritable, LongWritable, IntWritable> {
        private final IntWritable degree = new IntWritable();

        @Override
        protected void map(final LongWritable key, final LongDoublePairArrayWritable value, final Context context)
                throws IOException, InterruptedException {
            degree.set(value.size());
            context.write(key, degree);
        }
    }

    public static class WeightedInDegreeMapper
            extends Mapper<LongWritable, LongDoublePairArrayWritable, LongWritable, IntWritable> {
        private final LongWritable vertex = new LongWritable();
        private final IntWritable degree = new IntWritable(1);

        @Override
        protected void map(final LongWritable key, final LongDoublePairArrayWritable value, final Context context)
                throws IOException, InterruptedException {
            for (int i = 0, size = value.size(); i < size; ++i) {
                vertex.set(value.getLong(i));
                context.write(vertex, degree);
            }
        }
    }

    public static class WeightedDegreeMapper
            extends Mapper<LongWritable, LongDoublePairArrayWritable, LongWritable, IntWritable> {
        private final LongWritable vertex = new LongWritable();
        private final IntWritable degree = new IntWritable(1);

        @Override
        protected void map(final LongWritable key, final LongDoublePairArrayWritable value, final Context context)
                throws IOException, InterruptedException {
            // in-degree
            for (int i = 0, size = value.size(); i < size; ++i) {
                vertex.set(value.getLong(i));
                context.write(vertex, degree);
            }

            // out-degree
            degree.set(value.size());
            context.write(key, degree);
        }
    }

    public static class DegreeReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
        private final IntWritable degree = new IntWritable();

        @Override
        protected void reduce(final LongWritable key, final Iterable<IntWritable> values, final Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            degree.set(sum);

            context.write(key, degree);
        }
    }
}
