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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import flexgraph.Constants;
import flexgraph.cache.*;
import flexgraph.io.ComputationInputFormat;
import flexgraph.io.VectorOutputFormat;
import flexgraph.types.IntArrayWritable;
import flexgraph.utils.IDUtils;

import java.io.IOException;

/**
 * Connected Components Computation on GIM-V.
 *
 * This implementation is based on CC computation in PEGASUS.
 * For more detailed description of CC on GIM-V, see ICDM 2009 paper from Kang et. al.
 *
 * Parameters
 * - input: the path to pre-partitioned matrices
 * - output: the path to connected components results
 * - maxIterations (optional): the number of maximum itertions
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class CCComputation extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CCComputation(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 4) return -1;

        final Path input = new Path(args[0]);
        final Path output = new Path(args[1]);
        final int parallelism = Integer.parseInt(args[2]);
        final int maxIterations = getConf().getInt("maxIterations", -1);

        final Job job = Job.getInstance(getConf(), String.format("FlexGraph - CCComputation (%s)", input.toString()));
        job.setJarByClass(CCComputation.class);

        final Configuration conf = job.getConfiguration();
        conf.setInt(Constants.Parallelism, parallelism);
        conf.setInt(Constants.MaxIterations, maxIterations);
        conf.setInt("mapreduce.task.timeout", 0);
        ComputationMapper.setEstimatedSize(job);

        // input
        FileInputFormat.setInputPaths(job, input);
        FileInputFormat.setInputDirRecursive(job, true);
        job.setInputFormatClass(ComputationInputFormat.class);
        ComputationMapper.setEstimatedSize(job);

        // mapper
        job.setMapperClass(CCMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        // reducer
        job.setNumReduceTasks(0); // map-only job

        // output
        FileOutputFormat.setOutputPath(job, output);
        job.setOutputFormatClass(VectorOutputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static class CCMapper
            extends ComputationMapper<IntArrayWritable, LongWritable, LongWritable, LongWritable> {
        @Override
        protected VectorCache<LongWritable> createVectorCache(
                final int numBlocks, final int blockId, final long numVertices) {
            final LongVectorCache cache = new LongVectorCache(numBlocks, blockId, numVertices, new LongWritable(0));
            final LongWritable value = new LongWritable();
            for (int i = 0, size = cache.size(); i < size; ++i) {
                value.set(IDUtils.localToGlobal(numBlocks, blockId, i));
                cache.put(i, value);
            }
            return cache;
        }

        @Override
        protected VectorCache<LongWritable> createIntermediateVectorCache(
                final int numBlocks, final long numVertices) {
            return new LongVectorCache(numBlocks, 0, numVertices, new LongWritable(Long.MAX_VALUE / 2));
        }

        @Override
        protected MatrixCache<IntArrayWritable> createMatrixCache(
                final String blockName, final Configuration conf, final int numCols, final int numRows)
                throws IOException {
            return new UnweightedMatrixCache(blockName, conf, numCols, numRows);
        }

        @Override
        protected Class<LongWritable> vectorValueClass() {
            return LongWritable.class;
        }

        @Override
        protected Class<LongWritable> intermediateVectorValueClass() {
            return LongWritable.class;
        }

        @Override
        protected Class<IntArrayWritable> matrixValueClass() {
            return IntArrayWritable.class;
        }

        @Override
        protected void combine2Op(
                final int matBlockId,
                final MatrixCache<IntArrayWritable> matrix,
                final VectorCache<LongWritable> vecInput,
                final VectorCache<LongWritable> vecOutput) throws Exception {
            final LongWritable min = new LongWritable();
            final MatrixIterator<IntArrayWritable> iter = matrix.iterator();
            while (iter.next()) {
                final int col = iter.currentCol();
                final long cc = vecInput.get(col).get();
                final IntArrayWritable rows = iter.currentRows();
                for (int i = 0, size = rows.size(); i < size; ++i) {
                    final int row = rows.get(i);
                    final long rowCC = vecOutput.get(row).get();
                    if (rowCC > cc) {
                        min.set(cc);
                        vecOutput.put(row, min);
                    }
                }
            }
            iter.close();
        }

        @Override
        protected void combineAllOp(
                final int matBlockId,
                final RecordReader<IntWritable, LongWritable> vecInput,
                final VectorCache<LongWritable> vecOutput)
                throws Exception {
            final LongWritable min = new LongWritable();
            while (vecInput.nextKeyValue()) {
                final int row = vecInput.getCurrentKey().get();
                final long cc = vecInput.getCurrentValue().get();
                min.set(Math.min(cc, vecOutput.get(row).get()));
                vecOutput.put(row, min);
            }
        }

        @Override
        protected void applyOp(
                final int vecBlockId,
                final VectorCache<LongWritable> vecResult,
                final VectorCache<LongWritable> vecIntermediate) throws Exception {
            final LongWritable value = new LongWritable();
            final VectorIterator<LongWritable> iter = vecResult.iterator(false, null);
            while (iter.next()) {
                final int id = iter.currentId();
                final long oldCC = iter.currentValue().get();
                final long newCC = vecIntermediate.get(id).get();

                if (oldCC > newCC) {
                    notifyChanged();
                    value.set(newCC);
                    vecResult.put(id, value);
                }
            }
        }
    }
}
