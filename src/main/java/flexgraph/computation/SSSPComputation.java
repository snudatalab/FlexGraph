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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import flexgraph.Constants;
import flexgraph.cache.*;
import flexgraph.coordination.CoordinationService;
import flexgraph.io.ComputationInputFormat;
import flexgraph.io.VectorOutputFormat;
import flexgraph.types.IntDoublePairArrayWritable;
import flexgraph.utils.IDUtils;

import java.io.IOException;

/**
 * Computes the distances of shortest paths to all vertices from a source vertex.
 *
 * Note that this implementation requires weighted graph as input graph.
 *
 * - Parameters
 *   - input: the path to input graph
 *   - output: the path to store shortest distance
 *   - parallelism: the number of workers
 *   - sourceVertex: the id of source vertex
 *   - maxIterations (optional): the number of maximum iterations
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class SSSPComputation extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SSSPComputation(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 4) return -1;

        final Path input = new Path(args[0]);
        final Path output = new Path(args[1]);
        final int parallelism = Integer.parseInt(args[2]);
        final int sourceVertex = Integer.parseInt(args[3]);
        final int maxIterations = getConf().getInt("maxIterations", -1);

        final Job job = Job.getInstance(getConf(), String.format("FlexGraph - SSSPComputation (%s)", input.toString()));
        job.setJarByClass(SSSPComputation.class);

        final Configuration conf = job.getConfiguration();
        conf.setInt(Constants.Parallelism, parallelism);
        conf.setInt(Constants.SSSPSourceVertex, sourceVertex);
        conf.setInt(Constants.MaxIterations, maxIterations);
        conf.setInt("mapreduce.task.timeout", 0);
        ComputationMapper.setEstimatedSize(job);

        // input
        FileInputFormat.setInputPaths(job, input);
        FileInputFormat.setInputDirRecursive(job, true);
        job.setInputFormatClass(ComputationInputFormat.class);
        ComputationMapper.setEstimatedSize(job);

        // mapper
        job.setMapperClass(SSSPMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // reducer
        job.setNumReduceTasks(0); // map-only job

        // output
        FileOutputFormat.setOutputPath(job, output);
        job.setOutputFormatClass(VectorOutputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static class SSSPMapper
            extends ComputationMapper<IntDoublePairArrayWritable, DoubleWritable, DoubleWritable, DoubleWritable> {
        private long sourceVertex;
        private final DoubleWritable infinite = new DoubleWritable(Double.MAX_VALUE / 2);

        @Override
        protected void setupComputation(final Context context, final CoordinationService coordService) {
            sourceVertex = context.getConfiguration().getLong(Constants.SSSPSourceVertex, -1L);
        }

        @Override
        protected VectorCache<DoubleWritable> createVectorCache(
                final int numBlocks, final int blockId, final long numVertices) {
            final DoubleVectorCache cache = new DoubleVectorCache(numBlocks, blockId, numVertices, infinite);
            for (int i = 0, size = cache.size(); i < size; ++i) {
                if (IDUtils.localToGlobal(numBlocks, blockId, i) == sourceVertex) {
                    cache.put(i, new DoubleWritable());
                } else {
                    cache.put(i, infinite);
                }
            }
            return cache;
        }

        @Override
        protected VectorCache<DoubleWritable> createIntermediateVectorCache(
                final int numBlocks, final long numVertices) {
            return new DoubleVectorCache(numBlocks, 0, numVertices, infinite);
        }

        @Override
        protected MatrixCache<IntDoublePairArrayWritable> createMatrixCache(
                final String blockName, final Configuration conf, final int numCols, final int numRows)
                throws IOException {
            return new WeightedMatrixCache(blockName, conf, numCols, numRows);
        }

        @Override
        protected Class<DoubleWritable> vectorValueClass() {
            return DoubleWritable.class;
        }

        @Override
        protected Class<DoubleWritable> intermediateVectorValueClass() {
            return DoubleWritable.class;
        }

        @Override
        protected Class<IntDoublePairArrayWritable> matrixValueClass() {
            return IntDoublePairArrayWritable.class;
        }

        @Override
        protected void combine2Op(
                final int matBlockId,
                final MatrixCache<IntDoublePairArrayWritable> matrix,
                final VectorCache<DoubleWritable> vecInput,
                final VectorCache<DoubleWritable> vecOutput) throws Exception {
            final DoubleWritable min = new DoubleWritable();
            final MatrixIterator<IntDoublePairArrayWritable> iter = matrix.iterator();
            while (iter.next()) {
                final int col = iter.currentCol();
                final double colDist = vecInput.get(col).get();
                if (colDist < infinite.get()) {
                    final IntDoublePairArrayWritable rows = iter.currentRows();
                    for (int i = 0, size = rows.size(); i < size; ++i) {
                        final int row = rows.getInt(i);
                        final double weight = rows.getDouble(i);
                        min.set(Math.min(vecOutput.get(row).get(), colDist + weight));
                        vecOutput.put(row, min);
                    }
                }
            }
            iter.close();
        }

        @Override
        protected void combineAllOp(
                final int matBlockId,
                final RecordReader<IntWritable, DoubleWritable> vecInput,
                final VectorCache<DoubleWritable> vecOutput)
                throws Exception {
            final DoubleWritable min = new DoubleWritable();
            while (vecInput.nextKeyValue()) {
                final int row = vecInput.getCurrentKey().get();
                final double dist = vecInput.getCurrentValue().get();
                min.set(Math.min(dist, vecOutput.get(row).get()));
                vecOutput.put(row, min);
            }
        }

        @Override
        protected void applyOp(
                final int vecBlockId,
                final VectorCache<DoubleWritable> vecResult,
                final VectorCache<DoubleWritable> vecIntermediate) throws Exception {
            final DoubleWritable value = new DoubleWritable();
            final VectorIterator<DoubleWritable> iter = vecResult.iterator(false, null);
            while (iter.next()) {
                final int id = iter.currentId();
                final double oldDist = iter.currentValue().get();
                final double newDist = vecIntermediate.get(id).get();

                if (oldDist > newDist) {
                    notifyChanged();
                    value.set(newDist);
                    vecResult.put(id, value);
                }
            }
        }
    }
}
