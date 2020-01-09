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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import flexgraph.Constants;
import flexgraph.cache.*;
import flexgraph.coordination.Barrier;
import flexgraph.coordination.CoordinationService;
import flexgraph.coordination.Counter;
import flexgraph.io.ComputationInputFormat;
import flexgraph.io.VectorOutputFormat;
import flexgraph.types.IntArrayWritable;
import flexgraph.utils.IDUtils;

import java.io.IOException;

/**
 * Computes PageRank scores for all vertices in input graph.
 *
 * - Parameters
 *   - input: the path to input grpah
 *   - output: the path to store PageRank scores
 *   - parallelism: the number of workers
 *   - maxIterations (optional): the number of maximum iterations
 *   - personalized (optional): whether the algorithm computes Personalized PageRank (PPR) scores or not
 *   - sourceVertex (optional): the source vertex id in PPR
 *   - dampingFactor (optional): the damping factor in PageRank
 *   - threshold (optional): the converge threshold
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class PageRankComputation extends Configured implements Tool {
    public static void main(final String[] args) throws Exception {
        int exitCode = ToolRunner.run(new PageRankComputation(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(final String[] args) throws Exception {
        if (args.length < 3) return -1;

        final Path input = new Path(args[0]);
        final Path output = new Path(args[1]);
        final int parallelism = Integer.valueOf(args[2]);
        final int maxIterations = getConf().getInt("maxIterations", -1);
        final boolean personalized = getConf().getBoolean("personalized", false);
        final long sourceVertex = getConf().getLong("sourceVertex", -1L);
        final double dampingFactor = getConf().getDouble("dampingFactor", Constants.DefaultPageRankDampingFactor);
        final double threshold = getConf().getDouble("threshold", Constants.DefaultPageRankThreshold);

        final Job job = Job.getInstance(getConf(), String.format("FlexGraph - PageRankComputation (%s)", input.toString()));
        job.setJarByClass(PageRankComputation.class);

        final Configuration conf = job.getConfiguration();
        conf.setInt(Constants.MaxIterations, maxIterations);
        conf.setInt(Constants.Parallelism, parallelism);
        conf.setBoolean(Constants.PersonalizedPageRank, personalized);
        conf.setLong(Constants.PageRankSourceVertex, sourceVertex);
        conf.setDouble(Constants.PageRankDampingFactor, dampingFactor);
        conf.setDouble(Constants.PageRankThreshold, threshold);
        conf.setInt("mapreduce.task.timeout", 0);

        // input
        FileInputFormat.setInputPaths(job, input);
        FileInputFormat.setInputDirRecursive(job, true);
        job.setInputFormatClass(ComputationInputFormat.class);
        ComputationMapper.setEstimatedSize(job);

        // mapper
        job.setMapperClass(PageRankMapper.class);
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

    public static class PageRankMapper
            extends ComputationMapper<IntArrayWritable, DoubleWritable, DoubleWritable, DoubleWritable> {
        private final static Logger LOG = LoggerFactory.getLogger(PageRankMapper.class);

        private double threshold;
        private double rankSum;
        private double alpha;
        private long sourceVertex;
        private boolean personalized;
        private final DoubleWritable zero = new DoubleWritable(0.0);

        @Override
        protected void setupComputation(final Context context, final CoordinationService coordService) {
            final Configuration conf = context.getConfiguration();

            // retrieve parameters for PageRank algorithm
            alpha = conf.getDouble(Constants.PageRankDampingFactor, Constants.DefaultPageRankDampingFactor);
            personalized = conf.getBoolean(Constants.PersonalizedPageRank, false);
            sourceVertex = conf.getLong(Constants.PageRankSourceVertex, -1L);
            threshold = conf.getDouble(Constants.PageRankThreshold, Constants.DefaultPageRankThreshold);
        }

        @Override
        protected void cleanupComputation(final Context context, final CoordinationService coordService) {
            // calculate sum of PageRank scores
            double partialSum = 0;
            final VectorIterator<DoubleWritable> iter = currentVector().iterator(false, null);
            while (iter.next()) {
                partialSum += iter.currentValue().get();
            }

            // update sum of PageRank scores
            final Counter<Double> rankSumCounter = coordService.getZkCounter(
                    Constants.PageRankSumCounter, Double.class);
            rankSumCounter.update(partialSum);

            final Barrier barrier = coordService.getSemaphoreBarrier(Constants.PageRankSumBarrier);
            barrier.markCompleted();
            barrier.waitBarrier();

            rankSum = rankSumCounter.getSum();
        }

        @Override
        protected VectorCache<DoubleWritable> createVectorCache(
                final int numBlocks, final int blockId, final long numVertices) {
            final DoubleVectorCache cache = new DoubleVectorCache(numBlocks, blockId, numVertices, zero);
            final DoubleWritable init = new DoubleWritable(1.0 / numVertices());
            for (int i = 0, size = cache.size(); i < size; ++i) {
                if (!personalized) {
                    cache.put(i, init);
                } else {
                    if (IDUtils.localToGlobal(numBlocks, blockId, i) == sourceVertex) {
                        cache.put(i, new DoubleWritable(1.0));
                    }
                }
            }
            return cache;
        }

        @Override
        protected VectorCache<DoubleWritable> createIntermediateVectorCache(
                final int numBlocks, final long numVertices) {
            return new DoubleVectorCache(numBlocks, 0, numVertices, zero);
        }

        @Override
        protected MatrixCache<IntArrayWritable> createMatrixCache(
                final String blockName, final Configuration conf, final int numCols, final int numRows)
                throws IOException {
            return new UnweightedMatrixCache(blockName, conf, numCols, numRows);
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
        protected Class<IntArrayWritable> matrixValueClass() {
            return IntArrayWritable.class;
        }

        @Override
        protected void combine2Op(
                final int matBlockId,
                final MatrixCache<IntArrayWritable> matrix, final VectorCache<DoubleWritable> vecInput,
                final VectorCache<DoubleWritable> vecOutput) throws Exception {
            final DoubleWritable value = new DoubleWritable();
            final MatrixIterator<IntArrayWritable> iter = matrix.iterator();
            while (iter.next()) {
                final int col = iter.currentCol();
                final int degree = iter.numNonzeros();
                final IntArrayWritable rows = iter.currentRows();
                value.set(vecInput.get(col).get() / degree);
                for (int i = 0, size = rows.size(); i < size; ++i) {
                    final int row = rows.get(i);
                    vecOutput.addTo(row, value);
                }
            }
            iter.close();
        }

        @Override
        protected void combineAllOp(
                final int matBlockId,
                final RecordReader<IntWritable, DoubleWritable> vecInput, final VectorCache<DoubleWritable> vecOutput)
                throws Exception {
            while (vecInput.nextKeyValue()) {
                final int id = vecInput.getCurrentKey().get();
                vecOutput.addTo(id, vecInput.getCurrentValue());
            }
        }

        @Override
        protected void applyOp(
                final int vecBlockId,
                final VectorCache<DoubleWritable> vecResult, final VectorCache<DoubleWritable> vecIntermediate)
                throws Exception {
            final long numVertices = numVertices();
            final DoubleWritable value = new DoubleWritable();
            final VectorIterator<DoubleWritable> iter = vecResult.iterator(false, null);
            while (iter.next()) {
                final int id = iter.currentId();
                final double prevScore = iter.currentValue().get();
                final double newScore = alpha * vecIntermediate.get(id).get() + (1 - alpha) / numVertices;
                if (Math.abs(prevScore - newScore) > threshold) {
                    notifyChanged();
                    value.set(newScore);
                    vecResult.put(id, value);
                }
            }
        }

        @Override
        protected DoubleWritable finalizeOp(final int id, final DoubleWritable vec) throws Exception {
            vec.set(vec.get() / rankSum);
            return vec;
        }
    }
}
