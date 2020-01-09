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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import flexgraph.Constants;
import flexgraph.coordination.Barrier;
import flexgraph.coordination.CoordinationService;
import flexgraph.utils.IOUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * A base implementation for reducer in pre-partitioning step.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class GraphPrepartitioningReducer<IV, OV> extends Reducer<LongWritable, IV, IntWritable, OV> {
    private static final Logger LOG = LoggerFactory.getLogger(GraphPrepartitioningReducer.class);

    protected long maxVertex = Long.MIN_VALUE;
    protected int numBlocks;
    protected int[] numSparseCols;
    protected int[] numSparseRows;
    protected int[] numDenseCols;
    protected int[] numDenseRows;

    protected RecordWriter<IntWritable, NullWritable> svWriter;
    protected RecordWriter<IntWritable, NullWritable> dvWriter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // initialize vector writer
        final int taskId = context.getTaskAttemptID().getTaskID().getId();
        svWriter = IOUtils.getIntermediateVectorWriter(context, Constants.SparseVectorKey, taskId);
        dvWriter = IOUtils.getIntermediateVectorWriter(context, Constants.DenseVectorKey, taskId);

        // initialize statistics of blocks
        numBlocks = context.getNumReduceTasks();
        numSparseRows = new int[numBlocks];
        numSparseCols = new int[numBlocks];
        numDenseRows = new int[numBlocks];
        numDenseCols = new int[numBlocks];
    }

    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException {
        svWriter.close(context);
        dvWriter.close(context);

        final CoordinationService coordService = new CoordinationService();
        try {
            coordService.setup(context);
        } catch (KeeperException e) {
            LOG.error("Initialization of coordination service failed.", e);
            return;
        }

        final int vecBlockId = coordService.getWorkerId();
        coordService.getZkCounter("maxVertex", Long.class).update(maxVertex);
        for (int i = 0; i < numBlocks; ++i) {
            final int matBlockId = i * numBlocks + vecBlockId;
            final String sparseRowCounterId = String.format("numSparseRows%05d", matBlockId);
            final String denseRowCounterId = String.format("numDenseRows%05d", matBlockId);
            final String sparseColCounterId = String.format("numSparseCols%05d", matBlockId);
            final String denseColCounterId = String.format("numDenseCols%05d", matBlockId);
            coordService.getZkCounter(sparseRowCounterId, Integer.class).update(numSparseRows[i]);
            coordService.getZkCounter(denseRowCounterId, Integer.class).update(numDenseRows[i]);
            coordService.getZkCounter(sparseColCounterId, Integer.class).update(numSparseCols[i]);
            coordService.getZkCounter(denseColCounterId, Integer.class).update(numDenseCols[i]);
        }

        final Barrier barrier = coordService.getSemaphoreBarrier("countBarrier");
        barrier.setup();
        barrier.markCompleted();

        if (vecBlockId == 0) {
            barrier.waitBarrier();
            final long numVertices = coordService.getZkCounter("maxVertex", Long.class).getMaximum() + 1;

            final Path statPath = new Path(FileOutputFormat.getOutputPath(context), Constants.StatPath);
            final FileSystem fs = statPath.getFileSystem(context.getConfiguration());
            try (final BufferedWriter statOut = new BufferedWriter(new OutputStreamWriter(fs.create(statPath)))) {
                statOut.write(String.format("numVertices=%d\n", numVertices));
                for (int i = 0; i < numBlocks; ++i) {
                    for (int j = 0; j < numBlocks; ++j) {
                        final int matBlockId = i * numBlocks + j;
                        final String sparseRowCounterId = String.format("numSparseRows%05d", matBlockId);
                        final String denseRowCounterId = String.format("numDenseRows%05d", matBlockId);
                        final String sparseColCounterId = String.format("numSparseCols%05d", matBlockId);
                        final String denseColCounterId = String.format("numDenseCols%05d", matBlockId);
                        statOut.write(String.format("%s=%d\n", sparseRowCounterId,
                                coordService.getZkCounter(sparseRowCounterId, Integer.class).getSum()));
                        statOut.write(String.format("%s=%d\n", denseRowCounterId,
                                coordService.getZkCounter(denseRowCounterId, Integer.class).getSum()));
                        statOut.write(String.format("%s=%d\n", sparseColCounterId,
                                coordService.getZkCounter(sparseColCounterId, Integer.class).getSum()));
                        statOut.write(String.format("%s=%d\n", denseColCounterId,
                                coordService.getZkCounter(denseColCounterId, Integer.class).getSum()));
                    }
                }
            }
        }

        coordService.cleanup(context);
    }
}
