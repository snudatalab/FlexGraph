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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import flexgraph.Constants;
import flexgraph.cache.BitSet;
import flexgraph.cache.MatrixCache;
import flexgraph.cache.VectorCache;
import flexgraph.cache.VectorIterator;
import flexgraph.coordination.Barrier;
import flexgraph.coordination.CoordinationService;
import flexgraph.coordination.Counter;
import flexgraph.io.ComputationTaskSplit;
import flexgraph.io.MatrixRecordReader;
import flexgraph.types.IntIntPairWritable;
import flexgraph.utils.IDUtils;
import flexgraph.utils.IOUtils;
import flexgraph.utils.MemoryUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A Mapper for GIM-V abstraction on FlexGraph.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public abstract class ComputationMapper
        <M extends Writable, V extends Writable, PV extends Writable, OV extends Writable>
        extends Mapper<NullWritable, NullWritable, LongWritable, OV> {
    private final Logger LOG = LoggerFactory.getLogger(ComputationMapper.class);

    private final CoordinationService coordService = new CoordinationService();

    private MatrixCache<M>[] denseBlocks;
    private MatrixCache<M>[] sparseBlocks;

    private long numVertices;
    private int denseThreshold;

    private Context context;

    private VectorCache<V> vecInput;
    private VectorCache<PV> vecIntermediate;
    private VectorCache<V> vecRemote;
    private BitSet denseSet;

    private int taskId;
    private int parallelism;
    private int numBlocks;

    private int numIterations = 1;
    private int maxIterations;

    private boolean vectorChanged;

    /**
     * Computes the estimated size of vector cache, and puts the size into configuration object.
     */
    public static void setEstimatedSize(final Job job) throws IOException {
        final Configuration conf = job.getConfiguration();
        final Path[] paths = FileInputFormat.getInputPaths(job);
        if (paths == null || paths.length == 0) {
            throw new IllegalArgumentException("Input graph is not set.");
        }

        final FileSystem fs = paths[0].getFileSystem(conf);
        long numVertices = -1;
        try (final BufferedReader reader =
                     new BufferedReader(new InputStreamReader(fs.open(new Path(paths[0], Constants.StatPath))))) {
            for (final String line : reader.lines().collect(Collectors.toList())) {
                if (line.startsWith("numVertices")) {
                    numVertices = Long.parseLong(line.split("=")[1]);
                }
            }
        }

        conf.setLong(Constants.SizeOfVector, numVertices);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////
    // Utility Methods for Coordination
    ////////////////////////////////////////////////////////////////////////////////////////////////////

    protected void statusMessage(final String message) {
        context.setStatus(message);
    }

    protected void reportStats(final String name, final long value) {
        coordService.getHadoopCounter(name).update(value);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////
    // Utility Methods for Vector I/O
    ////////////////////////////////////////////////////////////////////////////////////////////////////

    private void readDenseSet(final Path path) throws IOException, InterruptedException {
        final RecordReader<IntWritable, NullWritable> reader = IOUtils.getIntermediateVectorReader(
                context, path, NullWritable.class);
        if (reader != null) {
            while (reader.nextKeyValue()) {
                final int key = reader.getCurrentKey().get();
                denseSet.add(key);
            }
            reader.close();
        }
    }

    protected void writeOutputVecOp(final int vecBlockId, final VectorCache<V> vector) throws Exception {
        final LongWritable keyOut = new LongWritable();
        final VectorIterator<V> iter = vector.iterator(false, null);
        while (iter.next()) {
            final long key = IDUtils.localToGlobal(numBlocks, vecBlockId, iter.currentId());
            keyOut.set(key);
            context.write(keyOut, finalizeOp(iter.currentId(), iter.currentValue()));
        }
    }

    protected <T extends Writable> void writeIntermediateVecOp(
            final VectorCache<T> vector, final String key, final int blockId) throws IOException, InterruptedException {
        final boolean denseFlag = Constants.DenseVectorKey.equalsIgnoreCase(key);
        final RecordWriter<IntWritable, T> vecWriter = IOUtils.getIntermediateVectorWriter(context, key, blockId);
        final VectorIterator<T> iter;
        if (denseFlag) {
            iter = vector.iterator(true, denseSet);
        } else {
            iter = vector.iterator(false, null);
        }

        final IntWritable keyOut = new IntWritable();
        while (iter.next()) {
            keyOut.set(iter.currentId());
            vecWriter.write(keyOut, iter.currentValue());
        }
        vecWriter.close(context);
    }

    private void combine2Sparse() throws Exception {
        // Avoid to run this operation with FlexGraph_horizontal
        if (denseThreshold == 0) return;

        LOG.info("Run combine2 for sparse blocks (iteration: {}/{})", numIterations, maxIterations);
        statusMessage(String.format("iter%d: combine2(sparse)", numIterations));
        for (int i = 0; i < parallelism; ++i) {
            final int blockId = ((taskId + i + 1) % parallelism) * parallelism + taskId;

            // apply 'combine2' operation on sparse matrix block
            combine2Op(blockId, sparseBlocks[blockId / parallelism], vecInput, vecIntermediate);

            // write the result partial vector to distributed storage
            if (i == parallelism - 1) break;
            writeIntermediateVecOp(vecIntermediate, Constants.IntermediateVectorKey, blockId);
            vecIntermediate.clear();
        }

        coordService.getSemaphoreBarrier(String.format("combine2Sparse-iter%d", numIterations)).markCompleted();
    }

    private void combineAllSparse(final Class<PV> pvValueClass) throws Exception {
        // Avoid to run this operation with FlexGraph_horizontal
        if (denseThreshold == 0) return;

        // wait all other workers finish combine2Sparse operation
        LOG.info("Wait combine2 for sparse blocks (iteration: {}/{})", numIterations, maxIterations);
        coordService.getSemaphoreBarrier(String.format("combine2Sparse-iter%d", numIterations)).waitBarrier();

        LOG.info("Run combineAll for sparse blocks (iteration: {}/{})", numIterations, maxIterations);
        statusMessage(String.format("iter%d: combineAll(sparse)", numIterations));
        for (int i = 0; i < parallelism - 1; ++i) {
            final int blockId = (taskId + i + 1) % parallelism + taskId * parallelism;

            // read the partial vector and apply the 'combineAll' operation
            final RecordReader<IntWritable, PV> pvReader = IOUtils.getIntermediateVectorReader(
                    context, Constants.IntermediateVectorKey, blockId, pvValueClass);
            if (pvReader != null) {
                combineAllOp(blockId, pvReader, vecIntermediate);
                pvReader.close();
            }
        }
    }

    private void combineDense(final Class<V> vecValueClass) throws Exception {
        if (denseThreshold < 0) return;

        LOG.info("Run combine2 and combineAll for dense blocks (iteration: {}/{})", numIterations, maxIterations);
        statusMessage(String.format("iter%d: combine(dense)", numIterations));
        combine2Op(taskId * parallelism + taskId, denseBlocks[taskId], vecInput, vecIntermediate);
        for (int i = 0; i < parallelism - 1; ++i) {
            final int blockId = (taskId + i + 1) % parallelism;

            // read the dense vector
            vecRemote.clear();
            final RecordReader<IntWritable, V> dvReader = IOUtils.getIntermediateVectorReader(
                    context, Constants.DenseVectorKey, blockId, vecValueClass);
            if (dvReader != null) {
                while (dvReader.nextKeyValue()) {
                    vecRemote.put(dvReader.getCurrentKey().get(), dvReader.getCurrentValue());
                }
                dvReader.close();
            }

            // apply the 'combine2' and 'combineAll' operations to dense block in once
            combine2Op(taskId * parallelism + blockId, denseBlocks[blockId], vecRemote, vecIntermediate);
        }
    }

    /**
     * Main loop of FlexGraph.
     *
     * @param context Hadoop context object
     */
    @Override
    public void run(final Context context) throws IOException, InterruptedException {
        try {
            final ComputationTaskSplit task = ((ComputationTaskSplit) context.getInputSplit());
            taskId = task.taskId;

            setupFramework(context);
            setupComputation(context, coordService);

            // read vector blocks
            readDenseSet(task.dvSplit.getPath());

            // read matrix blocks
            readMatrixBlocks(context, task);

            // clear unused memory
            MemoryUtils.callGCMultipleTimes();

            boolean converged = false;
            while (!converged) {
                // initialize time counter
                final long tsIterBegin = System.nanoTime();

                // clear values for prev iteration
                vectorChanged = false;
                vecIntermediate.clear();

                // 'combine2' for sparse blocks
                combine2Sparse();
                if (numIterations == 1) MemoryUtils.callGCMultipleTimes();

                // 'combineAll' for sparse blocks
                combineAllSparse(intermediateVectorValueClass());
                if (numIterations == 1) MemoryUtils.callGCMultipleTimes();

                // combine for dense blocks
                combineDense(vectorValueClass());
                if (numIterations == 1) MemoryUtils.callGCMultipleTimes();

                // apply 'apply' operation to all blocks
                statusMessage(String.format("iter%d: apply", numIterations));
                LOG.info("Run apply for all blocks (iteration: {}/{})", numIterations, maxIterations);
                applyOp(taskId, vecInput, vecIntermediate);
                if (numIterations == 1) MemoryUtils.callGCMultipleTimes();

                ++numIterations;
                converged = (maxIterations != -1 && numIterations > maxIterations) || !vectorChanged;

                final Counter<Boolean> convergedCounter = coordService.getZkCounter(
                        String.format("iterConverged-%d", numIterations - 1), Boolean.class);
                convergedCounter.update(converged);

                final Barrier iterBarrier = coordService.getSemaphoreBarrier(
                        String.format("iterBarrier-%d", numIterations - 1));
                iterBarrier.markCompleted();
                iterBarrier.waitBarrier();

                converged = convergedCounter.getMinimum();

                // write dense blocks for next iteration
                if (!converged) {
                    if (denseThreshold >= 0) {
                        statusMessage(String.format("iter%d: write dense vector", numIterations));
                        LOG.info("Write dense blocks for next iteration (iteration: {}/{})",
                                numIterations - 1, maxIterations);
                        writeIntermediateVecOp(vecInput, Constants.DenseVectorKey, taskId);
                    }

                    statusMessage(String.format("iter%d: waiting", numIterations));
                    LOG.info("Wait until other workers are completed");
                    final Barrier cleanupBarrier = coordService.getSemaphoreBarrier(
                            String.format("cleanupBarrier-%d", numIterations - 1));
                    cleanupBarrier.markCompleted();
                    cleanupBarrier.waitBarrier();

                    LOG.info("Cleanup iteration (iteration: {}/{})", numIterations - 1, maxIterations);
                    if (numIterations == 2) MemoryUtils.callGCMultipleTimes();
                }

                // report time per iteration
                if (coordService.getWorkerId() == 0) {
                    final long duration = (System.nanoTime() - tsIterBegin) / 1000000;
                    reportStats(String.format("time.iter%03d", numIterations - 1), duration);
                }
            }

            cleanupComputation(context, coordService);

            // sparse and dense part
            LOG.info("Write the final vector");
            statusMessage("write sparse vector");
            writeOutputVecOp(taskId, vecInput);

            LOG.info("Cleanup MapReduce job");
            cleanupFramework(context);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private void setupFramework(final Context context) throws InterruptedException, IOException {
        final Configuration conf = context.getConfiguration();
        parallelism = conf.getInt(Constants.Parallelism, 1);
        numBlocks = parallelism; // TODO: number of blocks != parallelism
        maxIterations = conf.getInt(Constants.MaxIterations, 1);

        // initialize compression codec
        final CompressionCodec codec = IOUtils.getPreferredCompressionCodec(conf);
        CodecPool.returnCompressor(CodecPool.getCompressor(codec));
        CodecPool.returnDecompressor(CodecPool.getDecompressor(codec));

        // initialize coordination service
        try {
            coordService.setup(context);
        } catch (KeeperException e) {
            // ignore
        }

        // initialize graph information
        denseThreshold = conf.getInt(Constants.DenseThreshold, Constants.DefaultDenseThreshold);
        numVertices = conf.getLong(Constants.SizeOfVector, -1L);

        // initialize matrix
        denseBlocks = new MatrixCache[parallelism];
        sparseBlocks = new MatrixCache[parallelism];

        // initialize vector
        vecInput = createVectorCache(numBlocks, taskId, numVertices);
        vecIntermediate = createIntermediateVectorCache(numBlocks, numVertices);
        vecRemote = createVectorCache(numBlocks, 0, numVertices);

        denseSet = new BitSet((int) Math.ceil(numVertices / (double) numBlocks));

        // initialize context
        this.context = context;
    }

    private void cleanupFramework(final Context context) throws InterruptedException, IOException {
        if (coordService.getWorkerId() == 0) {
            coordService.getHadoopCounter(Constants.ExecutedIterations).update((long) (numIterations - 1));

            final Configuration conf = context.getConfiguration();
            final Path intermediatePath = IOUtils.getIntermediateVectorPath(
                    context, Constants.IntermediateVectorKey, 0).getParent();
            final FileSystem fs = intermediatePath.getFileSystem(conf);
            fs.delete(intermediatePath, true);
        }

        coordService.cleanup(context);
    }

    private void readMatrixBlocks(final Context context, final ComputationTaskSplit task)
            throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();
        final RecordReader<IntIntPairWritable, M> reader = new MatrixRecordReader<>(matrixValueClass(), conf);

        final List<FileSplit> blocks = new ArrayList<>();
        if (denseThreshold >= 0) {
            final List<FileSplit> denseBlocks = new ArrayList<>(task.denseSplits);
            denseBlocks.sort((left, right) -> {
                final String leftName = left.getPath().getName();
                final String rightName = right.getPath().getName();
                final int leftId = Integer.parseInt(leftName.substring(leftName.length() - 5));
                final int rightId = Integer.parseInt(rightName.substring(rightName.length() - 5));
                final int leftOrder = (leftId - parallelism * taskId + parallelism - taskId) % parallelism;
                final int rightOrder = (rightId - parallelism * taskId + parallelism - taskId) % parallelism;
                return leftOrder - rightOrder;
            });

            blocks.addAll(denseBlocks);
        }
        if (denseThreshold != 0) blocks.addAll(task.sparseSplits);

        for (FileSplit block : blocks) {
            final String blockName = block.getPath().getName();
            final int blockId = ComputationTaskSplit.blockId(block);
            statusMessage(String.format("load matrix block: %s", blockName));
            final boolean isSparse = blockName.startsWith("sparse");
            final int numCols;
            final int numRows;
            if (isSparse) {
                numCols = task.sparseCols[blockId / parallelism];
                numRows = task.sparseRows[blockId / parallelism];
            } else {
                numCols = task.denseCols[blockId % parallelism];
                numRows = task.denseRows[blockId % parallelism];
            }

            final MatrixCache<M> cache = createMatrixCache(blockName, conf, numCols, numRows);
            reader.initialize(block, context);
            while (reader.nextKeyValue()) {
                cache.put(reader.getCurrentKey(), reader.getCurrentValue());
            }
            reader.close();
            cache.freeze();

            if (isSparse) sparseBlocks[blockId / parallelism] = cache;
            else denseBlocks[blockId % parallelism] = cache;
        }
    }

    /**
     * Notifies the fact that there are some changes on vector to master.
     */
    protected void notifyChanged() {
        vectorChanged = true;
    }

    /**
     * Returns the number of vertices in the input graph.
     *
     * @return the number of vertices
     */
    protected long numVertices() {
        return numVertices;
    }

    /**
     * Returns the number of current iteration.
     *
     * @return the number of current iteration
     */
    protected int currentIteration() {
        return numIterations;
    }

    /**
     * Returns the number of maximum iterations which set by user.
     *
     * @return -1 if there is no specified number of maximum iterations, otherwise the set number
     */
    protected int maxIterations() {
        return maxIterations;
    }

    /**
     * Returns the number of vector blocks.
     *
     * @return the number of vector blocks
     */
    protected int numBlocks() {
        return numBlocks;
    }

    /**
     * Returns the input vector of current iteration.
     *
     * @return a vector cache that contains the input vector of current iteration
     */
    protected VectorCache<V> currentVector() {
        return vecInput;
    }

    /**
     * Returns Hadoop context object.
     *
     * @return Hadoop context object
     */
    protected Context computationContext() {
        return context;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////
    // Matrix-Vector Multiplication Interfaces for Graph Mining
    ////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Initializes GIM-V computation on distributed workers.
     *
     * Note that this method is called once before starting iterative computations.
     *
     * @param context Hadoop context object
     * @param coordService a coordnation utility object that can communicate with other workers.
     */
    protected void setupComputation(final Context context, final CoordinationService coordService) {
    }

    /**
     * Finalizes GIM-V computation.
     *
     * Note that this method is called once after finishing iterative computations.
     *
     * @param context Hadoop context object
     * @param coordService a coordnation utility object that can communicate with other workers.
     */
    protected void cleanupComputation(final Context context, final CoordinationService coordService) {
    }

    /**
     * Creates a VectorCache for input vector block.
     *
     * This method is required to initialize the value of input vector properly.
     * For example, in PageRank, the value of initialized vector should be 1/n.
     *
     * @param numBlocks the number of vector blocks
     * @param blockId the block id
     * @param numVertices the number of vertices in input graph
     */
    protected abstract VectorCache<V> createVectorCache(final int numBlocks, final int blockId, final long numVertices);

    /**
     * Creates a VectorCache for intermediate vector block.
     *
     * This method is required to initialize the value of intermeidate vector properly.
     * For example, in connected components computation, the value of initialized vector should be vertex index.
     *
     * @param numBlocks the number of vector blocks
     * @param numVertices the number of vertices in input graph
     */
    protected abstract VectorCache<PV> createIntermediateVectorCache(final int numBlocks, final long numVertices);

    /**
     * Creates a MatrixCache for matrix local caching.
     *
     * @param blockName the name of matrix block
     * @param conf Hadoop configuration object
     * @param numCols the number of columns in a block
     * @param numRows the number of rows in a block
     */
    protected abstract MatrixCache<M> createMatrixCache(
            final String blockName, final Configuration conf, final int numCols, final int numRows) throws IOException;

    /**
     * Returns a class object that represents the vector value type.
     *
     * @return a class object
     */
    protected abstract Class<V> vectorValueClass();

    /**
     * Returns a class object that represents that intermeidate vector value.
     *
     * @return a class object
     */
    protected abstract Class<PV> intermediateVectorValueClass();

    /**
     * Returns a class object that represents that matrix value.
     *
     * @return a class object
     */
    protected abstract Class<M> matrixValueClass();

    /**
     * Performs the combine2 operation given a vector block and a matrix block.
     *
     * @param matBlockId the block id of marix block
     * @param vecInput the vector block that contains input vector values
     * @param vecOutput the vector block that will contain results of partial multiplication
     */
    protected void combine2Op(
            final int matBlockId,
            final MatrixCache<M> matrix, final VectorCache<V> vecInput, final VectorCache<PV> vecOutput)
            throws Exception {
    }

    /**
     * Performs the combineAll operation given a partially computed vector block, and a merged block.
     *
     * @param matBlockId the block id of matrix block which produces this partially computed vector block
     * @param vecInput the partially computed vector block
     * @param vecOutput the vector block that will contain merged results
     */
    protected void combineAllOp(
            final int matBlockId,
            final RecordReader<IntWritable, PV> vecInput, final VectorCache<PV> vecOutput)
            throws Exception {
    }

    /**
     * Performs the apply operation given a combined vector block, and an input vector block.
     *
     * @param vecBlockId the block id of vector block
     * @param vecResult the vector block that contains input vector block
     * @param vecIntermediate the vector block that contains combined vector block
     */
    protected void applyOp(
            final int vecBlockId, final VectorCache<V> vecResult, final VectorCache<PV> vecIntermediate)
            throws Exception {
    }

    /**
     * Finalizes each value of output vector.
     *
     * @param id index
     * @param vec value of indexed element
     *
     * @return a final value of the vector element
     */
    @SuppressWarnings("unchecked")
    protected OV finalizeOp(final int id, final V vec) throws Exception {
        return (OV) vec;
    }
}
