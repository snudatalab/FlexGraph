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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import flexgraph.Constants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * An input format for iterative computation.
 *
 * Each input split represents a worker that processes multiple sub-multiplication.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class ComputationInputFormat extends FileInputFormat<NullWritable, NullWritable> {
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public List<InputSplit> getSplits(final JobContext job) throws IOException {
        // get splits
        final List<InputSplit> fileSplits = super.getSplits(job);
        final int numBlocks = (int) Math.sqrt(fileSplits.parallelStream().mapToInt(
                split -> ComputationTaskSplit.blockId((FileSplit) split) + 1).max().getAsInt());

        // get statistics of blocks
        final Path path = FileInputFormat.getInputPaths(job)[0];
        final Path statsPath = new Path(path, Constants.StatPath);
        final FileSystem fs = statsPath.getFileSystem(job.getConfiguration());
        final int[] sparseCols = new int[numBlocks * numBlocks];
        final int[] sparseRows = new int[numBlocks * numBlocks];
        final int[] denseCols = new int[numBlocks * numBlocks];
        final int[] denseRows = new int[numBlocks * numBlocks];
        try (final BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(statsPath)))) {
            for (final String line : in.lines().collect(Collectors.toList())) {
                if (!line.startsWith("numVertices") && line.startsWith("num")) {
                    final String[] token = line.split("=");
                    final int blockId = Integer.parseInt(token[0].substring(token[0].length() - 5));
                    final int value = Integer.parseInt(token[1]);
                    if (line.startsWith("numSparseCols")) {
                        sparseCols[blockId] = value;
                    } else if (line.startsWith("numDenseCols")) {
                        denseCols[blockId] = value;
                    } else if (line.startsWith("numSparseRows")) {
                        sparseRows[blockId] = value;
                    } else if (line.startsWith("numDenseRows")) {
                        denseRows[blockId] = value;
                    }
                }
            }
        }

        // order the splits
        fileSplits.sort((left, right) -> {
            final Path pLeft = ((FileSplit) left).getPath();
            final Path pRight = ((FileSplit) right).getPath();
            return pLeft.toString().compareTo(pRight.toString());
        });

        // assign the splits
        final List<InputSplit> results = new ArrayList<>();
        for (int i = 0; i < numBlocks; ++i) {
            final ComputationTaskSplit split = new ComputationTaskSplit();
            split.taskId = i;
            split.sparseRows = new int[numBlocks];
            split.sparseCols = new int[numBlocks];
            split.denseRows = new int[numBlocks];
            split.denseCols = new int[numBlocks];
            results.add(split);
        }

        for (InputSplit split : fileSplits) {
            final FileSplit fSplit = (FileSplit) split;
            final String fileName = fSplit.getPath().getName();
            final int matBlockId = Integer.parseInt(fileName.split("-")[1]);
            if (fileName.contains("dense-")) {
                final ComputationTaskSplit taskSplit = (ComputationTaskSplit) results.get(matBlockId / numBlocks);
                taskSplit.denseSplits.add(fSplit);
                taskSplit.denseCols[matBlockId % numBlocks] = denseCols[matBlockId];
                taskSplit.denseRows[matBlockId % numBlocks] = denseRows[matBlockId];
            } else if (fileName.contains("sparse-")) {
                final ComputationTaskSplit taskSplit = (ComputationTaskSplit) results.get(matBlockId % numBlocks);
                taskSplit.sparseSplits.add(fSplit);
                taskSplit.sparseCols[matBlockId / numBlocks] = sparseCols[matBlockId];
                taskSplit.sparseRows[matBlockId / numBlocks] = sparseRows[matBlockId];
            } else if (fileName.contains(Constants.DenseVectorKey)) {
                final ComputationTaskSplit taskSplit = (ComputationTaskSplit) results.get(matBlockId);
                taskSplit.dvSplit = fSplit;
            } else if (fileName.contains(Constants.SparseVectorKey)) {
                final ComputationTaskSplit taskSplit = (ComputationTaskSplit) results.get(matBlockId);
                taskSplit.svSplit = fSplit;
            }
        }

        return results;
    }

    @Override
    public RecordReader<NullWritable, NullWritable> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new RecordReader<NullWritable, NullWritable>() {
            private boolean read = false;

            @Override
            public void initialize(InputSplit split, TaskAttemptContext context)
                    throws IOException, InterruptedException {
                read = false;
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                if (!read) {
                    read = true;
                    return true;
                }
                return false;
            }

            @Override
            public NullWritable getCurrentKey() throws IOException, InterruptedException {
                return NullWritable.get();
            }

            @Override
            public NullWritable getCurrentValue() throws IOException, InterruptedException {
                return NullWritable.get();
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return 0.5f;
            }

            @Override
            public void close() throws IOException {
            }
        };
    }

}
