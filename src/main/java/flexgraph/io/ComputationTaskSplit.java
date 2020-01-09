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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * An input split that represents a single worker that processes multiple sub-multiplications.
 *
 * The sub-multiplications are classified into two classes: (1) sparse sub-multiplication, and
 * (2) dense sub-multiplication.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class ComputationTaskSplit extends InputSplit implements Writable {
    public int taskId;
    public List<FileSplit> sparseSplits = new ArrayList<>();
    public int[] sparseRows;
    public int[] sparseCols;
    public List<FileSplit> denseSplits = new ArrayList<>();
    public int[] denseRows;
    public int[] denseCols;
    public FileSplit svSplit;
    public FileSplit dvSplit;

    @Override
    public long getLength() throws IOException, InterruptedException {
        int length = 0;
        for (final FileSplit child : sparseSplits) {
            length += child.getLength();
        }
        for (final FileSplit child : denseSplits) {
            length += child.getLength();
        }
        length += svSplit.getLength() + dvSplit.getLength();
        return length;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        final Set<String> locations = new HashSet<>();

        for (final FileSplit child : sparseSplits) {
            locations.addAll(Arrays.asList(child.getLocations()));
        }

        return locations.toArray(new String[0]);
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeInt(taskId);
        out.writeInt(sparseSplits.size());
        for (int i = 0, size = sparseSplits.size(); i < size; ++i) {
            sparseSplits.get(i).write(out);
            out.writeInt(sparseCols[i]);
            out.writeInt(sparseRows[i]);
        }

        out.writeInt(denseSplits.size());
        for (int i = 0, size = denseSplits.size(); i < size; ++i) {
            denseSplits.get(i).write(out);
            out.writeInt(denseRows[i]);
            out.writeInt(denseCols[i]);
        }

        svSplit.write(out);
        dvSplit.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        taskId = in.readInt();
        final int sparseLength = in.readInt();
        sparseSplits = new ArrayList<>(sparseLength);
        sparseCols = new int[sparseLength];
        sparseRows = new int[sparseLength];
        for (int i = 0; i < sparseLength; ++i) {
            sparseSplits.add(new FileSplit());
            sparseSplits.get(i).readFields(in);
            sparseCols[i] = in.readInt();
            sparseRows[i] = in.readInt();
        }

        final int denseLength = in.readInt();
        denseSplits = new ArrayList<>(denseLength);
        denseCols = new int[denseLength];
        denseRows = new int[denseLength];
        for (int i = 0; i < denseLength; ++i) {
            denseSplits.add(new FileSplit());
            denseSplits.get(i).readFields(in);
            denseCols[i] = in.readInt();
            denseRows[i] = in.readInt();
        }

        svSplit = new FileSplit();
        svSplit.readFields(in);
        dvSplit = new FileSplit();
        dvSplit.readFields(in);
    }

    public static int blockId(FileSplit split) {
        return Integer.parseInt(split.getPath().getName().split("-")[1]);
    }
}
