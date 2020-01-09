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
package flexgraph;

public class Constants {
    public static final String EdgeListDelimiter = "flexgraph.io.delimiter.edgeListPair";
    public static final String AdjacencyListVertexDelimiter = "flexgraph.io.delimiter.adjacencyList.vertex";
    public static final String AdjacencyListEdgeDelimiter = "flexgraph.io.delimiter.adjacencyList.edge";

    public static final String StatPath = "_STATS";

    public static final String DenseThreshold = "flexgraph.io.denseThreshold";
    public static final int DefaultDenseThreshold = 100;

    public static final String SizeOfVector = "flexgraph.vector.size";

    public static final int InitArrayWritableSize = 1024;

    public static final String Parallelism = "flexgraph.parallelism";
    public static final String ExecutedIterations = "executedIterations";
    public static final String MaxIterations = "flexgraph.maxIterations";

    public static final int SizeOfMemoryBarrier = 256 * 1024 * 1024;

    public static final int MaxNumWorkers = 1024;
    public static final int CoordinationTickTime = 3000;
    public static final String BarrierRoot = "/flexgraph.barrier";
    public static final String CounterRoot = "/flexgraph.counter";
    public static final String CounterGroup = "flexgraph.counter";

    public static final String DFSTemporaryPath = "/tmp";

    public static final String SparseVectorKey = "sv";
    public static final String DenseVectorKey = "dv";
    public static final String IntermediateVectorKey = "pv";

    public static final double DoubleValueZeroThreshold = 1e-7;

    public static final String PageRankDampingFactor = "flexgraph.pagerank.dampingFactor";
    public static final double DefaultPageRankDampingFactor = 0.85;
    public static final String PersonalizedPageRank = "flexgraph.pagerank.personalized";
    public static final String PageRankSourceVertex = "flexgraph.pagerank.sourceVertex";
    public static final String PageRankSumCounter = "flexgraph.pagerank.rankSum";
    public static final String PageRankSumBarrier = "pagerank-sum";
    public static final String PageRankThreshold = "flexgraph.pagerank.threshold";
    public static final double DefaultPageRankThreshold = 1e-8;

    public static final String SSSPSourceVertex = "flexgraph.sssp.sourceVertex";

    public static final double PHI = 0.77351;
    public static final String DiameterNumIterations = "flexgraph.diameter.numIterations";
    public static final String DiameterNeighborhood = "diameter.neighborhood";
    public static final String DiameterNeighborhoodFile = "flexgraph.diameter-neighborhood";

    public static final int DefaultNumBitstrings = 32;

    public static final String DistributionMinValue = "flexgraph.distribution.minValue";
    public static final String DistributionMaxValue = "flexgraph.distribution.maxValue";
    public static final String DistributionNumBins = "flexgraph.distribution.numBins";
    public static final int DistributionDefaultNumBins = 1000;
}
