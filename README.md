# FlexGraph
FlexGraph is a scalable graph mining system. It runs in parallel, distributed manner on top of Hadoop which is a widely-used distributed computation system.

FlexGraph provides the following algorithms:

* PageRank
* Random Walk with Restart (RWR)
* Single Source Shortest Path
* Weakly Connected Components

## Requirements
* Apache Hadoop 2.7.x, 2.8.x (other versions are not supported yet)
* Java 1.8.x or above

## Build
FlexGraph uses Maven to manage dependencies and build the whole project. To build the project, type the following command in terminal:

```
sbin/compile.sh
```

## Preparing Graph
FlexGraph assumes that the input graph is uploaded into HDFS (Hadoop Distributed File System), and consisted of multiple text files. FlexGraph supports two types of graphs: (1) unweighted graph, and (2) weighted graph.

For a unweighted graph, each line in the graph should be formatted as follows:

```
source_id<TAB>out_degree destination_id_1 destination_id_2 ... destination_id_n
```

Each line in a weighted graph should be formatted as follows:

```
source_id<TAB>out_degree destination_id_1 weight_1 destination_id_2 weight_2 ... destination_id_n weight_n
```

## Run Algorithms

### PageRank and Random Walk with Restart (RWR)
To run PageRank or Random Walk with Restart (RWR), you may type the following commands in terminal:

```
sbin/pagerank.sh \
  <PATH_TO_INPUT_GRAPH> \
  <PATH_TO_PARTITIONED_MATRIX> \
  <PATH_TO_STORE_PAGERANK> \
  <THRESHOLD_TO_SPLIT_HIGH_DEG_VERTEX> \
  <NUMBER_OF_BLOCKS> \
  <NUMBER_OF_MAXIMUM_ITERATION> \
  <DAMPING_FACTOR> \
  <SOURCE_VERTEX>
```

Note that the source vertex id is only required when you want to compute RWR scores instead of PageRank scores.

### Connected Components
To run connected components computation, you may type the following commands in terminal:

```
sbin/cc.sh \
  <PATH_TO_INPUT_GRAPH> \
  <PATH_TO_PARTITIONED_MATRIX> \
  <PATH_TO_STORE_PAGERANK> \
  <THRESHOLD_TO_SPLIT_HIGH_DEG_VERTEX> \
  <NUMBER_OF_BLOCKS>
```

### Single Source Shortest Path (SSSP)
To run single source shortest path computation, you may type the following commands in terminal:

```
sbin/sssp.sh \
  <PATH_TO_INPUT_GRAPH> \
  <PATH_TO_PARTITIONED_MATRIX> \
  <PATH_TO_STORE_PAGERANK> \
  <THRESHOLD_TO_SPLIT_HIGH_DEG_VERTEX> \
  <NUMBER_OF_BLOCKS> \
  <SOURCE_VERTEX>
```

# Contact
If you encounter any problem with FlexGraph, please feel free to contact [chiwanpark@snu.ac.kr](mailto:chiwanparK@snu.ac.kr).

# License
The source codes are distributed under Apache License 2.0.

```
Copyright 2018 SNU Data Mining Lab.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
