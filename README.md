# FlexGraph

This project is an open source implementation of "FlexGraph: Flexible Partitioning and Storage for Scalable Graph Mining". FlexGraph is a scalable graph mining system that runs in parallel and distributed manners on top of Apache Hadoop.

FlexGraph provides the following algorithms:

* PageRank
* Random Walk with Restart (RWR)
* Single Source Shortest Path
* Weakly Connected Components

## Abstract

How can we analyze large graphs such as the Web, and social networks with hundreds of billions of vertices and edges? Although many graph mining systems have been proposed to perform various graph mining algorithms on such large graphs, they have difficulties in processing Web-scale graphs due to massive I/O cost caused by communication between workers, and reading subgraphs repeatedly.

In this paper, we propose FlexGraph, a scalable distributed graph mining method reducing the I/O cost by exploiting properties of real-world graphs. FlexGraph significantly decreases the communication cost, which is the main bottleneck of distributed systems, by exploiting different edge placement policies based on the type of vertices. Furthermore, we propose a flexible storage format to reduce the I/O cost when reading input graph repeatedly. Experiments show that FlexGraph succeeds in processing up to 64x larger graphs than existing distributed memory-based graph mining methods, and consistently outperforms previous disk-based graph mining methods.

## Datasets

| Name | # of Vertices | # of Edges | Description | Source |
| ---- | ------------: | ---------: | ----------- | ------ |
| ClueWeb12 | 6,231,126,594	| 71,746,553,402 | Page-level hyperlink network on the WWW | [Lemur Project](http://www.lemurproject.org/clueweb12.php/) |
| ClueWeb09 | 1,684,876,525	|  7,939,647,897 | Page-level hyperlink network on the WWW | [Lemur Project](http://www.lemurproject.org/clueweb12.php/) |
| YahooWeb | 720,242,173 | 6,636,600,779 | Page-level hyperlink network on the WWW | [Yahoo!](https://webscope.sandbox.yahoo.com/) |
| Twitter | 41,652,230 | 1,468,365,182 | Who-follows-whom in Twitter | [Kwak et al.](http://an.kaist.ac.kr/traces/WWW2010.html) |

## Reference

- FlexGraph: Flexible Partitioning and Storage for Scalable Graph Mining (under-review)  
Chiwan Park, Ha-Myung Park, and U Kang (Seoul National University)

# How to Run

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

## Running Algorithms

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

## Contact

If you encounter any problem with FlexGraph, please feel free to contact [chiwanpark@snu.ac.kr](mailto:chiwanparK@snu.ac.kr).

