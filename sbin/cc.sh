#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

# compile FlexGraph
$DIR/compile.sh

source $DIR/mapreduce_opts.sh

input=$1
partitioned=$2
output=$3
threshold=$4
blocks=$5

if [ ! -z "$6" ]; then
  max_iter=$6
else
  max_iter=-1
fi

# pre-partitioning step
$DIR/prepartitioning_weighted.sh $input $partitioned $threshold $blocks directed

hdfs dfs -test -e $output
if [[ $? -eq 0 ]]; then
  hdfs dfs -rm -r $output
fi

# sssp
hadoop jar $DIR/$jarname \
  flexgraph.computation.CCComputation \
  -D mapreduce.map.java.opts="$mapper_opts" -D mapreduce.map.memory.mb=$mapper_memory \
  -D mapreduce.reduce.java.opts="$reducer_opts" -D mapreduce.reduce.memory.mb=$reducer_memory \
  -D flexgraph.io.denseThreshold=$threshold -D maxIterations=$max_iter \
  $partitioned $output $blocks

hdfs dfs -rm -r $partitioned
