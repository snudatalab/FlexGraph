#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

source $DIR/mapreduce_opts.sh

input=$1
output=$2
threshold=$3
blocks=$4
direction=$5

hdfs dfs -test -e $output
if [[ $? -eq 0 ]]; then
  hdfs dfs -rm -r $output
fi

hadoop jar $DIR/$jarname \
  flexgraph.prepartitioning.UnweightedGraphPrepartitioning \
  -D mapreduce.map.java.opts="$mapper_opts" -D mapreduce.map.memory.mb=$mapper_memory \
  -D mapreduce.reduce.java.opts="$reducer_opts" -D mapreduce.reduce.memory.mb=$reducer_memory \
  -D mapreduce.job.reduces=$blocks \
  -D mapreduce.map.output.compress=true -D mapreduce.map.output.compress.codec=$codec \
  -D mapreduce.output.fileoutputformat.compress=true -D mapreduce.output.fileoutputformat.compress.type=BLOCK \
  -D mapreduce.output.fileoutputformat.compress.codec=$codec -D flexgraph.io.denseThreshold=$threshold \
  $input $output $direction adjlist
