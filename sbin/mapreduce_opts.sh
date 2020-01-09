#!/bin/bash

mapper_memory=6144
mapper_opts="-Xmx2304m -XX:+UseG1GC"
reducer_memory=6144
reducer_opts="-Xmx5120m -XX:+UseG1GC"
codec=org.apache.hadoop.io.compress.Lz4Codec
