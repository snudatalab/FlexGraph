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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * A vertex partitioner that assigns vertices randomly via hash function.
 */
public class PrepartitioningPartitioner extends Partitioner<LongWritable, Object> {
    @Override
    public int getPartition(LongWritable vertex, Object ignored, int numPartitions) {
        return (int) (vertex.get() % numPartitions);
    }
}
