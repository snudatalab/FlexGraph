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
package flexgraph.coordination;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import flexgraph.Constants;

/**
 * A counter implementation storing long-type value via Hadoop-native counter.
 *
 * Note that some operations (getMinimum, getMaximum, getSum) are not supported.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class HadoopCounter implements Counter<Long> {
    private final org.apache.hadoop.mapreduce.Counter counter;

    /**
     * Creates a counter.
     *
     * @param context Hadoop context object
     * @param name the name of counter
     */
    public HadoopCounter(final TaskAttemptContext context, final String counterName) {
        counter = context.getCounter(Constants.CounterGroup, counterName);
    }

    @Override
    public void update(Long value) {
        counter.increment(value);
    }

    @Override
    public Long getMinimum() {
        throw new RuntimeException("Not supported operation");
    }

    @Override
    public Long getMaximum() {
        throw new RuntimeException("Not supported operation");
    }

    @Override
    public Long getSum() {
        throw new RuntimeException("Not supported operation");
    }
}
