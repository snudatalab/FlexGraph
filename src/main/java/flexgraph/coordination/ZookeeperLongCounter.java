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

import org.apache.zookeeper.ZooKeeper;

import java.util.function.BinaryOperator;

/**
 * A counter implementation storing long values.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class ZookeeperLongCounter extends ZookeeperCounter<Long> {
    public ZookeeperLongCounter(ZooKeeper zk, String name, int workerId) {
        super(zk, name, workerId);
    }

    @Override
    protected Long negativeInfinite() {
        return Long.MIN_VALUE;
    }

    @Override
    protected Long positiveInfinite() {
        return Long.MAX_VALUE;
    }

    @Override
    protected Long zero() {
        return 0L;
    }

    @Override
    protected BinaryOperator<Long> sumOp() {
        return (a, b) -> a + b;
    }

    @Override
    protected Long valueFromString(final String string) {
        return Long.parseLong(string);
    }

    @Override
    protected String valueToString(final Long value) {
        return value.toString();
    }
}
