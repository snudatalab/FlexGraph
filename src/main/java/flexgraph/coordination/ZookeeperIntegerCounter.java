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
 * A counter implementation storing integer values.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class ZookeeperIntegerCounter extends ZookeeperCounter<Integer> {
    public ZookeeperIntegerCounter(ZooKeeper zk, String name, int workerId) {
        super(zk, name, workerId);
    }

    @Override
    protected Integer negativeInfinite() {
        return Integer.MIN_VALUE;
    }

    @Override
    protected Integer positiveInfinite() {
        return Integer.MAX_VALUE;
    }

    @Override
    protected Integer zero() {
        return 0;
    }

    @Override
    protected BinaryOperator<Integer> sumOp() {
        return (a, b) -> a + b;
    }

    @Override
    protected Integer valueFromString(String string) {
        return Integer.parseInt(string);
    }

    @Override
    protected String valueToString(Integer value) {
        return value.toString();
    }
}
