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
 * A counter implementation storing boolean values.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class ZookeeperBooleanCounter extends ZookeeperCounter<Boolean> {
    public ZookeeperBooleanCounter(ZooKeeper zk, String name, int workerId) {
        super(zk, name, workerId);
    }

    @Override
    protected Boolean negativeInfinite() {
        return false;
    }

    @Override
    protected Boolean positiveInfinite() {
        return true;
    }

    @Override
    protected Boolean zero() {
        return false;
    }

    @Override
    protected BinaryOperator<Boolean> sumOp() {
        return (a, b) -> a | b;
    }

    @Override
    protected Boolean valueFromString(String string) {
        return "true".equalsIgnoreCase(string);
    }

    @Override
    protected String valueToString(Boolean value) {
        return value.toString();
    }
}
