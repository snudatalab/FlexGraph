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
import flexgraph.Constants;
import flexgraph.utils.ZookeeperUtils;

import java.util.Comparator;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

/**
 * A base implementation of counter based on Zookeeper.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public abstract class ZookeeperCounter<T extends Comparable<T>> implements Counter<T> {
    private final ZooKeeper zk;
    private final String counterPath;
    private final String myCounterPath;

    public ZookeeperCounter(final ZooKeeper zk, final String name, final int workerId) {
        this.zk = zk;
        counterPath = String.format("%s/%s", Constants.CounterRoot, name);
        myCounterPath = String.format("%s/%d", counterPath, workerId);

        if (!ZookeeperUtils.exists(zk, counterPath)) {
            ZookeeperUtils.createPersistentNode(zk, counterPath);
        }
    }

    @Override
    public void update(final T value) {
        ZookeeperUtils.createOrUpdatePersistentNode(zk, myCounterPath, valueToString(value));
    }

    @Override
    public T getMinimum() {
        return getCounters().min(Comparator.naturalOrder()).orElse(positiveInfinite());
    }

    @Override
    public T getMaximum() {
        return getCounters().max(Comparator.naturalOrder()).orElse(negativeInfinite());
    }

    @Override
    public T getSum() {
        return getCounters().reduce(zero(), sumOp());
    }

    /**
     * Returns the negative infinite value in the type of this counter.
     */
    protected abstract T negativeInfinite();

    /**
     * Returns the positive infinite value in the type of this counter.
     */
    protected abstract T positiveInfinite();

    /**
     * Returns the zero value in the type of this counter.
     */
    protected abstract T zero();

    /**
     * Computes the summation of two values.
     */
    protected abstract BinaryOperator<T> sumOp();

    /**
     * Parses the given string to the value of this counter.
     */
    protected abstract T valueFromString(final String string);

    /**
     * Returns a string that represents the given value.
     */
    protected abstract String valueToString(final T value);

    private Stream<T> getCounters() {
        return ZookeeperUtils.getChildren(zk, counterPath).parallelStream().map(
                path -> valueFromString(ZookeeperUtils.getNodeData(zk, String.format("%s/%s", counterPath, path))));
    }
}
