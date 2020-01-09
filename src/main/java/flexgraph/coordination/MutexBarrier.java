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

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import flexgraph.Constants;
import flexgraph.utils.ZookeeperUtils;

/**
 * Implements Mutex (Mutual Exclusion) barrier based on Zookeeper.
 *
 * @see flexgraph.coordination.Barrier
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class MutexBarrier implements Watcher, Barrier {
    private static final Logger LOG = LoggerFactory.getLogger(MutexBarrier.class);
    private final ZooKeeper zk;
    private final String barrierPath;
    private boolean isMine;
    private boolean finished;

    public MutexBarrier(
            final ZooKeeper zk, final String name, final int mutexId, final boolean isMine) {
        this.zk = zk;
        this.isMine = isMine;
        this.barrierPath = String.format("%s/%s-%d", Constants.BarrierRoot, name, mutexId);
    }

    @Override
    public final void setup() {
        finished = ZookeeperUtils.exists(zk, barrierPath);
        registerWatcher();
    }

    @Override
    public final void markCompleted() {
        finished = true;
        ZookeeperUtils.createPersistentNode(zk, barrierPath);
        LOG.info("Mark barrier {}", barrierPath);
    }

    @Override
    public synchronized final void waitBarrier() {
        LOG.info("Wait barrier {}", barrierPath);
        while (!finished) {
            try {
                wait(Constants.CoordinationTickTime);
                finished = ZookeeperUtils.exists(zk, barrierPath);
            } catch (InterruptedException e) {
                // ignore
            }
            registerWatcher();
        }

        finished = false;
    }

    @Override
    public void cleanup() {
        if (isMine) {
            finished = false;
            ZookeeperUtils.delete(zk, barrierPath);
            LOG.info("Barrier cleanup: {}", barrierPath);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeCreated) {
            finished = ZookeeperUtils.exists(zk, barrierPath);
            notifyFromWatcher();
        }

        registerWatcher();
    }

    private synchronized void notifyFromWatcher() {
        notifyAll();
    }

    private void registerWatcher() {
        try {
            zk.exists(barrierPath, this);
        } catch (KeeperException | InterruptedException e) {
            // ignore
        }
    }

    @Override
    public String toString() {
        return String.format("MutexBarrier(barrierPath='%s')", barrierPath);
    }
}
