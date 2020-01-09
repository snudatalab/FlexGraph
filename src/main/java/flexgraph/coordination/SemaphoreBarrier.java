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

import java.util.List;

/**
 * Implements a semaphore barrier based on Zookeeper.
 *
 * @see flexgraph.coordination.Barrier
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class SemaphoreBarrier implements Barrier, Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(SemaphoreBarrier.class);
    private final int workerId;
    private final ZooKeeper zk;
    private final String barrierRoot;
    private boolean initialized;
    private boolean isMine;
    private boolean[] finished;

    public SemaphoreBarrier(
            final ZooKeeper zk, final String name, final int workerId, final int parallelism, final boolean isMine) {
        this.zk = zk;
        this.workerId = workerId;
        this.barrierRoot = String.format("%s/%s", Constants.BarrierRoot, name);
        this.isMine = isMine;
        finished = new boolean[parallelism];
    }

    private void registerWatcher() {
        try {
            updateState();
            zk.getChildren(barrierRoot, this);
        } catch (KeeperException | InterruptedException e) {
            // ignore
        }
    }

    private void updateState() {
        if (initialized) {
            final List<String> children = ZookeeperUtils.getChildren(zk, barrierRoot);
            for (String child : children) {
                finished[Integer.parseInt(child)] = true;
            }
        }
    }

    @Override
    public void setup() {
        // initialize barrier structure
        initialized = ZookeeperUtils.exists(zk, barrierRoot);
        if (!initialized) {
            ZookeeperUtils.createPersistentNode(zk, barrierRoot);
            initialized = ZookeeperUtils.exists(zk, barrierRoot);
        }

        updateState();
        registerWatcher();
    }

    @Override
    public void markCompleted() {
        ZookeeperUtils.createPersistentNode(zk, String.format("%s/%d", barrierRoot, workerId));
        registerWatcher();
    }

    @Override
    public synchronized void waitBarrier() {
        LOG.info("Wait barrier {}", barrierRoot);
        boolean completed = false;
        while (!completed) {
            completed = true;
            for (boolean fin : finished) {
                if (!fin) {
                    completed = false;
                    break;
                }
            }

            if (!completed) {
                try {
                    wait(Constants.CoordinationTickTime);
                } catch (InterruptedException e) {
                    // ignore
                }
                registerWatcher();
            }
        }

        initialized = false;
        for (int i = 0; i < finished.length; ++i) {
            finished[i] = false;
        }
    }

    @Override
    public void cleanup() {
        if (isMine) {
            for (int i = 0; i < finished.length; ++i) {
                ZookeeperUtils.delete(zk, String.format("%s/%d", barrierRoot, i));
            }
        }
    }

    private synchronized void notifyFromWatcher() {
        notifyAll();
    }

    @Override
    public void process(final WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeChildrenChanged) {
            updateState();
            notifyFromWatcher();
        }

        registerWatcher();
    }

    @Override
    public String toString() {
        return String.format("SemaphoreBarrier(path = %s/%d)", barrierRoot, workerId);
    }
}
