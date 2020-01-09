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
package flexgraph.utils;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.ArrayList;
import java.util.List;

/**
 * A utility class that communicates with Zookeeper instance.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class ZookeeperUtils {
    public static void createPersistentNode(final ZooKeeper zk, final String path) {
        createNode(zk, path, CreateMode.PERSISTENT);
    }

    public static void createOrUpdatePersistentNode(final ZooKeeper zk, final String path, final String value) {
        final byte[] valueBytes = value.getBytes();
        if (exists(zk, path)) {
            try {
                zk.setData(path, valueBytes, -1);
            } catch (KeeperException | InterruptedException e) {
                // ignore
            }
        } else {
            createNode(zk, path, valueBytes, CreateMode.PERSISTENT);
        }
    }

    public static String getNodeData(final ZooKeeper zk, final String path) {
        try {
            return new String(zk.getData(path, false, null));
        } catch (KeeperException | InterruptedException e) {
            return "";
        }
    }

    public static void createEphemeralNode(final ZooKeeper zk, final String path) {
        createNode(zk, path, CreateMode.EPHEMERAL);
    }

    private static void createNode(final ZooKeeper zk, final String path, final CreateMode createMode) {
        createNode(zk, path, new byte[0], createMode);
    }

    private static void createNode(
            final ZooKeeper zk, final String path, final byte[] values, final CreateMode createMode) {
        try {
            zk.create(path, values, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
        } catch (KeeperException e) {
            switch (e.code()) {
                case NODEEXISTS:
                    break;
                default:
                    break;
            }
        } catch (InterruptedException e) {
            // ignore
        }
    }

    public static boolean exists(final ZooKeeper zk, final String path) {
        try {
            return zk.exists(path, false) != null;
        } catch (KeeperException | InterruptedException e) {
            return false;
        }
    }

    public static List<String> getChildren(final ZooKeeper zk, final String path) {
        try {
            return zk.getChildren(path, false);
        } catch (KeeperException | InterruptedException e) {
            return new ArrayList<>();
        }
    }

    public static void delete(final ZooKeeper zk, final String path) {
        try {
            zk.delete(path, -1);
        } catch (KeeperException | InterruptedException ignored) {
        }
    }
}
