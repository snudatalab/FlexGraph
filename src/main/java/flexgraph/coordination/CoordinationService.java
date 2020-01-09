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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.net.DNS;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import flexgraph.Constants;
import flexgraph.utils.ZookeeperUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Coordinates all workers via Zookeeper.
 *
 * @author Chiwan Park, Ha-Myung Park, and U Kang
 */
public class CoordinationService implements Watcher {
    private final Logger LOG = LoggerFactory.getLogger(CoordinationService.class);

    private int workerId;
    private int parallelism;

    private String zkAddr;

    // master-related variables
    private ZooKeeperServer zkServer;

    // client-related variables
    private ZooKeeper zk;
    private boolean barrierInitialized = false;
    private boolean counterInitialized = false;

    // Hadoop-related variables
    private TaskAttemptContext context;

    /**
     * Initializes this coordination service.
     *
     * This methods connects to master node via Zookeeper, and registers this worker.
     *
     * @param context Hadoop context object
     */
    public final void setup(final TaskAttemptContext context)
            throws IOException, InterruptedException, KeeperException {
        this.context = context;
        final Configuration conf = context.getConfiguration();

        workerId = context.getTaskAttemptID().getTaskID().getId();
        parallelism = conf.getInt(Constants.Parallelism, -1);
        if (parallelism == -1) {
            parallelism = context.getNumReduceTasks();
        }

        if (workerId == 0) {
            // initialize zookeeper server
            final File zkDir = new File(
                    System.getProperty("java.io.tmpdir"),
                    String.format("zkData-%s", context.getTaskAttemptID().getJobID())).getAbsoluteFile();
            final InetSocketAddress addr = new InetSocketAddress(DNS.getDefaultHost("default").toLowerCase(), 0);
            final ServerCnxnFactory factory = ServerCnxnFactory.createFactory(addr, Constants.MaxNumWorkers);

            zkServer = new ZooKeeperServer(zkDir, zkDir, Constants.CoordinationTickTime);
            zkServer.setMinSessionTimeout(Constants.CoordinationTickTime * 2);
            zkServer.setMaxSessionTimeout(Constants.CoordinationTickTime * 50);
            zkAddr = String.format("%s:%d", addr.getHostName(), factory.getLocalPort());

            factory.startup(zkServer);

            // write zookeeper address to DFS
            final Path zkAddrFilePath = getZkAddrPath(context);
            final FileSystem fs = zkAddrFilePath.getFileSystem(conf);
            try (final DataOutputStream out = fs.create(zkAddrFilePath)) {
                Text.writeString(out, getServiceAddress());
            }

            connectToZkServer();
        } else {
            readZookeeperAddress(context);
            connectToZkServer();
        }
        waitInitialization();
    }

    private void connectToZkServer() throws IOException {
        zk = new ZooKeeper(zkAddr, Constants.CoordinationTickTime * 5, this);
    }

    private synchronized void waitInitialization() throws KeeperException, InterruptedException {
        while (!barrierInitialized || !counterInitialized) {
            wait();
        }
    }

    /**
     * Finalizes this coordination service.
     *
     * This method unregisters this worker, and disconnects the connection between master node.
     *
     * @param context Hadoop context object
     */
    public final void cleanup(final TaskAttemptContext context) throws InterruptedException {
        zk.close();
        if (workerId == 0) {
            int connections = zkServer.getNumAliveConnections();
            while (connections > 0) {
                LOG.info("Wait for connected clients: {}", connections);
                Thread.sleep(Constants.CoordinationTickTime);
                connections = zkServer.getNumAliveConnections();
            }
            zkServer.getServerCnxnFactory().shutdown();
        }
    }

    /**
     * Returns the id of this worker.
     *
     * @return worker id
     */
    public final int getWorkerId() {
        return workerId;
    }

    /**
     * Returns the address of master Zookeeper instance.
     *
     * @return ip address of master instance with port
     */
    public final String getServiceAddress() {
        return zkAddr;
    }

    private Path getZkAddrPath(final TaskAttemptContext context) throws IOException {
        return new Path(Constants.DFSTemporaryPath, String.format("%s-zkServer", context.getJobID().toString()));
    }

    private void readZookeeperAddress(final TaskAttemptContext context) throws IOException, InterruptedException {
        final Path zkPath = getZkAddrPath(context);
        final FileSystem fs = zkPath.getFileSystem(context.getConfiguration());

        while (!fs.exists(zkPath) || fs.getFileStatus(zkPath).getLen() <= 0) {
            Thread.sleep(Constants.CoordinationTickTime);
        }

        try (final DataInputStream is = fs.open(zkPath)) {
            zkAddr = Text.readString(is);
        }
        LOG.info("Zookeeper instance detected: {}", zkAddr);
    }

    private void registerWatcher() {
        try {
            zk.exists(Constants.BarrierRoot, this);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Receives a Zookeeper event, and updates internal data structure.
     *
     * @param event the Zookeeper event
     */
    @Override
    public void process(final WatchedEvent event) {
        if (event.getType() == Event.EventType.None) {
            switch (event.getState()) {
                case SyncConnected:
                    if (!barrierInitialized || !counterInitialized) {
                        barrierInitialized |= ZookeeperUtils.exists(zk, Constants.BarrierRoot);
                        counterInitialized |= ZookeeperUtils.exists(zk, Constants.CounterRoot);
                        createRootStructure();
                        barrierInitialized |= ZookeeperUtils.exists(zk, Constants.BarrierRoot);
                        counterInitialized |= ZookeeperUtils.exists(zk, Constants.CounterRoot);
                        registerWatcher();
                    }
                    notifyFromWatcher();
                    break;
                case Disconnected:
                case Expired:
                    try {
                        connectToZkServer();
                        registerWatcher();
                    } catch (IOException e) {
                        e.printStackTrace(); // FIXME!
                    }
                    break;
                default:
                    break;
            }
        }
    }

    private void createRootStructure() {
        // barrier
        if (!barrierInitialized) {
            ZookeeperUtils.createPersistentNode(zk, Constants.BarrierRoot);
        }
        // counter
        if (!counterInitialized) {
            ZookeeperUtils.createPersistentNode(zk, Constants.CounterRoot);
        }
    }

    /**
     * Creates a semaphore barrier with given name.
     *
     * @param name the name of barrier
     * @return a semaphore barrier
     * @see flexgraph.coordination.SemaphoreBarrier
     */
    public Barrier getSemaphoreBarrier(final String name) {
        final SemaphoreBarrier barrier = new SemaphoreBarrier(zk, name, workerId, parallelism, workerId == 0);
        barrier.setup();
        return barrier;
    }

    /**
     * Creates a mutex barrier with given name.
     *
     * @param name the name of barrier
     * @param mutexId the unique mutex id (in general, owner worker id is used)
     * @param isMine whether this barrier is mine or owned by other worker
     * @return a mutex barrier
     * @see flexgraph.coordination.MutexBarrier
     */
    public Barrier getMutexBarrier(final String name, final int mutexId, final boolean isMine) {
        final MutexBarrier barrier = new MutexBarrier(zk, name, mutexId, isMine);
        barrier.setup();
        return barrier;
    }

    /**
     * Creates a counter based on Zookeeper.
     *
     * @param name the name of counter
     * @param clazz a class object that represents stored value
     * @return a counter object based on Zookeeper
     * @see flexgraph.coordination.Counter
     * @see flexgraph.coordination.ZookeeperCounter
     */
    @SuppressWarnings("unchecked")
    public <T extends Comparable<T>> Counter<T> getZkCounter(final String name, final Class<T> clazz) {
        if (clazz == Long.class) {
            return (Counter<T>) new ZookeeperLongCounter(zk, name, workerId);
        } else if (clazz == Double.class) {
            return (Counter<T>) new ZookeeperDoubleCounter(zk, name, workerId);
        } else if (clazz == Integer.class) {
            return (Counter<T>) new ZookeeperIntegerCounter(zk, name, workerId);
        } else if (clazz == Boolean.class) {
            return (Counter<T>) new ZookeeperBooleanCounter(zk, name, workerId);
        }
        return null;
    }

    /**
     * Creates a counter based on Hadoop-native counter.
     *
     * Note that this counter supports only long-typed value, and the value in this counter cannot be
     * retrieved during computation. The value can be accessed in client code after completion of program.
     *
     * @param name the name of counter
     * @return a counter object based on Hadoop-native counter
     * @see flexgraph.coordination.Counter
     * @see flexgraph.coordination.HadoopCounter
     */
    public Counter<Long> getHadoopCounter(final String name) {
        return new HadoopCounter(context, name);
    }

    private synchronized void notifyFromWatcher() {
        notifyAll();
    }
}
