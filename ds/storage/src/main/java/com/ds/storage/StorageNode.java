package com.ds.storage;

import com.ds.common.JsonSerde;
import com.ds.storage.grpc.StorageServiceImpl;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageNode {
  private static final Logger log = LoggerFactory.getLogger(StorageNode.class);
  private static final String STORAGE_NODES_PATH = "/ds/nodes/storage";

  public static void main(String[] args) throws Exception {
    int port = 8001;
    String dataDir = "./data/node1";
    String zkConnect = "localhost:2181";
    String zone = "z1";

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--port":
          if (i + 1 < args.length) {
            port = Integer.parseInt(args[++i]);
          }
          break;
        case "--data":
          if (i + 1 < args.length) {
            dataDir = args[++i];
          }
          break;
        case "--zk":
          if (i + 1 < args.length) {
            zkConnect = args[++i];
          }
          break;
        case "--zone":
          if (i + 1 < args.length) {
            zone = args[++i];
          }
          break;
        default:
          // ignore
      }
    }

    Path dataPath = Paths.get(dataDir);
    Files.createDirectories(dataPath);
    long freeBytes = Files.getFileStore(dataPath).getUsableSpace();

    String host = java.net.InetAddress.getLocalHost().getHostName();
    String nodeId = host + ":" + port;

    CuratorFramework curator =
        CuratorFrameworkFactory.newClient(zkConnect, new ExponentialBackoffRetry(200, 10));
    curator.start();
    curator.blockUntilConnected();

    registerNode(curator, nodeId, host, port, zone, freeBytes);

    Server server =
        NettyServerBuilder.forPort(port)
            .addService(new StorageServiceImpl())
            .addService(ProtoReflectionService.newInstance())
            .build()
            .start();

    log.info(
        "gRPC StorageService started on :{} (Stage 2), data={}, zone={}, zk={}",
        port,
        dataDir,
        zone,
        zkConnect);

    AtomicBoolean shuttingDown = new AtomicBoolean(false);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  if (shuttingDown.compareAndSet(false, true)) {
                    log.info("Shutdown signal received, stopping storage node {}", nodeId);
                    server.shutdown();
                    curator.close();
                  }
                }));

    server.awaitTermination();
    if (shuttingDown.compareAndSet(false, true)) {
      curator.close();
    }
  }

  private static void registerNode(
      CuratorFramework curator, String nodeId, String host, int port, String zone, long freeBytes)
      throws Exception {
    NodePayload payload = new NodePayload(host, port, zone, freeBytes);
    byte[] data = JsonSerde.write(payload);
    String path = STORAGE_NODES_PATH + "/" + nodeId;
    try {
      curator
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.EPHEMERAL)
          .forPath(path, data);
    } catch (KeeperException.NodeExistsException e) {
      curator.delete().forPath(path);
      curator
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.EPHEMERAL)
          .forPath(path, data);
    }
  }

  private record NodePayload(String host, int port, String zone, long freeBytes) {}
}
