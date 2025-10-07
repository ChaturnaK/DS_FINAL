package com.ds.metadata;

import com.ds.metadata.grpc.MetadataServiceImpl;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import org.apache.curator.utils.EnsurePath;

public class MetadataServer {
  public static void main(String[] args) {
    int port = 7000;
    String zkConnect = "localhost:2181";
    int replication = 3;

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--port":
          if (i + 1 < args.length) {
            port = Integer.parseInt(args[++i]);
          }
          break;
        case "--zk":
          if (i + 1 < args.length) {
            zkConnect = args[++i];
          }
          break;
        case "--replication":
          if (i + 1 < args.length) {
            replication = Integer.parseInt(args[++i]);
          }
          break;
        default:
          // ignore unknown args
      }
    }

    try {
      String host = resolveLocalHost();
      String serverId = host + ":" + port + ":" + UUID.randomUUID();

      try (ZkCoordinator coordinator = new ZkCoordinator(zkConnect, serverId)) {
        EnsurePath filesPath = new EnsurePath(MetaStore.FILES);
        EnsurePath blocksPath = new EnsurePath(MetaStore.BLOCKS);
        filesPath.ensure(coordinator.client().getZookeeperClient());
        blocksPath.ensure(coordinator.client().getZookeeperClient());

        MetaStore metaStore = new MetaStore(coordinator.client());
        PlacementService placementService = new PlacementService(coordinator.client(), replication);
        MetadataServiceImpl service = new MetadataServiceImpl(metaStore, placementService, coordinator);

        Server server =
            NettyServerBuilder.forPort(port)
                .addService(service)
                .addService(ProtoReflectionService.newInstance())
                .build()
                .start();

        System.out.println("[MetadataServer] gRPC started on :" + port + " (Stage 2)");

        Runtime.getRuntime()
            .addShutdownHook(
                new Thread(
                    () -> {
                      System.out.println("[MetadataServer] Shutdown signal received");
                      server.shutdown();
                    }));

        server.awaitTermination();
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static String resolveLocalHost() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      return "unknown-host";
    }
  }
}
