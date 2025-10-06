package com.ds.metadata;

import com.ds.metadata.grpc.MetadataServiceImpl;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataServer {
  private static final Logger log = LoggerFactory.getLogger(MetadataServer.class);

  public static void main(String[] args) throws Exception {
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
          // ignore
      }
    }

    String host = resolveLocalHost();
    String serverId = host + ":" + port + ":" + UUID.randomUUID();

    try (ZkCoordinator coordinator = new ZkCoordinator(zkConnect, serverId)) {
      MetaStore metaStore = new MetaStore(coordinator.client());
      metaStore.ensureRoots();
      PlacementService placementService = new PlacementService(coordinator.client(), replication);

      MetadataServiceImpl service =
          new MetadataServiceImpl(coordinator, metaStore, placementService);

      Server server =
          NettyServerBuilder.forPort(port)
              .addService(service)
              .addService(ProtoReflectionService.newInstance())
              .build()
              .start();

      log.info("gRPC MetadataService started on :{} (Stage 2)", port);
      log.info("ZooKeeper connection: {} | serverId={} | replication={} ", zkConnect, serverId, replication);

      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    log.info("Shutdown signal received, stopping metadata server...");
                    server.shutdown();
                  }));

      server.awaitTermination();
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
