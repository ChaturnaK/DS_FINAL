package com.ds.metadata;

import com.ds.common.Metrics;
import com.ds.metadata.grpc.MetadataServiceImpl;
import com.ds.time.NtpSync;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.micrometer.core.instrument.Counter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
        HealingPlanner planner =
            new HealingPlanner(coordinator.client(), placementService, metaStore, replication);
        ScheduledExecutorService healExec = Executors.newSingleThreadScheduledExecutor();
        healExec.scheduleAtFixedRate(
            () -> {
              if (coordinator.isLeader()) {
                planner.run();
              }
            },
            5,
            10,
            TimeUnit.SECONDS);

        ScheduledExecutorService metricsExec = Executors.newScheduledThreadPool(1);
        metricsExec.scheduleAtFixedRate(
            new NtpSync("pool.ntp.org", 123), 1, 600, TimeUnit.SECONDS);
        metricsExec.scheduleAtFixedRate(Metrics::dumpCsv, 5, 5, TimeUnit.SECONDS);

        Counter leaderChanges = Metrics.counter("leader_changes");
        coordinator.addLeadershipListener(
            isLeader -> {
              leaderChanges.increment();
            });
        if (coordinator.isLeader()) {
          leaderChanges.increment();
        }

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
                      healExec.shutdownNow();
                      metricsExec.shutdownNow();
                      server.shutdown();
                    }));

        server.awaitTermination();
        healExec.shutdownNow();
        metricsExec.shutdownNow();
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
