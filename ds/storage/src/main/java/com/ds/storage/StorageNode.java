package com.ds.storage;

import com.ds.storage.grpc.StorageServiceImpl;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import java.io.IOException;

public class StorageNode {
  public static void main(String[] args) throws IOException, InterruptedException {
    int port = 8001;
    String dataDir = "./data/node1";
    for (int i = 0; i + 1 < args.length; i++) {
      if ("--port".equals(args[i])) {
        port = Integer.parseInt(args[i + 1]);
      }
      if ("--data".equals(args[i])) {
        dataDir = args[i + 1];
      }
    }

    Server server = NettyServerBuilder.forPort(port)
        .addService(new StorageServiceImpl())
        .addService(ProtoReflectionService.newInstance())
        .build()
        .start();

    System.out.println(
        "[StorageNode] gRPC StorageService started on :" + port + " (Stage 1), data=" + dataDir);

    Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));

    server.awaitTermination();
  }
}
