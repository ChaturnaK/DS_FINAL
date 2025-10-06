package com.ds.metadata;

import com.ds.metadata.grpc.MetadataServiceImpl;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import java.io.IOException;

public class MetadataServer {
  public static void main(String[] args) throws IOException, InterruptedException {
    int port = 7000;
    for (int i = 0; i + 1 < args.length; i++) {
      if ("--port".equals(args[i])) {
        port = Integer.parseInt(args[i + 1]);
      }
    }

    Server server = NettyServerBuilder.forPort(port)
        .addService(new MetadataServiceImpl())
        .addService(ProtoReflectionService.newInstance())
        .build()
        .start();

    System.out.println("[MetadataServer] gRPC MetadataService started on :" + port + " (Stage 1)");

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      server.shutdown();
    }));

    server.awaitTermination();
  }
}
