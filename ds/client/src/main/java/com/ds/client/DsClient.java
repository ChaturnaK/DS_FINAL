package com.ds.client;

import ds.BlockPlan;
import ds.CommitBlock;
import ds.CommitReq;
import ds.FilePath;
import ds.LocateResp;
import ds.MetadataServiceGrpc;
import ds.PlanPutReq;
import ds.PlanPutResp;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class DsClient {
  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      printUsage();
      return;
    }
    switch (args[0]) {
      case "putBlock" -> {
        if (args.length < 4) {
          System.out.println("putBlock requires host:port blockId file");
          return;
        }
        String[] hp = args[1].split(":");
        StreamTool.put(hp[0], Integer.parseInt(hp[1]), args[2], Paths.get(args[3]));
      }
      case "getBlock" -> {
        if (args.length < 4) {
          System.out.println("getBlock requires host:port blockId out");
          return;
        }
        String[] hp = args[1].split(":");
        StreamTool.get(hp[0], Integer.parseInt(hp[1]), args[2], Paths.get(args[3]));
      }
      case "put" -> {
        if (args.length < 3) {
          System.out.println("Usage: put <localFile> <remotePath>");
          return;
        }
        Path local = Paths.get(args[1]);
        String remote = args[2];
        ManagedChannel mc =
            ManagedChannelBuilder.forAddress("localhost", 7000).usePlaintext().build();
        MetadataServiceGrpc.MetadataServiceBlockingStub meta =
            MetadataServiceGrpc.newBlockingStub(mc);

        PlanPutResp plan =
            meta.planPut(
                PlanPutReq.newBuilder()
                    .setPath(remote)
                    .setSize(Files.size(local))
                    .setChunkSize(8 * 1024 * 1024)
                    .build());
        byte[] buf = new byte[8 * 1024 * 1024];
        java.util.List<Integer> actualSizes = new java.util.ArrayList<>();
        try (var in = Files.newInputStream(local)) {
          for (BlockPlan bp : plan.getBlocksList()) {
            java.util.List<String> replicas = bp.getReplicaUrlsList();
            String[] hp = replicas.get(0).split(":");
            Path tmpChunk = Files.createTempFile("chunk", ".bin");
            int r = in.readNBytes(buf, 0, buf.length);
            if (r > 0) {
              Files.write(tmpChunk, java.util.Arrays.copyOf(buf, r));
            } else {
              Files.write(tmpChunk, new byte[0]);
            }
            StreamTool.put(hp[0], Integer.parseInt(hp[1]), bp.getBlockId(), tmpChunk);
            Files.deleteIfExists(tmpChunk);
            actualSizes.add(r);
          }
        }
        CommitReq.Builder commitBuilder =
            CommitReq.newBuilder().setPath(remote).setSize(Files.size(local));
        for (int i = 0; i < plan.getBlocksCount(); i++) {
          BlockPlan bp = plan.getBlocks(i);
          int sz = i < actualSizes.size() ? actualSizes.get(i) : (int) bp.getSize();
          commitBuilder.addBlocks(
              CommitBlock.newBuilder()
                  .setBlockId(bp.getBlockId())
                  .setSize(sz)
                  .addAllReplicas(bp.getReplicaUrlsList())
                  .build());
        }
        meta.commit(commitBuilder.build());
        mc.shutdownNow();
        System.out.println("Upload complete for " + remote);
      }
      case "get" -> {
        if (args.length < 3) {
          System.out.println("Usage: get <remotePath> <localFile>");
          return;
        }
        String remote = args[1];
        Path localOut = Paths.get(args[2]);
        ManagedChannel mc =
            ManagedChannelBuilder.forAddress("localhost", 7000).usePlaintext().build();
        MetadataServiceGrpc.MetadataServiceBlockingStub meta =
            MetadataServiceGrpc.newBlockingStub(mc);
        LocateResp loc = meta.locate(FilePath.newBuilder().setPath(remote).build());
        try (var out = Files.newOutputStream(localOut)) {
          for (BlockPlan bp : loc.getBlocksList()) {
            String[] hp = bp.getReplicaUrls(0).split(":");
            Path tmp = Files.createTempFile("getchunk", ".bin");
            StreamTool.get(hp[0], Integer.parseInt(hp[1]), bp.getBlockId(), tmp);
            Files.copy(tmp, out);
            Files.delete(tmp);
          }
        }
        mc.shutdownNow();
        System.out.println("Download complete → " + localOut);
      }
      default -> System.out.println("Unknown command");
    }
  }

  private static void printUsage() {
    System.out.println("Usage:");
    System.out.println("  put <localFile> <remotePath>");
    System.out.println("  get <remotePath> <localFile>");
    System.out.println("  putBlock <host:port> <blockId> <file>");
    System.out.println("  getBlock <host:port> <blockId> <out>");
  }
}
