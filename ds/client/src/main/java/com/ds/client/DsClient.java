package com.ds.client;

import static com.ds.client.NetUtil.host;
import static com.ds.client.NetUtil.port;

import ds.BlockPlan;
import ds.CommitBlock;
import ds.CommitReq;
import ds.FilePath;
import ds.GetHdr;
import ds.GetMeta;
import ds.LocateResp;
import ds.MetadataServiceGrpc;
import ds.PlanPutReq;
import ds.PlanPutResp;
import ds.StorageServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

public class DsClient {
  private static final int DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024;
  private static final int CHUNK_SIZE = Math.max(1, Integer.getInteger("ds.chunk.size", DEFAULT_CHUNK_SIZE));
  private static final int QUORUM_W = Math.max(1, Integer.getInteger("ds.quorum.w", 2));
  private static final int QUORUM_R = Math.max(1, Integer.getInteger("ds.quorum.r", 2));
  private static final long WRITE_TIMEOUT_MS = Math.max(1L, Long.getLong("ds.quorum.timeout.ms", 20_000L));
  private static final long READ_TIMEOUT_MS = Math.max(1L, Long.getLong("ds.quorum.timeout.read.ms", 15_000L));

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
        long fileSize = Files.size(local);
        ManagedChannel mc =
            ManagedChannelBuilder.forAddress("localhost", 7000).usePlaintext().build();
        MetadataServiceGrpc.MetadataServiceBlockingStub meta =
            MetadataServiceGrpc.newBlockingStub(mc);

        PlanPutResp plan =
            meta.planPut(
                PlanPutReq.newBuilder()
                    .setPath(remote)
                    .setSize(fileSize)
                    .setChunkSize(CHUNK_SIZE)
                    .build());
        byte[] buf = new byte[CHUNK_SIZE];
        List<Integer> actualSizes = new ArrayList<>();
        HashMap<String, List<String>> successfulReplicas = new HashMap<>();
        try (var in = Files.newInputStream(local)) {
          for (int i = 0; i < plan.getBlocksCount(); i++) {
            BlockPlan bp = plan.getBlocks(i);
            Path tmpChunk = Files.createTempFile("chunk", ".bin");
            int read = in.readNBytes(buf, 0, buf.length);
            if (read < 0) {
              read = 0;
            }
            Files.write(tmpChunk, java.util.Arrays.copyOf(buf, read));
            actualSizes.add(read);

            var replicas = bp.getReplicaUrlsList();
            List<String> ids = new ArrayList<>(replicas);
            List<java.util.function.Supplier<Boolean>> tasks = new ArrayList<>();
            for (String hp : replicas) {
              tasks.add(
                  () -> {
                    try {
                      StreamTool.put(host(hp), port(hp), bp.getBlockId(), tmpChunk);
                      return Boolean.TRUE;
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                  });
            }
            Quorum q =
                new Quorum(replicas.size(), Duration.ofMillis(WRITE_TIMEOUT_MS), QUORUM_W);
            try {
              var res = q.runRace(ids, tasks);
              successfulReplicas.put(
                  bp.getBlockId(),
                  new ArrayList<>(res.successes()));
              System.out.println(
                  "[PUT quorum] block="
                      + bp.getBlockId()
                      + " ok="
                      + res.successes()
                      + " fails="
                      + res.failures().size());
            } catch (TimeoutException te) {
              throw new RuntimeException(
                  "Failed to meet W="
                      + QUORUM_W
                      + " for block "
                      + bp.getBlockId()
                      + ": "
                      + te.getMessage());
            } catch (Exception e) {
              throw new RuntimeException("Quorum error for block " + bp.getBlockId(), e);
            } finally {
              q.shutdown();
              Files.deleteIfExists(tmpChunk);
            }
          }
        }
        CommitReq.Builder commitBuilder =
            CommitReq.newBuilder().setPath(remote).setSize(fileSize);
        for (int i = 0; i < plan.getBlocksCount(); i++) {
          BlockPlan bp = plan.getBlocks(i);
          int sz = i < actualSizes.size() ? actualSizes.get(i) : (int) bp.getSize();
          List<String> replicas = successfulReplicas.get(bp.getBlockId());
          if (replicas == null || replicas.isEmpty()) {
            replicas = bp.getReplicaUrlsList();
          }
          commitBuilder.addBlocks(
              CommitBlock.newBuilder()
                  .setBlockId(bp.getBlockId())
                  .setSize(sz)
                  .addAllReplicas(replicas)
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
            var replicas = bp.getReplicaUrlsList();
            Map<String, GetMeta> metaByReplica = new ConcurrentHashMap<>();
            List<String> ids = new ArrayList<>(replicas);
            List<java.util.function.Supplier<GetMeta>> tasks = new ArrayList<>();
            for (String hp : replicas) {
              tasks.add(
                  () -> {
                    ManagedChannel ch =
                        ManagedChannelBuilder.forAddress(host(hp), port(hp)).usePlaintext().build();
                    try {
                      StorageServiceGrpc.StorageServiceBlockingStub stub =
                          StorageServiceGrpc.newBlockingStub(ch);
                      var it =
                          stub.getBlock(
                              GetHdr.newBuilder().setBlockId(bp.getBlockId()).setMinR(1).build());
                      if (!it.hasNext()) {
                        throw new IllegalStateException("No response from " + hp);
                      }
                      var first = it.next();
                      if (!first.hasMeta()) {
                        throw new IllegalStateException("Missing meta from " + hp);
                      }
                      GetMeta metaResp = first.getMeta();
                      metaByReplica.put(hp, metaResp);
                      return metaResp;
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    } finally {
                      ch.shutdownNow();
                    }
                  });
            }
            Quorum qR =
                new Quorum(replicas.size(), Duration.ofMillis(READ_TIMEOUT_MS), QUORUM_R);
            Quorum.Result<GetMeta> r2;
            try {
              r2 = qR.runRace(ids, tasks);
              System.out.println(
                  "[GET quorum] block="
                      + bp.getBlockId()
                      + " ok="
                      + r2.successes()
                      + " fails="
                      + r2.failures().size());
            } catch (Exception e) {
              throw new RuntimeException(
                  "Failed to meet R="
                      + QUORUM_R
                      + " for block "
                      + bp.getBlockId()
                      + ": "
                      + e.getMessage());
            } finally {
              qR.shutdown();
            }

            String chosenHp = null;
            int bestClock = -1;
            for (String hp : r2.successes()) {
              GetMeta metaResp = metaByReplica.get(hp);
              if (metaResp == null) {
                continue;
              }
              int vcLen = metaResp.getVectorClock().length();
              if (vcLen > bestClock) {
                bestClock = vcLen;
                chosenHp = hp;
              }
            }
            if (chosenHp == null) {
              if (!replicas.isEmpty()) {
                chosenHp = replicas.get(0);
              } else {
                throw new RuntimeException(
                    "No replicas available to read block " + bp.getBlockId());
              }
            }

            Path tmp = Files.createTempFile("getchunk", ".bin");
            try {
              StreamTool.get(host(chosenHp), port(chosenHp), bp.getBlockId(), tmp);
              Files.copy(tmp, out);
            } finally {
              Files.deleteIfExists(tmp);
            }
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
