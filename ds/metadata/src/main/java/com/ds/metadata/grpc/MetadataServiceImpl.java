package com.ds.metadata.grpc;

import com.ds.metadata.MetaStore;
import com.ds.metadata.MetaStore.BlockEntry;
import com.ds.metadata.MetaStore.FileEntry;
import com.ds.metadata.PlacementService;
import com.ds.metadata.PlacementService.NodeInfo;
import com.ds.metadata.ZkCoordinator;
import ds.Ack;
import ds.BlockPlan;
import ds.CommitBlock;
import ds.CommitReq;
import ds.FilePath;
import ds.LocateResp;
import ds.MetadataServiceGrpc;
import ds.PlanPutReq;
import ds.PlanPutResp;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class MetadataServiceImpl extends MetadataServiceGrpc.MetadataServiceImplBase {
  private static final long DEFAULT_CHUNK_SIZE = 8L * 1024 * 1024;

  private final ZkCoordinator coordinator;
  private final MetaStore metaStore;
  private final PlacementService placementService;

  public MetadataServiceImpl(
      ZkCoordinator coordinator, MetaStore metaStore, PlacementService placementService) {
    this.coordinator = coordinator;
    this.metaStore = metaStore;
    this.placementService = placementService;
  }

  @Override
  public void planPut(PlanPutReq request, StreamObserver<PlanPutResp> responseObserver) {
    if (!coordinator.isLeader()) {
      notLeader(responseObserver);
      return;
    }
    try {
      long fileSize = Math.max(0, request.getSize());
      long chunkSize = request.getChunkSize() > 0 ? request.getChunkSize() : DEFAULT_CHUNK_SIZE;
      int blockCount = (fileSize == 0) ? 1 : (int) ((fileSize + chunkSize - 1) / chunkSize);
      blockCount = Math.max(1, blockCount);

      long remaining = fileSize;
      PlanPutResp.Builder resp = PlanPutResp.newBuilder();
      for (int i = 0; i < blockCount; i++) {
        String blockId = generateBlockId(request.getPath(), i);
        List<NodeInfo> replicas = placementService.chooseReplicas(blockId);

        long blockSize;
        if (blockCount == 1) {
          blockSize = fileSize;
        } else if (i == blockCount - 1) {
          blockSize = Math.max(remaining, 0);
        } else {
          blockSize = Math.min(chunkSize, Math.max(remaining, chunkSize));
        }
        remaining = Math.max(0, remaining - blockSize);

        BlockPlan.Builder blockPlan = BlockPlan.newBuilder().setBlockId(blockId).setSize(blockSize);
        for (NodeInfo ni : replicas) {
          blockPlan.addReplicaUrls(ni.host + ":" + ni.port);
        }
        resp.addBlocks(blockPlan);
      }

      responseObserver.onNext(resp.build());
      responseObserver.onCompleted();
    } catch (IllegalStateException e) {
      responseObserver.onError(
          Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asRuntimeException());
    } catch (Exception e) {
      responseObserver.onError(
          Status.INTERNAL.withDescription("PlanPut failed").withCause(e).asRuntimeException());
    }
  }

  @Override
  public void locate(FilePath request, StreamObserver<LocateResp> responseObserver) {
    try {
      Optional<FileEntry> feOpt = metaStore.getFile(request.getPath());
      if (feOpt.isEmpty()) {
        responseObserver.onError(
            Status.NOT_FOUND
                .withDescription("File not found: " + request.getPath())
                .asRuntimeException());
        return;
      }

      FileEntry fe = feOpt.get();
      LocateResp.Builder resp = LocateResp.newBuilder().setSize(fe.size);
      for (String blockId : fe.blocks) {
        Optional<BlockEntry> beOpt = metaStore.getBlock(blockId);
        if (beOpt.isEmpty()) {
          responseObserver.onError(
              Status.NOT_FOUND
                  .withDescription("Block metadata missing for " + blockId)
                  .asRuntimeException());
          return;
        }
        BlockEntry be = beOpt.get();
        BlockPlan.Builder blockPlan = BlockPlan.newBuilder().setBlockId(blockId).setSize(be.size);
        blockPlan.addAllReplicaUrls(be.replicas);
        resp.addBlocks(blockPlan);
      }

      responseObserver.onNext(resp.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(
          Status.INTERNAL.withDescription("Locate failed").withCause(e).asRuntimeException());
    }
  }

  @Override
  public void commit(CommitReq request, StreamObserver<Ack> responseObserver) {
    if (!coordinator.isLeader()) {
      notLeader(responseObserver);
      return;
    }
    if (request.getBlocksCount() == 0) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT.withDescription("No blocks supplied").asRuntimeException());
      return;
    }

    try {
      long now = System.currentTimeMillis();
      FileEntry fileEntry = new FileEntry();
      fileEntry.size = request.getSize();
      fileEntry.ctime = now;
      fileEntry.mtime = now;

      Map<String, BlockEntry> blocks = new LinkedHashMap<>();
      List<String> blockIds = new ArrayList<>();
      for (CommitBlock block : request.getBlocksList()) {
        blockIds.add(block.getBlockId());
        BlockEntry blockEntry = new BlockEntry();
        blockEntry.size = block.getSize();
        blockEntry.replicas.addAll(block.getReplicasList());
        blocks.put(block.getBlockId(), blockEntry);
      }
      fileEntry.blocks = blockIds;

      metaStore.commit(request.getPath(), fileEntry, blocks);

      Ack ack = Ack.newBuilder().setOk(true).setMsg("Committed").build();
      responseObserver.onNext(ack);
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(
          Status.INTERNAL.withDescription("Commit failed").withCause(e).asRuntimeException());
    }
  }

  private static String generateBlockId(String path, int index) {
    String input = path + "#" + index + "#" + System.nanoTime() + "#" + UUID.randomUUID();
    return UUID.nameUUIDFromBytes(input.getBytes(StandardCharsets.UTF_8)).toString();
  }

  private <T> void notLeader(StreamObserver<T> observer) {
    observer.onError(
        Status.FAILED_PRECONDITION.withDescription("Not leader").asRuntimeException());
  }
}
