package com.ds.storage.grpc;

import com.ds.storage.BlockStore;
import ds.Ack;
import ds.BlockChunk;
import ds.GetHdr;
import ds.GetMeta;
import ds.GetResponse;
import ds.PutAck;
import ds.PutOpen;
import ds.PutRequest;
import ds.StorageServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;

public class StorageServiceImpl extends StorageServiceGrpc.StorageServiceImplBase {
  private final BlockStore store;

  public StorageServiceImpl(BlockStore store) {
    this.store = store;
  }

  @Override
  public StreamObserver<PutRequest> putBlock(StreamObserver<PutAck> responseObserver) {
    return new StreamObserver<>() {
      String blockId;
      String vectorClock;
      final List<byte[]> chunks = new ArrayList<>();

      @Override
      public void onNext(PutRequest request) {
        switch (request.getPayloadCase()) {
          case OPEN -> {
            PutOpen open = request.getOpen();
            blockId = open.getBlockId();
            vectorClock = open.getVectorClock();
          }
          case CHUNK -> chunks.add(request.getChunk().getData().toByteArray());
          case PAYLOAD_NOT_SET -> {
            // ignore
          }
        }
      }

      @Override
      public void onError(Throwable throwable) {
        responseObserver.onError(throwable);
      }

      @Override
      public void onCompleted() {
        if (blockId == null || blockId.isBlank()) {
          responseObserver.onError(
              Status.INVALID_ARGUMENT.withDescription("Missing blockId").asRuntimeException());
          return;
        }
        try {
          BlockStore.PutResult result = store.writeStreaming(blockId, chunks);
          store.writeMeta(blockId, vectorClock, result.checksumHex);
          PutAck ack =
              PutAck.newBuilder()
                  .setOk(true)
                  .setChecksum(result.checksumHex)
                  .setMsg("bytes=" + result.bytesWritten)
                  .build();
          responseObserver.onNext(ack);
          responseObserver.onCompleted();
        } catch (Exception e) {
          responseObserver.onError(
              Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
      }
    };
  }

  @Override
  public void getBlock(GetHdr request, StreamObserver<GetResponse> responseObserver) {
    String blockId = request.getBlockId();
    try {
      byte[] metaBytes = store.readMeta(blockId);
      String metaStr = new String(metaBytes);
      String vc = metaStr.replaceAll(".*\"vectorClock\"\\s*:\\s*\"([^\"]*)\".*", "$1");
      String checksum = metaStr.replaceAll(".*\"checksum\"\\s*:\\s*\"([^\"]*)\".*", "$1");
      GetMeta meta = GetMeta.newBuilder().setVectorClock(vc).setChecksum(checksum).build();
      responseObserver.onNext(GetResponse.newBuilder().setMeta(meta).build());

      for (byte[] buf : store.streamRead(blockId, 1024 * 1024)) {
        if (buf.length == 0) {
          continue;
        }
        GetResponse chunkResp =
            GetResponse.newBuilder()
                .setChunk(BlockChunk.newBuilder().setData(com.google.protobuf.ByteString.copyFrom(buf)))
                .build();
        responseObserver.onNext(chunkResp);
      }
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(
          Status.NOT_FOUND.withDescription(e.getMessage()).asRuntimeException());
    }
  }

  @Override
  public void replicateBlock(GetHdr request, StreamObserver<Ack> responseObserver) {
    responseObserver.onNext(
        Ack.newBuilder().setOk(false).setMsg("Not implemented").build());
    responseObserver.onCompleted();
  }
}
