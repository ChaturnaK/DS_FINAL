package com.ds.storage.grpc;

import ds.Ack;
import ds.GetHdr;
import ds.GetMeta;
import ds.PutAck;
import ds.PutHdr;
import ds.StorageServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

/**
 * Stage 1 placeholder implementation for storage RPC surface.
 */
public class StorageServiceImpl extends StorageServiceGrpc.StorageServiceImplBase {

  @Override
  public void putBlock(PutHdr request, StreamObserver<PutAck> responseObserver) {
    responseObserver.onError(Status.UNIMPLEMENTED.asRuntimeException());
  }

  @Override
  public void getBlock(GetHdr request, StreamObserver<GetMeta> responseObserver) {
    responseObserver.onError(Status.UNIMPLEMENTED.asRuntimeException());
  }

  @Override
  public void replicateBlock(GetHdr request, StreamObserver<Ack> responseObserver) {
    responseObserver.onError(Status.UNIMPLEMENTED.asRuntimeException());
  }
}
