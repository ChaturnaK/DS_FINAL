package com.ds.metadata.grpc;

import ds.Ack;
import ds.CommitReq;
import ds.FilePath;
import ds.LocateResp;
import ds.MetadataServiceGrpc;
import ds.PlanPutReq;
import ds.PlanPutResp;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

/**
 * Stage 1 placeholder implementation for metadata RPC surface.
 */
public class MetadataServiceImpl extends MetadataServiceGrpc.MetadataServiceImplBase {

  @Override
  public void planPut(PlanPutReq request, StreamObserver<PlanPutResp> responseObserver) {
    responseObserver.onError(Status.UNIMPLEMENTED.asRuntimeException());
  }

  @Override
  public void locate(FilePath request, StreamObserver<LocateResp> responseObserver) {
    responseObserver.onError(Status.UNIMPLEMENTED.asRuntimeException());
  }

  @Override
  public void commit(CommitReq request, StreamObserver<Ack> responseObserver) {
    responseObserver.onError(Status.UNIMPLEMENTED.asRuntimeException());
  }
}
