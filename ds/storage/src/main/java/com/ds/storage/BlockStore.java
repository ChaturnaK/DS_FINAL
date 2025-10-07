package com.ds.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.HexFormat;

public class BlockStore {
  private final Path dataDir = Paths.get(System.getProperty("ds.data.dir", "./data/node1"));
  private final Path blocksDir = dataDir.resolve("blocks");
  private final Path metaDir = dataDir.resolve("meta");

  public BlockStore() throws IOException {
    Files.createDirectories(blocksDir);
    Files.createDirectories(metaDir);
  }

  public static final class PutResult {
    public final String checksumHex;
    public final long bytesWritten;

    public PutResult(String checksumHex, long bytesWritten) {
      this.checksumHex = checksumHex;
      this.bytesWritten = bytesWritten;
    }
  }

  public Path blockPath(String blockId) {
    return blocksDir.resolve(blockId);
  }

  public Path metaPath(String blockId) {
    return metaDir.resolve(blockId + ".json");
  }

  public PutResult writeStreaming(String blockId, Iterable<byte[]> chunks) throws Exception {
    Path path = blockPath(blockId);
    Files.createDirectories(path.getParent());
    try (FileChannel channel =
        FileChannel.open(
            path,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING)) {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      long total = 0L;
      for (byte[] bytes : chunks) {
        if (bytes == null || bytes.length == 0) {
          continue;
        }
        digest.update(bytes);
        total += bytes.length;
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        while (buffer.hasRemaining()) {
          channel.write(buffer);
        }
      }
      String hex = HexFormat.of().formatHex(digest.digest());
      return new PutResult(hex, total);
    }
  }

  public void writeMeta(String blockId, String vectorClock, String checksumHex) throws IOException {
    String vc = vectorClock == null ? "" : vectorClock;
    String json =
        "{\"vectorClock\": \"" + vc + "\", \"checksum\": \"" + checksumHex + "\"}";
    Files.writeString(
        metaPath(blockId), json, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
  }

  public byte[] readMeta(String blockId) throws IOException {
    Path path = metaPath(blockId);
    if (!Files.exists(path)) {
      return "{\"vectorClock\":\"\",\"checksum\":\"\"}".getBytes();
    }
    return Files.readAllBytes(path);
  }

  public long size(String blockId) throws IOException {
    Path path = blockPath(blockId);
    return Files.exists(path) ? Files.size(path) : -1L;
  }

  public Iterable<byte[]> streamRead(String blockId, int chunkSize) throws IOException {
    final Path path = blockPath(blockId);
    final int resolvedChunkSize = Math.max(64 * 1024, chunkSize);
    return () ->
        new java.util.Iterator<>() {
          FileChannel channel;
          long remaining;
          boolean closed;

          {
            try {
              channel = FileChannel.open(path, StandardOpenOption.READ);
              remaining = Files.size(path);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public boolean hasNext() {
            if (remaining <= 0 && !closed) {
              try {
                channel.close();
              } catch (IOException ignored) {
              }
              closed = true;
            }
            return remaining > 0;
          }

          @Override
          public byte[] next() {
            try {
              int toRead = (int) Math.min(resolvedChunkSize, remaining);
              ByteBuffer buffer = ByteBuffer.allocate(toRead);
              int read = channel.read(buffer);
              if (read <= 0) {
                remaining = 0;
                channel.close();
                closed = true;
                return new byte[0];
              }
              remaining -= read;
              return buffer.array();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        };
  }
}
