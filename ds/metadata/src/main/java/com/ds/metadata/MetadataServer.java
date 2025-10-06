package com.ds.metadata;
public class MetadataServer {
  public static void main(String[] args) {
    int port = 7000;
    for (int i = 0; i + 1 < args.length; i++)
      if (args[i].equals("--port")) port = Integer.parseInt(args[i + 1]);
    System.out.println("[MetadataServer] Starting on port " + port + " (Stage 0 skeleton)");
  }
}
