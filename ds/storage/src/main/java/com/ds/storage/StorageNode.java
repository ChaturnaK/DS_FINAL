package com.ds.storage;
public class StorageNode {
  public static void main(String[] args) {
    int port = 8001; String dataDir = "./data/node1";
    for (int i = 0; i + 1 < args.length; i++) {
      if (args[i].equals("--port")) port = Integer.parseInt(args[i + 1]);
      if (args[i].equals("--data")) dataDir = args[i + 1];
    }
    System.out.println("[StorageNode] Starting on port " + port + ", data dir " + dataDir + " (Stage 0 skeleton)");
  }
}
