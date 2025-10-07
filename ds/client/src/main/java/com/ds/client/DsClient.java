package com.ds.client;

public class DsClient {
  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.out.println(
          "Usage: putBlock <host:port> <blockId> <file> | getBlock <host:port> <blockId> <out>");
      return;
    }
    switch (args[0]) {
      case "putBlock" -> {
        if (args.length < 4) {
          System.out.println("putBlock requires host:port blockId file");
          return;
        }
        String[] hp = args[1].split(":");
        StreamTool.put(
            hp[0], Integer.parseInt(hp[1]), args[2], java.nio.file.Paths.get(args[3]));
      }
      case "getBlock" -> {
        if (args.length < 4) {
          System.out.println("getBlock requires host:port blockId out");
          return;
        }
        String[] hp = args[1].split(":");
        StreamTool.get(
            hp[0], Integer.parseInt(hp[1]), args[2], java.nio.file.Paths.get(args[3]));
      }
      default -> System.out.println("Unknown command");
    }
  }
}
