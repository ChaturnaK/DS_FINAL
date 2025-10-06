package com.ds.metadata;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkCoordinator implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(ZkCoordinator.class);

  private final CuratorFramework client;
  private final LeaderLatch leaderLatch;

  public ZkCoordinator(String zk, String id) throws Exception {
    this.client =
        CuratorFrameworkFactory.newClient(zk, new ExponentialBackoffRetry(200, 10));
    this.client.start();
    this.client.blockUntilConnected();
    this.leaderLatch = new LeaderLatch(client, "/ds/metadata/leader", id);
    this.leaderLatch.addListener(
        new LeaderLatchListener() {
          @Override
          public void isLeader() {
            log.info("Leadership granted for {}", id);
          }

          @Override
          public void notLeader() {
            log.info("Leadership revoked for {}", id);
          }
        });
    this.leaderLatch.start();
  }

  public CuratorFramework client() {
    return client;
  }

  public boolean isLeader() {
    return leaderLatch.hasLeadership();
  }

  @Override
  public void close() {
    try {
      leaderLatch.close();
    } catch (Exception ignore) {
      // ignore
    }
    client.close();
  }
}
