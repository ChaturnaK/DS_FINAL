package com.ds.time;

import com.ds.common.Metrics;
import io.micrometer.core.instrument.Gauge;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

/**
 * Periodically samples NTP servers to measure clock offset. Exposes gauges so the rest of the
 * system can detect when samples go stale and fall back to logical clocks.
 */
public final class NtpSync implements Runnable {
  private static final String STATUS_GAUGE = "ntp.sync.status";
  private static final String FAILURE_GAUGE = "ntp.sync.failures";
  private static final String STALENESS_GAUGE = "ntp.sync.staleness.sec";
  private static final int FAILURE_THRESHOLD = 3;

  private final String[] hosts;
  private final int port;
  private final Gauge offsetGauge;
  private volatile double offsetMs = 0.0;
  private volatile long lastSuccessMs = 0L;
  private volatile int consecutiveFailures = 0;
  private volatile int currentHostIdx = 0;
  private volatile boolean degradedLogged = false;

  public NtpSync(String host, int port) {
    this.hosts =
        host == null || host.isBlank()
            ? new String[] {"pool.ntp.org"}
            : host.split(",");
    this.port = port;
    this.offsetGauge = Metrics.gauge("ntp.offset.ms", 0.0);
    Metrics.gauge(STATUS_GAUGE, 1.0);
    Metrics.gauge(FAILURE_GAUGE, 0.0);
    Metrics.gauge(STALENESS_GAUGE, 0.0);
  }

  @Override
  public void run() {
    Exception lastError = null;
    for (int attempts = 0; attempts < hosts.length; attempts++) {
      int idx = (currentHostIdx + attempts) % hosts.length;
      String target = hosts[idx].trim();
      if (target.isEmpty()) {
        continue;
      }
      try {
        double off = query(target);
        currentHostIdx = idx;
        handleSuccess(off);
        return;
      } catch (Exception e) {
        lastError = e;
      }
    }
    handleFailure(lastError);
  }

  private void handleSuccess(double offset) {
    offsetMs = offset;
    lastSuccessMs = System.currentTimeMillis();
    consecutiveFailures = 0;
    Metrics.gauge("ntp.offset.ms", offsetMs);
    Metrics.gauge(STATUS_GAUGE, 1.0);
    Metrics.gauge(FAILURE_GAUGE, 0.0);
    Metrics.gauge(STALENESS_GAUGE, 0.0);
    if (degradedLogged) {
      System.out.println("[NTP] synchronization recovered; resuming normal mode");
      degradedLogged = false;
    }
    System.out.println("[NTP] offset_ms=" + offsetMs);
  }

  private void handleFailure(Exception error) {
    consecutiveFailures++;
    Metrics.counter("ntp.sync.failures_total").increment();
    Metrics.gauge(STATUS_GAUGE, 0.0);
    Metrics.gauge(FAILURE_GAUGE, consecutiveFailures);
    long stalenessMs =
        lastSuccessMs == 0 ? Long.MAX_VALUE : System.currentTimeMillis() - lastSuccessMs;
    double stalenessSeconds =
        stalenessMs == Long.MAX_VALUE ? Double.MAX_VALUE : stalenessMs / 1000.0;
    Metrics.gauge(STALENESS_GAUGE, stalenessSeconds);
    String msg = error == null ? "unknown error" : error.getMessage();
    System.out.println("[NTP] failed: " + msg + " (falling back to local clock)");
    if (!degradedLogged && consecutiveFailures >= FAILURE_THRESHOLD) {
      System.out.println(
          "[NTP] consecutive failures >= "
              + FAILURE_THRESHOLD
              + "; operating in degraded mode until sync recovers");
      degradedLogged = true;
    }
  }

  private double query(String targetHost) throws Exception {
    byte[] buf = new byte[48];
    buf[0] = 0b0010_0011;
    long t0 = System.currentTimeMillis();
    DatagramSocket sock = new DatagramSocket();
    sock.setSoTimeout(2000);
    DatagramPacket pkt =
        new DatagramPacket(buf, buf.length, InetAddress.getByName(targetHost), port);
    sock.send(pkt);
    DatagramPacket resp = new DatagramPacket(buf, buf.length);
    sock.receive(resp);
    long t3 = System.currentTimeMillis();
    long secs = ((buf[40] & 0xffL) << 24)
        | ((buf[41] & 0xffL) << 16)
        | ((buf[42] & 0xffL) << 8)
        | (buf[43] & 0xffL);
    long fracs = ((buf[44] & 0xffL) << 24)
        | ((buf[45] & 0xffL) << 16)
        | ((buf[46] & 0xffL) << 8)
        | (buf[47] & 0xffL);
    double t2 = (secs - 2208988800L) * 1000.0 + (fracs * 1000.0 / 4294967296.0);
    double offset = ((t2 - t0) + (t2 - t3)) / 2.0;
    sock.close();
    return offset;
  }
}
