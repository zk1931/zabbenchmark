package org.apache.zabbenchmark;

import java.util.concurrent.CountDownLatch;
import java.util.Set;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Properties;
import org.apache.zab.QuorumZab;
import org.apache.zab.StateMachine;
import org.apache.zab.Zab;
import org.apache.zab.Zxid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Benchmark.
 */
public class Benchmark implements StateMachine {

  private static final Logger LOG = LoggerFactory.getLogger(Benchmark.class);

  final QuorumZab zab;

  String serverId;

  private static final String CONFIG = "benchmark_config";

  int txnCount;

  int deliveredCount = 0;

  int membersCount = 0;

  CountDownLatch condFinish = new CountDownLatch(1);

  CountDownLatch condBroadcasting = new CountDownLatch(1);

  CountDownLatch condMembers = new CountDownLatch(1);

  Zab.State currentState = null;

  public Benchmark() {
    LOG.debug("Benchmark.");
    try {
      String selfId = System.getProperty("serverId");
      String logDir = System.getProperty("logdir");
      String joinPeer = System.getProperty("join");

      if (selfId != null && joinPeer == null) {
        joinPeer = selfId;
      }

      Properties prop = new Properties();
      if (selfId != null) {
        prop.setProperty("serverId", selfId);
        prop.setProperty("logdir", selfId);
      }
      if (joinPeer != null) {
        prop.setProperty("joinPeer", joinPeer);
      }
      if (logDir != null) {
        prop.setProperty("logdir", logDir);
      }
      zab = new QuorumZab(this, prop);
      this.serverId = zab.getServerId();
    } catch (Exception ex) {
      LOG.error("Caught exception : ", ex);
      throw new RuntimeException();
    }
  }

  @Override
  public void getState(OutputStream os) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setState(InputStream is) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void stateChanged(Zab.State state) {
    if (state == Zab.State.LEADING || state == Zab.State.FOLLOWING) {
      LOG.info("Entering broadcasting phase.");
      this.currentState = state;
      this.condBroadcasting.countDown();
    }
  }

  @Override
  public void deliver(Zxid zxid, ByteBuffer stateUpdate, String clientId) {
    this.deliveredCount++;
    if (this.deliveredCount == this.txnCount) {
      this.condFinish.countDown();
    }
  }

  @Override
  public ByteBuffer preprocess(Zxid zxid, ByteBuffer message) {
    return message;
  }

  @Override
  public void membersChange(Set<String> members) {
    LOG.info("Members change to size of {}", members.size());
    if (members.size() == this.membersCount) {
      this.condMembers.countDown();
    }
  }

  public void start() throws IOException, InterruptedException {
    Properties prop = new Properties();
    try (FileInputStream fin = new FileInputStream(CONFIG)) {
      prop.load(fin);
    } catch (FileNotFoundException ex) {
      LOG.warn("Can't find benchmark_config file, use default config.");
    }
    this.membersCount = Integer.parseInt(prop.getProperty("membersCount", "1"));
    int txnSize = Integer.parseInt(prop.getProperty("txnSize", "128"));
    this.txnCount = Integer.parseInt(prop.getProperty("txnCount", "1000000"));
    LOG.info("Benchmark begins : txnSize {}, txnCount : {}, membersCount : {}",
             txnSize, this.txnCount, this.membersCount);
    this.condBroadcasting.await();
    long startNs = System.nanoTime();
    if (this.currentState == Zab.State.LEADING) {
      LOG.info("It's leading.");
      LOG.info("Waiting for member size changes to {}", this.membersCount);
      this.condMembers.await();
      String message = new String(new char[txnSize]).replace('\0', 'a');
      ByteBuffer buffer = ByteBuffer.wrap(message.getBytes());
      for (int i = 0; i < this.txnCount; ++i) {
        this.zab.send(buffer);
      }
    }
    this.condFinish.await();
    long endNs = System.nanoTime();
    double duration = ((double)(endNs - startNs)) / 1000000000;
    LOG.info("Benchmark finished.");
    LOG.info("Duration : {} s", duration);
    LOG.info("Throughput : {} txns/s", this.txnCount / duration);
  }
}
