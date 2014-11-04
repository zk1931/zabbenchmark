package com.github.zk1931.zabbenchmark;

import com.google.protobuf.ByteString;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import com.github.zk1931.zabbenchmark.proto.Transaction.Txn;
import com.github.zk1931.jzab.Zab;
import com.github.zk1931.jzab.ZabConfig;
import com.github.zk1931.jzab.ZabException;
import com.github.zk1931.jzab.StateMachine;
import com.github.zk1931.jzab.Zxid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Benchmark.
 */
public class Benchmark extends TimerTask implements StateMachine {
  private static final Logger LOG = LoggerFactory.getLogger(Benchmark.class);
  final Zab zab;
  String serverId;
  private static final String CONFIG = "benchmark_config";
  int txnCount;
  int txnSize;
  volatile int deliveredCount = 0;
  volatile long latencyTotal = 0;
  volatile int deliveredCountForLastTimer = 0;
  volatile long latencyTotalForLastTimer = 0;
  int membersCount = 0;
  int stateMemory = 0;
  int timeInterval = 0;
  CountDownLatch condFinish = new CountDownLatch(1);
  CountDownLatch condMembers = new CountDownLatch(1);
  CountDownLatch condBroadcasting = new CountDownLatch(1);
  State currentState = null;
  ConcurrentHashMap<Integer, String> state = new ConcurrentHashMap<>();
  boolean multipleFsync;

  enum State {
    LEADING,
    FOLLOWING
  }

  public Benchmark() {
    LOG.debug("Benchmark.");
    try {
      this.multipleFsync = System.getProperty("multipleFsync", "0").equals("1");
      String selfId = System.getProperty("serverId");
      String logDir = System.getProperty("logdir");
      String joinPeer = System.getProperty("join");
      long snapshot = Long.parseLong(System.getProperty("snapshot", "-1"));
      if (selfId != null && joinPeer == null) {
        joinPeer = selfId;
      }
      ZabConfig config = new ZabConfig();
      if (selfId != null) {
        config.setServerId(selfId);
        config.setLogDir(selfId);
      }
      if (logDir != null) {
        config.setLogDir(logDir);
      }
      config.setTimeoutMs(5000);
      config.setMinSyncTimeoutMs(5000);
      config.setSnapshotThreshold(snapshot);
      zab = new Zab(this, config, joinPeer);
      this.serverId = zab.getServerId();
    } catch (Exception ex) {
      LOG.error("Caught exception : ", ex);
      throw new RuntimeException();
    }
  }

  @Override
  public void save(OutputStream os) {
    LOG.info("SAVE is called.");
    long startNs = System.nanoTime();
    int count = 0;
    int syncThreshold = state.size() / 10;
    try {
      DataOutputStream out =
        new DataOutputStream(new BufferedOutputStream(os));
      Iterator<Map.Entry<Integer, String>> iter
        = state.entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<Integer, String> pairs = iter.next();
        out.writeInt(pairs.getKey());
        out.writeBytes(pairs.getValue());
        count++;
        if (multipleFsync && count % syncThreshold == 0) {
          out.flush();
          ((FileOutputStream)os).getChannel().force(false);
        }
      }
    } catch (Exception e) {
      LOG.error("Caught exception", e);
    }
    LOG.info("SAVE ends, it took {} milliseconds to finish.",
             (System.nanoTime() - startNs) / 1000 / 1000);
  }

  @Override
  public void restore(InputStream is) {
    LOG.info("RESTORE is called.");
  }

  @Override
  public void deliver(Zxid zxid, ByteBuffer stateUpdate, String clientId) {
    int idx = 0;
    try {
      this.deliveredCount++;
      FakeTxn txn = FakeTxn.fromByteBuffer(stateUpdate);
      // state.put(deliveredCount % state.size(), new String(txn.body));
      if (this.deliveredCount == this.txnCount) {
        this.condFinish.countDown();
      }
      if (clientId != null && clientId.equals(this.serverId)) {
        long latency = (System.nanoTime() - txn.timestamp) / 1000000;
        this.latencyTotal += latency;
      }
    } catch (Exception ex) {
      LOG.warn("exception {}", idx);
    }
  }

  @Override
  public void flushed(ByteBuffer request) {
    // Does nothing.
  }

  @Override
  public ByteBuffer preprocess(Zxid zxid, ByteBuffer message) {
    return message;
  }

  @Override
  public void leading(Set<String> activeFollowers, Set<String> members) {
    this.currentState = State.LEADING;
    this.condBroadcasting.countDown();
    LOG.info("LEADING with active followers : ");
    for (String peer : activeFollowers) {
      LOG.info(" -- {}", peer);
    }
    LOG.info("Cluster configuration change : ", members.size());
    for (String peer : members) {
      LOG.info(" -- {}", peer);
    }
    if (members.size() >= this.membersCount) {
      this.condMembers.countDown();
    }
  }

  @Override
  public void following(String leader, Set<String> members) {
    this.currentState = State.FOLLOWING;
    this.condBroadcasting.countDown();
    LOG.info("FOLLOWING {}", leader);
    LOG.info("Cluster configuration change : ", members.size());
    for (String peer : members) {
      LOG.info(" -- {}", peer);
    }
    if (members.size() >= this.membersCount) {
      this.condMembers.countDown();
    }
  }

  @Override
  public void recovering() {
    LOG.info("Recovering...");
  }

  void initializeConfiguration() throws IOException {
    Properties prop = new Properties();
    try (FileInputStream fin = new FileInputStream(CONFIG)) {
      prop.load(fin);
    } catch (FileNotFoundException ex) {
      LOG.warn("Can't find benchmark_config file, use default config.");
    }
    this.membersCount = Integer.parseInt(prop.getProperty("membersCount", "1"));
    this.txnSize = Integer.parseInt(prop.getProperty("txnSize", "128"));
    this.txnCount = Integer.parseInt(prop.getProperty("txnCount", "1000000"));
    this.stateMemory =
      Integer.parseInt(prop.getProperty("stateMemory", "1000000"));
    this.timeInterval =
      Integer.parseInt(prop.getProperty("timeInterval", "1000"));
    LOG.info("Benchmark configurations { txnSize: {}, txnCount: {}" +
             ", membersCount: {}, timeInterval: {}, stateMemory: {} }.",
             this.txnSize, this.txnCount, this.membersCount, this.timeInterval,
             this.stateMemory);
  }

  public void start() throws IOException, InterruptedException {
    // Initializes configuration from configuration file or use default
    // configuration.
    initializeConfiguration();
    // Initialze the state machine.
    initState();

    this.condBroadcasting.await();
    long startNs;
    Timer timer = new Timer();

    if (this.currentState == State.LEADING) {
      LOG.info("It's leading.");
      LOG.info("Waiting for member size changes to {}", this.membersCount);
      this.condMembers.await();
      timer.scheduleAtFixedRate(this, 0, timeInterval);
      startNs = System.nanoTime();
      byte[] data = new String(new char[txnSize]).replace('\0', 'a').getBytes();

      for (int i = 0; i < this.txnCount; ++i) {
        FakeTxn txn = new FakeTxn(ByteBuffer.wrap(data));
        try {
          this.zab.send(txn.toByteBuffer());
        } catch (ZabException.NotBroadcastingPhaseException e) {
          LOG.warn("Send transaction not in broadcasting phase.");
          Thread.sleep(500);
        }
      }
    } else {
      this.condMembers.await();
      timer.scheduleAtFixedRate(this, 0, timeInterval);
      startNs = System.nanoTime();
    }
    this.condFinish.await();
    timer.cancel();
    long endNs = System.nanoTime();
    double duration = ((double)(endNs - startNs)) / 1000000000;
    LOG.info("Benchmark finished.");
    LOG.info("Duration : {} s", duration);
    LOG.info("Throughput : {} txns/s", this.txnCount / duration);
    LOG.info("Latency : {} ms/txn", this.latencyTotal / this.txnCount);
    this.zab.shutdown();
  }

  void initState() {
    LOG.info("Initializing the state.");
    int numKeys = this.stateMemory / this.txnSize;
    String value = new String(new char[txnSize]).replace('\0', 'a');
    for (int i = 0; i < numKeys; ++i) {
      state.put(i, value);
    }
    LOG.info("After initialize the memory, the state has size {}",
             state.size());
  }

  @Override
  public void run() {
    int lastIntervalThroughput = deliveredCount - deliveredCountForLastTimer;
    long lastIntervalLatency = latencyTotal - latencyTotalForLastTimer;
    deliveredCountForLastTimer = deliveredCount;
    latencyTotalForLastTimer = latencyTotal;
    long avgLatency = (lastIntervalThroughput == 0)? 0 :
      lastIntervalLatency / lastIntervalThroughput;
    LOG.info("Timer: throughput {}, latency {}, memory {}, deliver {}.",
             lastIntervalThroughput / (float)(timeInterval / 1000),
             avgLatency,
             Runtime.getRuntime().totalMemory(),
             deliveredCount);
  }

  static class FakeTxn {
    final ByteBuffer body;
    final long timestamp;

    FakeTxn(ByteBuffer body) {
      this.timestamp = System.nanoTime();
      this.body = body;
    }

    FakeTxn(long ts, ByteBuffer body) {
      this.timestamp = ts;
      this.body = body;
    }

    ByteBuffer toByteBuffer() {
      Txn txn = Txn.newBuilder().setTimestamp(System.nanoTime())
                   .setBody(ByteString.copyFrom(body)).build();
      return ByteBuffer.wrap(txn.toByteArray());
    }

    static FakeTxn fromByteBuffer(ByteBuffer buffer) throws Exception {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      Txn txn = Txn.parseFrom(bytes);
      return new FakeTxn(txn.getTimestamp(),
                         txn.getBody().asReadOnlyByteBuffer());
    }
  }
}
