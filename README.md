zabbenchmark
============

Benchmark for [javazab](https://github.com/ZK-1931/javazab)

### Configuration
You can configure it by creating a configuration file ```benchmark_config``` in root directory of the project. The default configuration is 
```
membersCount = 1
txnSize = 128
txnCount = 1000000
```

### Usage
Let's say if you run the benchmark on a cluster of 3 servers.

    mvn -DserverId="host1:port1" exec:java
    mvn -DserverId="host2:port2" -Djoin="host1:port1" exec:java
    mvn -DserverId="host3:port3" -Djoin="host1:port1" exec:java
