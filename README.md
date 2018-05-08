# ADDB-SRConnector

## Requirements

* Build ADDB(Redis+RocksDB), configure and run redis-server
* Build [addb-jedis](http://vodka.yonsei.ac.kr/gitlab/hwani3142/addb-jedis)
* Spark v2.0.2 , set SPARK_HOME in .bashrc
* Install SBT(Simple Build Tool) for Scala

## How to run
arg1 : sparkall, sqlall, spark, sql, compile

```
./run.sh arg1
```

## SQL Example
When run spark-sql,

### CREATE
Set "table" option to INT type.(MUST)
Set "parition" option for partitioning specific column.(MUST)
Set "AUTH" option if use redis requirepass (OPTIONAL)

```
CREATE TABLE kv
(key STRING, value INT)
USING kr.ac.yonsei.delab.addb_srconnector
OPTIONS (host "127.0.0.1", port "7000", table "1", partitions "key", AUTH "foobared");
```

### INSERT

```
INSERT INTO table kv VALUES ('LJH', 3142, 'CWG', 1111);
```

### SELECT (cannot be implemented)


