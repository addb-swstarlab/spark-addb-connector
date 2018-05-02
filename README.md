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

```
CREATE TABLE kv
(key STRING, value INT)
USING kr.ac.yonsei.delab.addb_srconnector
OPTIONS (table "kv", AUTH "foobared", hi "bye");
```

### INSERT

```
INSERT OVERWRITE table kv SELECT t.* FROM (SELECT 'LJH', 3142) t;
```

### SELECT (cannot be implemented)


