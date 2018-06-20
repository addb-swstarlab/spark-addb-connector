# ADDB-SRConnector

## Requirements

* Build ADDB(Redis+RocksDB), configure and run redis-server
* Build [ADDB-Jedis](https://bitbucket.org/delab2017/addb-jedis/src/master/)
* Spark v2.0.2 , set SPARK_HOME in .bashrc
* Install maven for Scala

## How to build

```
mvn clean install
```

## How to run

```
spark-sql --jars=/ADDB_SRConnector_Path/target/addb-srconnector-0.0.1-jar-with-dependencies.jar
```

## SQL Example
After run spark-sql,

### CREATE
Set "table" option to INT type.(REQUIRED)
Set "parition" option for partitioning specific column.(REQUIRED)
Set "port" option used in redis-server port (REQUIRED)
Set "AUTH" option if use redis requirepass (OPTIONAL)

```
CREATE TABLE kv
(col1 STRING, col2 INT, col3 STRING, col4 INT)
USING kr.ac.yonsei.delab.addb_srconnector
OPTIONS (host "127.0.0.1", port "8000", table "1", partitions "col2", AUTH "foobared");
```

### INSERT

```
INSERT INTO table kv VALUES ('LJH', 1010, 'CWK', 1004);
```

### SELECT
It is not necessary to apply a filter to the partition key in the "WHERE" clause when "SELECT" statement is called.
But, we recommend to use a filter to the partition key in order to maximize ADDB's performance.
```
SELECT * FROM kv WHERE col2>1000;
```
