package kr.ac.yonsei.delab.addb_srconnector

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import redis.clients.addb_jedis.Protocol

import scala.collection.mutable.ArrayBuffer
import kr.ac.yonsei.delab.addb_srconnector.util.KeyUtil
import java.util.HashSet
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import org.apache.spark.sql.types._
import java.util.ArrayList


object ColumnType extends Enumeration {
  type ColumnType = Value
  val StringType = Value( "String" )
  val NumericType = Value( "Numeric" )
}

//import ColumnType._
case class RedisColumn(val name: String, val columnType: ColumnType.Value ) { }

case class RedisTable (
    val id: Int,
    val columns: Array[RedisColumn],
//  val indices: Array[RedisIndex],
    val partitionColumnNames: Array[String]) {
//  val scoreName: String) {
//	def buildTable():RedisTable = {
//	  this
//	}
}


class Source(
    tableId: Int,
    columnCnt: Int,
    Partition: String
    ) {  
}

//class RedisRowBase(Source
//  val columns: Map[String, String] )
//  extends Serializable {
//}

case class RedisRow( val table: RedisTable, val columns: Map[String, String])
//  override val columns: Map[String, String])
//  extends RedisRowBase(columns) {
extends Serializable {
	
}

class RedisStore (val redisConfig:RedisConfig)
  extends Configurable {
  
  // For INSERT statement (add function) 
  val redisCluster: RedisCluster = {
    new RedisCluster({
      val configuration = this.redisConfig.configuration
      val host = configuration.getOrElse("host", Protocol.DEFAULT_HOST)
    	val port = configuration.getOrElse("port", Protocol.DEFAULT_PORT.toString).toInt
    	val auth = configuration.getOrElse("auth", null)
    	val dbNum = configuration.getOrElse("dbNum", Protocol.DEFAULT_DATABASE.toString).toInt
    	val timeout = configuration.getOrElse("timeout", Protocol.DEFAULT_TIMEOUT.toString).toInt
    	new RedisConnection(host, port, auth, dbNum, timeout)
    })
  }
  def getTablePartitions(table: RedisTable ) : Array[(String, Array[String])] = {
    logInfo( s"[WONKI] : getTablePartitions called")
    val metaKey =KeyUtil.generateKeyForMeta(table.id)
    logInfo( s"[WONKI] : metaKey: $metaKey" )
//    val targetNode = KeyUtil.getNodeForKey(redisCluster.hosts, metaKey)
//    logInfo( s"[WONKI] : targetNode: $targetNode" )
//    val conn = targetNode.connect()
//
//    val ret = conn.getMeta(metaKey)
//    logInfo( s"[WONKI] : ret: $ret" )
//    conn.close()
    val ret_scala : ArrayBuffer[String] = ArrayBuffer[String]()    
    redisCluster.hosts.foreach{
        x =>
        val conn = x.connect()
        conn.getMeta(metaKey).foreach(
        x => ret_scala+=KeyUtil.getPartitionFromMeta(x)    
        )
        conn.close()      
    }
        
//        
//    val setIter = ret.iterator()
//    val ret_scala : ArrayBuffer[String] = ArrayBuffer[String]()
//
//    while ( setIter.hasNext() ) {
//      ret_scala+=KeyUtil.getPartitionFromMeta(setIter.next())
//    }
    logInfo( s"[WONKI] : ret_scala: $ret_scala" )

    /* id - column cnt - partition */
    val sourceInfos = KeyUtil.groupKeysByNode(redisCluster.hosts, ret_scala.toArray)
    sourceInfos.foreach{
      x => 
      logInfo( s"[WONKI] : sourceInfos host-port : $x._1  partition : $x._2")
    }
    logInfo( s"[WONKI] : sourceInfos: $sourceInfos" )
    sourceInfos
  }

  def add(rows: Iterator[RedisRow]): Unit = {
    // 1) generate datakey
    val keyRowPair:Map[String, RedisRow] = rows.map{ row => 
    val dataKey = KeyUtil.generateDataKey(row.table.id)
    // should add partitionInfo
//    val dataKey = KeyUtil.generateDataKey(row.table.id, partitionInfo)
      (dataKey, row)
    }.toMap
    // 2) execute on pipeline on each node
    // SRC := [RedisRelation]-RedisCluster(RedisConnection)
    // ADDB :=  [RedisStore]-RedisCluster(RedisConnection)
    KeyUtil.groupKeysByNode(redisCluster.hosts, keyRowPair.keysIterator).foreach{
      case(node, datakeys) => {
        val conn = node.connect
        val pipeline = conn.pipelined
        datakeys.foreach{
          datakey => {
            val row:RedisRow = keyRowPair(datakey)
            row.columns.foreach {
              column =>
              pipeline.set(datakey+column._2, column._2)
              }
//    					pipeline.hmset(datakey, row.getValuesMap(row.schema.fieldNames).map(x => (x._1, x._2.toString)))
    				}
          }
        pipeline.sync
        conn.close
        }
     }
//    try {
//      rows.foreach { row => add(row) }
//    } finally {
//    }
  }

  def add(row: RedisRow): Unit = {
    // 1) generate data key
//    val key = KeyUtil.generateDataKey(row.table.id)
    // 2) execute on pipeline
//    key.toIterator
    
//    val sourceInfo = handler.rowIdOf(row)
//    sessionManager.run { session: Session =>
//      session.nvwrite(sourceInfo, mutableSeqAsJavaList(row.table.columnNames.map { columnName => row.columns(columnName) }))
//    }
  }

  def get(key: String): Iterator[RedisRow] = {
    throw new RuntimeException(s"Unsupported method on this mode")
  }

  def getByRanges(
                   table: String,
                   key: String,
                   ranges: Array[Range]
                 ): Iterator[RedisRow] = {
    throw new RuntimeException(s"Unsupported method on this mode")
  }

  def remove(row: RedisRow): Unit = {
    throw new RuntimeException(s"Unsupported method on this mode")
  }

  def scan(
    table: RedisTable,
    location: String,
    partitions: Array[String], //partition key
    prunedColumns: Array[String]) : Iterator[RedisRow] = {

    val columnIndex = prunedColumns.map { columnName =>
      ""+ (table.columns.map(_.name).indexOf(columnName) + 1)
    }
    
    //val keys : Array[String] = columnIndex.toArray
    val keys = Array("col1","col2","col3","col1","col2","col3")

    
    val host = KeyUtil.returnHost(location)
    val port = KeyUtil.returnPort(location)

    val conn = new RedisConnection("127.0.0.1", 6379, redisCluster.host.auth, 
        					redisCluster.host.dbNum, redisCluster.host.timeout).connect()
    val pipeline = conn.pipelined()
    
    
    /* TO DO , Different IP is not enabled */
    partitions.foreach{
      partition =>
          val keys = KeyUtil.generateDataKey(table.id, partition)
        keys.foreach{
          x =>
             // TO DO, prunedCoulumns need to be changed columnIndex when nvscan is implemented
            prunedColumns.map {
              column =>
                //KeyUtil.generateDataKey(table.id, partition)
                pipeline.hmget(x, column)
            }
        }
    }
    val res = keys.zip(pipeline.syncAndReturnAll().map(_.asInstanceOf[ArrayList[String]].get(0)))

   val numRow = keys.length / prunedColumns.length
   conn.close()

    var totalRow = 0

    val result = res.grouped(prunedColumns.length).map { x =>
      val columns: Map[String, String] = x.toMap
      val redisRow = new RedisRow(table, columns)
      (redisRow)
    }
    result
  }
}
