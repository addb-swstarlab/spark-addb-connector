package kr.ac.yonsei.delab.addb_srconnector

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._

import scala.collection.JavaConversions.mutableSeqAsJavaList
import scala.collection.JavaConverters._
import scala.collection.mutable.Stack

import redis.clients.addb_jedis.Protocol
import redis.clients.addb_jedis.util.CommandArgsObject

import scala.collection.mutable.ArrayBuffer
import kr.ac.yonsei.delab.addb_srconnector.util.KeyUtil
import java.util.HashSet
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import org.apache.spark.sql.types._
import java.util.ArrayList
import kr.ac.yonsei.delab.addb_srconnector.util.FilterUtil
import kr.ac.yonsei.delab.addb_srconnector.util.Logging


object ColumnType extends Enumeration {
  type ColumnType = Value
  val StringType = Value( "String" )
  val NumericType = Value( "Numeric" )
}

/*
 * RedisRow class
 * 		represent table, column info associated with row
 * table := redis table including this RedisRow
 * columns := each columns information (column name -> value)
 */
case class RedisRow( val table: RedisTable, val columns: Map[String, String])
//  override val columns: Map[String, String])
//  extends RedisRowBase(columns) {
extends Serializable {
	
}
/*
 * RedisColumn class
 * 		represent each column
 * name := column name
 * columnType := column type(String | Numeric)
 */
case class RedisColumn(val name: String, val columnType: ColumnType.Value ) { }

/*
 * RedisTable class
 *		include table information
 * id := tableID from CREATE TABLE OPTIONS table
 * columns := RedisColumn array
 */
case class RedisTable (
    val id: Int,
    val columns: Array[RedisColumn],
//  val indices: Array[RedisIndex],
    val partitionColumnNames: Array[String]) {
//  val scoreName: String) {
  
  val columnCount = columns.size
  val columnNameWithID = columns.map(_.name).zip(Stream from 1).toMap // from index 1
  val partitionColumnID:Array[Int] = partitionColumnNames.map(columnName => 
                                 columnNameWithID(columnName)).toArray
//  val partitionInfo = PartitionUtil.getPartitionInfo(partitionColumnIndex)
  
  // get index column index&type
//  val index:Array[Int] = 
}
/*
 * For reducing overhead when build redis table, maintain RedisTable list
 */
object RedisTableList 
  extends Logging {
  var list = Map[Int, RedisTable]()
  def insertTableList (tableID: Int, redisTable:RedisTable) {
    list += (tableID -> redisTable)
    		logInfo(s"Insert into RedisTableList")
  }
  
  def checkList(tableID: Int):Boolean = {
    if (list.size == 0) {
    	 logInfo(s"RedisTableList is empty")
    	 false
    } else if (list.get(tableID) == None) {
    	 logInfo(s"RedisTable is not in RedisTableList ")
    	 false
    } else {
     	 logInfo(s"RedisTable is in RedisTableList ")
      true
    }
  }
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

/*
 * RedisStore class
 * 		run actual INSERT(add), SELECT(scan) statement from/to redis 
 */
class RedisStore (val redisConfig:RedisConfig)
  extends Configurable {
  
  // Retain host's cluster node
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
  
  def getTablePartitions(table: RedisTable, filter: Array[Filter]) : Array[(String, Array[String])] = {
    logInfo( s"[WONKI] : getTablePartitions called")
    val metaKey =KeyUtil.generateKeyForMeta(table.id)
    logInfo( s"[WONKI] : metaKey: $metaKey" )

    var retbuf = new StringBuilder
    filter.foreach { x =>
      var stack = new Stack[String]
      FilterUtil.makeFilterString(x, stack)
      while (!stack.isEmpty) {
        retbuf.append(stack.pop())
      }
      retbuf.append("$")
    }
    logInfo(s"new String for Filter = " + retbuf.toString())

    val ret_scala : ArrayBuffer[String] = ArrayBuffer[String]()    
    redisCluster.hosts.foreach{
        x =>
        val conn = x.connect()
        conn.metakeys(metaKey).foreach {
        x => ret_scala += KeyUtil.getPartitionFromMeta(x)
        }
        conn.close()      
    }
        
    logInfo( s"[WONKI] : ret_scala: $ret_scala" )

    /* id - column cnt - partition */
    val sourceInfos = KeyUtil.groupKeysByNode(redisCluster.hosts, KeyUtil.generateDataKey(table.id, ret_scala.toArray))
    sourceInfos.foreach{
      x => 
      logInfo( s"[WONKI] : sourceInfos host-port : $x._1  partition : $x._2")
    }
    logInfo( s"[WONKI] : sourceInfos: $sourceInfos" )
    sourceInfos
  }

  /*
   * Add data to redis through jedis pipeline
   * Process INSERT(fpwrite) command according each node
   */
  def add(rows: Iterator[RedisRow]): Unit = {
    var partitionInfo = ""
    // 0) Convert iterator to array for generating datakey
    val rowForTableInfo = rows.toArray
    
    // 1) Generate datakey
    val keyRowPair:Map[String, RedisRow] = rowForTableInfo.map{ row => 
      // Generate partition:= (1) (index, name) -> (2) (index, value)
      val partitionIndexWithName = row.table.partitionColumnID.zip(row.table.partitionColumnNames)
      val partitionIndexWithValue = partitionIndexWithName.map(column => 
                               (column._1, row.columns.get(column._2).get))
      val (dataKey, partition) = KeyUtil.generateDataKey(row.table.id, partitionIndexWithValue)
      // set PartitionInfo
      partitionInfo = partition
      (dataKey, row)
    }.toMap
    
    // 2) Execute pipelined command in each node
    //   From SRC := [RedisRelation]-RedisCluster(RedisConnection)
    //   To ADDB :=  [RedisStore]-RedisCluster(RedisConnection)
    KeyUtil.groupKeysByNode(redisCluster.hosts, keyRowPair.keysIterator).foreach{
      case(node, datakeys) => {
        val conn = node.connect
        val pipeline = conn.pipelined
        datakeys.foreach{
          datakey => {
            val row:RedisRow = keyRowPair(datakey)
            
            // Convert from data:String to data:List<String> (compatible with Java List type)
            val data = row.columns.map(_._2).toList.asJava
            logInfo(s"PartitionInfo: "+ partitionInfo)
            // parameters: datakey, ColumnCount:String, partitionInfo, rowData
            val commandArgsObject = new CommandArgsObject(datakey, row.table.columnCount.toString, 
                 partitionInfo, data);
            pipeline.fpwrite(commandArgsObject);
    				}
          }
        pipeline.sync
        conn.close
        }
     }
  }

  // Do not use each RedisRow parameter because of groupKeysByNode function
//  def add(row: RedisRow): Unit = {
//    
//  }

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
    prunedColumns: Array[String]): Iterator[RedisRow] = {

    val columnIndex = prunedColumns.map { columnName =>
      "" + (table.columns.map(_.name).indexOf(columnName) + 1)
    }

    //val keys : Array[String] = columnIndex.toArray

    val host = KeyUtil.returnHost(location)
    val port = KeyUtil.returnPort(location)
    
    // Redirection problem occur.
    val conn = new RedisConnection("127.0.0.1", port, redisCluster.host.auth,
      redisCluster.host.dbNum, redisCluster.host.timeout).connect()
    val pipeline = conn.pipelined()

    /* TO DO , Different IP is not enabled */
    partitions.foreach {
      partition =>
//        val keys = KeyUtil.generateDataKey(table.id, partition)
//        keys.foreach {
//          x =>
            logInfo( s"[WONKI] : key in scan = $partition | port = $port")
            // TO DO, prunedCoulumns need to be changed columnIndex when nvscan is implemented,
            // because Redis do not recognize columnName
            prunedColumns.map {
              column =>
                logInfo( s"column : $column")
                //KeyUtil.generateDataKey(table.id, partition)
                pipeline.hmget(partition, column)
//            }
        }
    }
    val values = pipeline.syncAndReturnAll().map(_.asInstanceOf[ArrayList[String]].get(0))
    values.foreach { x => logInfo(s"values: $x") }
    val numRow = values.length / prunedColumns.length
    println("value length = " + values.length)
    println("pruned length = " + prunedColumns.length)
    println("numRow = " + numRow)
    val columnList = Stream.continually(prunedColumns).take(numRow).flatten.toArray
    columnList.foreach( x => logInfo(s"columnList $x"))
    val res = columnList.zip(values)

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
