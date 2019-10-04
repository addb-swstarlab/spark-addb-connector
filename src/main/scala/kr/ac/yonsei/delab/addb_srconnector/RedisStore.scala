package kr.ac.yonsei.delab.addb_srconnector

import java.util.HashSet
import java.util.ArrayList

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.{Stack, ArrayBuffer, ListBuffer}

import redis.clients.addb_jedis.Protocol
import redis.clients.addb_jedis.util.CommandArgsObject
import redis.clients.addb_jedis.exceptions.JedisClusterException

import kr.ac.yonsei.delab.addb_srconnector.util.KeyUtil
import kr.ac.yonsei.delab.addb_srconnector.util.Filters
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
  extends Serializable { }
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
    val partitionColumnNames: Array[String]) {

  val columnCount = columns.size
  val columnNameWithIndex = columns.map(_.name).zip(Stream from 1) // index sorted Array
  val columnNameWithID = columns.map(_.name).zip(Stream from 1).toMap // from index 1. not sorted
  val partitionColumnID:Array[Int] = partitionColumnNames.map(
      columnName => columnNameWithID(columnName)).toArray
}
/*
 * For reducing overhead when build redis table, maintain RedisTable list
 */
object RedisTableList 
  extends Logging {
  var list = Map[Int, RedisTable]()
  def insertTableList (tableID: Int, redisTable:RedisTable) {
    list += (tableID -> redisTable)
  }

  def checkList(tableID: Int):Boolean = {
    if (list.size == 0) false
    else if (list.get(tableID) == None) false
    else true
  }
  
  def getTableColumnWithIndex(tableID: Int, table:RedisTable):Map[String, Int] = {
    var res = list.get(tableID)
    if (res == None) {
      RedisTableList.insertTableList(tableID, table)
      res = list.get(tableID)
      throw new NoSuchElementException(s"[ADDB] Fatal error: There is no corresponding RedisTable...")
    }
    res.get.columnNameWithID
  }
}
/*
 * RedisStore class
 * 		run actual INSERT(add), SELECT(scan) statement from/to redis 
 */
class RedisStore (val redisConfig:RedisConfig)
  extends Configurable 
  with Serializable {
  
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
  
  // Call by getPartitions
  def getTablePartitions(table: RedisTable, filter: Array[Filter]) : Array[(String, Array[String])] = {
    logDebug( s"[ADDB] : getTablePartitions called")
    val metaKey =KeyUtil.generateKeyForMeta(table.id)
    logDebug( s"[ADDB] : metaKey: $metaKey" )
    val sf = System.currentTimeMillis
    // Make filter
    var retbuf = new StringBuilder
    filter.foreach { 
      x =>
        var stack = new Stack[String]
        Filters.makeFilterString(x, stack, table.id, table)
        while (!stack.isEmpty) {
          retbuf.append(stack.pop())
         }
      retbuf.append("$")
     }
    val ef = System.currentTimeMillis
    logInfo(s"[ADDB] make filterString ${(ef-sf)/1000.0f}")
    logDebug(s"new String for Filter = " + retbuf.toString() +", "+ retbuf.toString.isEmpty)
		logInfo(s"new String for Filter = " + retbuf.toString() +", "+ retbuf.toString.isEmpty)

    val sg = System.currentTimeMillis
    val ret_scala : ArrayBuffer[String] = ArrayBuffer[String]()    
    redisCluster.nodes.foreach{
        x =>
          val conn = x.connect()
          conn.metakeys(metaKey, retbuf.toString).foreach (
            x => ret_scala += KeyUtil.getPartitionFromMeta(x) )
          conn.close()      
    }
        
    // Spark partitioning := partition keys with corresponding port
    val partitioning = KeyUtil.groupKeysByNode(redisCluster.nodes, KeyUtil.generateDataKey(table.id, ret_scala.toArray))
    val eg = System.currentTimeMillis
    logInfo(s"[ADDB] metakeys ${(eg-sg)/1000.0f}")
    partitioning
  }

  /*
   * Add data to redis through jedis pipeline
   * Process INSERT(fpwrite) command according each node
   */
  def add(row: RedisRow, 
          pipelinePool:PipelinePool
          /*,
          datakey: String,
          partitionInfo:String */): Unit = {
    //logInfo("[ADDB] add(INSERTION) function")
    var partitionInfo = new StringBuilder
//    rowForTableInfo.foreach { x => logInfo(s"rowForTableInfo: ${x.columns}") }
    
    // 1) Generate datakey and partitionInfo
    val datakey:String = { 
      // Generate partition:= (1) (index, name) -> (2) (index, value)
      val partitionIndexWithName = row.table.partitionColumnID.zip(row.table.partitionColumnNames)
      val partitionIndexWithValue = partitionIndexWithName.map(
          column => (column._1, row.columns.get(column._2).get))
      val (key, partition) = KeyUtil.generateDataKey(row.table.id, partitionIndexWithValue)
      partitionInfo.append(partition)
      key
    }
    // 2) Execute pipelined command in each node
    //   From SRC := [RedisRelation]-RedisCluster(RedisConnection)
    //   To ADDB :=  [RedisStore]-RedisCluster(RedisConnection)
    val node = KeyUtil.getNodeForKey(redisCluster.nodes, datakey)
    
    // 3) Convert from data:String to data:List<String> (compatible with Java List type)
    // Because of Map structure, need to sort data
    var data = new ListBuffer[String]()
    row.table.columns.foreach {
      x =>
        // convert null to "null" string
        data += row.columns.getOrElse(x.name, "null")
    }
    // 4) Get pipeline and insert fpwrite command into pipeline
//    System.out.println("fpwrite "+datakey+" "+row.table.columnCount.toString + " " + partitionInfo.toString + " " + data.mkString(" "))
    val commandArgsObject = new CommandArgsObject(datakey, row.table.columnCount.toString,
                                                    partitionInfo.toString, data.toList.asJava)
    val pipeline = pipelinePool.get(node.redisConnection.host+":"+node.redisConnection.port.toString)
    pipeline.fpwrite(commandArgsObject)
  }
  
  def _calculateDurationSec(start: Double, end: Double): Double = {
    return (end - start) / 1000.0f;
  }
  
  def scan(
    table: RedisTable,
    location: String,
    datakeys: Array[String], // datakeys including partition key
    prunedColumns: Array[String]): Iterator[RedisRow] = {

    val _time_prepare_s = System.currentTimeMillis
    logDebug("[ADDB] scan function")
    val columnIndex = prunedColumns.map { 
      columnName => "" + (table.columns.map(_.name).indexOf(columnName) + 1)
				}
    val host = KeyUtil.returnHost(location)
    val port = KeyUtil.returnPort(location)
    val nodeIndex = redisCluster.checkNodes(host, port)
    val _time_prepare_e = System.currentTimeMillis
    logInfo(s"[ADDB] prepare time ${_calculateDurationSec(_time_prepare_e, _time_prepare_s)}")

  		val group_size = {
			   if (datakeys.size >= 10) 10
			   else 1
				}

    val _time_flatmapscan_s = System.currentTimeMillis 
    val values = datakeys.grouped(group_size).flatMap { datakeyGroup =>
      val __time_connection_s = System.currentTimeMillis
      val conn = redisCluster.nodes(nodeIndex).redisConnection.connect
      val pipeline = conn.pipelined()
      val __time_connection_e = System.currentTimeMillis
      logInfo(s"[ADDB] connection time ${_calculateDurationSec(__time_connection_s, __time_connection_e)}")
      
      val __time_execution_s = System.currentTimeMillis
      datakeyGroup.foreach { dataKey =>
        val commandArgsObject = new CommandArgsObject(dataKey,
          KeyUtil.retRequiredColumnIndice(table.id, table, prunedColumns))
        pipeline.fpscan(commandArgsObject)
      }
      val __time_execution_e = System.currentTimeMillis
      logInfo(s"[ADDB] scan execution ${_calculateDurationSec(__time_execution_s, __time_execution_e)}")
   
      val __time_pipsync_s = System.currentTimeMillis
      // TODO(totoro): Implements syncAndReturnAll to Future API.
      val results = pipeline.syncAndReturnAll.flatMap { x =>
        logDebug(s"[ADDB] values getClass: ${x.getClass.toString()}")
        // If errors occur, casting exception is called
        try {
          /* For getting String data, transform original(List[Object]) data
          List[Object] -> List[ArrayList[String]] -> Buffer[ArrayList[String]] -> Append each String */
          x.asInstanceOf[ArrayList[String]]
        } catch {
          case e: java.lang.ClassCastException => {
            logError(s"[ADDB] Scan Error: ${x.asInstanceOf[JedisClusterException]}")
            throw e
          }
        }
      }
      val __time_pipsync_e = System.currentTimeMillis
      logInfo(s"[ADDB] pip sync ${_calculateDurationSec(__time_pipsync_s, __time_pipsync_e)}")
      conn.close()
      results
    }
    .toArray

    val _time_flatmapscan_e = System.currentTimeMillis
    logInfo(s"[ADDb] flatmap scan ${_calculateDurationSec(_time_flatmapscan_s, _time_flatmapscan_e)}")

    val _time_remainjob_s = System.currentTimeMillis
    val result = {
      if (prunedColumns.length != 0) {
        values.grouped(prunedColumns.length).map { x =>
          val columns: Map[String, String] = prunedColumns.zip(x).toMap
          new RedisRow(table, columns)
        }
      }
      else {
        values.map { x => 
          val columns: Map[String, String] = Map(x->x)
          new RedisRow(table, columns)
        }.toIterator
      }
    }
    val _time_remainjob_e = System.currentTimeMillis
    logInfo(s"[ADDB] remain job ${_calculateDurationSec(_time_remainjob_s, _time_remainjob_e)}")

    result
  }
  
  def add(row: RedisRow): Unit = {
    throw new RuntimeException(s"Unsupported method on this mode")
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
}
