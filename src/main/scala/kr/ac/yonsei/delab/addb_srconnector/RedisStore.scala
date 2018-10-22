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
    logDebug(s"new String for Filter = " + retbuf.toString() +", "+ retbuf.toString.isEmpty)

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
    logInfo("[ADDB] add(INSERTION) function")
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
  
  def scan(
    table: RedisTable,
    location: String,
    datakeys: Array[String], // datakeys including partition key
    prunedColumns: Array[String]): Iterator[RedisRow] = {

    logDebug("[ADDB] scan function")
    val columnIndex = prunedColumns.map { 
      columnName =>
    	   "" + (table.columns.map(_.name).indexOf(columnName) + 1)
    }

    val host = KeyUtil.returnHost(location)
    val port = KeyUtil.returnPort(location)
    val nodeIndex = redisCluster.checkNodes(host, port)
    val conn = redisCluster.nodes(nodeIndex).redisConnection.connect
    val pipeline = conn.pipelined()

    datakeys.foreach {
      datakey =>
//      logInfo( s"[WONKI] : key in scan = $datakey | port = $port")
      val commandArgsObject = new CommandArgsObject(datakey,
                                                      KeyUtil.makeRequiredColumnIndice(table.id, table, prunedColumns))
    	 pipeline.fpscan(commandArgsObject)
    }
    // For getting String data, transform original(List[Object]) data
    // List[Object] -> List[ArrayList[String]] -> Buffer[ArrayList[String]] -> Append each String
    val values = {
      val buffer : ArrayBuffer[String] = ArrayBuffer[String]()
      val fpscanResult = pipeline.syncAndReturnAll.map{x =>
        logDebug(s"[ADDB] values getClass: ${x.getClass.toString()}")

        // If errors occur, casting exception is called
        try {
          x.asInstanceOf[ArrayList[String]]
        } catch {
          case e: java.lang.ClassCastException => {
            logError(s"[ADDB] Scan Error: ${x.asInstanceOf[JedisClusterException]}")
            throw e
          }
        }
      }
      fpscanResult.foreach { 
        arrayList => arrayList.foreach ( content => buffer+=content )
       }
      buffer
    }
//    values.foreach { x => logInfo(s"values: $x") }
    
    // For coping with count(*) case. (When prunedColumns is empty)
    val numRow = {
      if (prunedColumns.length != 0) values.length / prunedColumns.length
      else values.length // Avoid divideByZero
    }
    
    val columnList = {
      if (prunedColumns.length != 0) Stream.continually(prunedColumns).take(numRow).flatten.toArray
      else Stream.continually(table.columns(0).name).take(numRow).toArray // set first column
    }
//    columnList.foreach( x => logInfo(s"columnList $x"))
    val res = columnList.zip(values)

    conn.close

    val result = {
      if (prunedColumns.length != 0) {
    	  res.grouped(prunedColumns.length).map { 
    		  x =>
//        x.foreach(x => s"### result : ${x._1} , ${x._2}")
    		  val columns: Map[String, String] = x.toMap
    		  new RedisRow(table, columns)
    	  }
      } 
      else {
        res.map{
          x =>
            val columns: Map[String, String] = Map(x._1 -> x._2)
            new RedisRow(table, columns)
        }.toIterator
      }
    }
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
