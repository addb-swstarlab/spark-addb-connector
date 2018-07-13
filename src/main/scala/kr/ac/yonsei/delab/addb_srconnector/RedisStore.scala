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
  val columnNameWithIndex = columns.map(_.name).zip(Stream from 1) // index sorted Array
  val columnNameWithID = columns.map(_.name).zip(Stream from 1).toMap // from index 1. not sorted
  val partitionColumnID:Array[Int] = partitionColumnNames.map(
      columnName => columnNameWithID(columnName)).toArray
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
//    		logInfo(s"Insert into RedisTableList")
  }

  def checkList(tableID: Int):Boolean = {
    if (list.size == 0) {
//    	 logInfo(s"RedisTableList is empty")
    	 false
    } else if (list.get(tableID) == None) {
//    	 logInfo(s"RedisTable is not in RedisTableList ")
    	 false
    } else {
//     	 logInfo(s"RedisTable is in RedisTableList ")
      true
    }
  }
  
  def getTableColumnWithIndex(tableID: Int):Map[String, Int] = {
    var res = list.get(tableID)
    if (res == None) {
      throw new NoSuchElementException(s"[Error] There is no corresponding RedisTable...")
    }
    res.get.columnNameWithID
  }
}
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

    // Make filter
    var retbuf = new StringBuilder
    filter.foreach { 
      x =>
        var stack = new Stack[String]
        Filters.makeFilterString(x, stack, table.id)
        while (!stack.isEmpty) {
          retbuf.append(stack.pop())
         }
      retbuf.append("$")
     }
    logInfo(s"new String for Filter = " + retbuf.toString() +", "+ retbuf.toString.isEmpty)

    val ret_scala : ArrayBuffer[String] = ArrayBuffer[String]()    
    redisCluster.nodes.foreach{
        x =>
          val conn = x.connect()
          conn.metakeys(metaKey, retbuf.toString).foreach (
            x => ret_scala += KeyUtil.getPartitionFromMeta(x) )
          conn.close()      
    }
        
//    logInfo( s"[WONKI] : ret_scala: $ret_scala" )

    // Spark partitioning := partition keys with corresponding port
    val partitioning = KeyUtil.groupKeysByNode(redisCluster.nodes, KeyUtil.generateDataKey(table.id, ret_scala.toArray))
    partitioning.foreach {
      x => 
        logInfo( s"[WONKI] : partitioning host-port : $x._1  partition : $x._2")
    }
    partitioning
  }

  /*
   * Add data to redis through jedis pipeline
   * Process INSERT(fpwrite) command according each node
   */
  def add(rows: Iterator[RedisRow]): Unit = {
//    logInfo(s"### Add command occurs")
    var partitionInfo = ""
    // 0) Convert iterator to array for generating datakey
    val rowForTableInfo = rows.toArray
    
//    rowForTableInfo.foreach { x => logInfo(s"rowForTableInfo: ${x.columns}") }
    
    // 1) Generate datakey
    val keyRowPair:Array[(String, RedisRow)] = rowForTableInfo.map{ 
      row => 
      // Generate partition:= (1) (index, name) -> (2) (index, value)
      val partitionIndexWithName = row.table.partitionColumnID.zip(row.table.partitionColumnNames)
      val partitionIndexWithValue = partitionIndexWithName.map(
          column => (column._1, row.columns.get(column._2).get))
      val (dataKey, partition) = KeyUtil.generateDataKey(row.table.id, partitionIndexWithValue)
      // set PartitionInfo
      partitionInfo = partition
//      row.columns.foreach(x => logInfo(s"#### : ${x._1}, ${x._2}"))
      (dataKey, row)
    }
    // 2) Execute pipelined command in each node
    //   From SRC := [RedisRelation]-RedisCluster(RedisConnection)
    //   To ADDB :=  [RedisStore]-RedisCluster(RedisConnection)
    KeyUtil.groupKeysByNode(redisCluster.nodes, keyRowPair.toMap.keysIterator).foreach{
      case(node, datakeys) => {
        val conn = node.connect
        val pipeline = conn.pipelined
        datakeys.foreach{
          datakey => {
            keyRowPair.filter(pair => pair._1 == datakey).foreach {
              pair =>
                val row:RedisRow = pair._2
                // Convert from data:String to data:List<String> (compatible with Java List type)
                // Because of Map structure, need to sort data
                var data = new ListBuffer[String]()
                row.table.columnNameWithIndex.foreach {
            	  x =>        	    
              	  val value = row.columns.getOrElse(x._1, null)
//              	  println(s"value: $value")
              	  if (!(value == null)) {
            		  data += value
            	    }
                }
              // parameters: datakey, ColumnCount:String, partitionInfo, rowData
//              data.toList.foreach(x => logInfo(s"data: $x"))
              val commandArgsObject = new CommandArgsObject(datakey, row.table.columnCount.toString, 
            		  partitionInfo, data.toList.asJava)
              pipeline.fpwrite(commandArgsObject)
              }
    				}
          }
        pipeline.sync
        conn.close
        }
     }
  }
  def add(row: RedisRow): Unit = {
  }
  def scan(
    table: RedisTable,
    location: String,
    datakeys: Array[String], // datakeys including partition key
    prunedColumns: Array[String]): Iterator[RedisRow] = {

    val columnIndex = prunedColumns.map { 
      columnName =>
    	   "" + (table.columns.map(_.name).indexOf(columnName) + 1)
    }
//    println(s"${redisCluster.nodes}")
//    redisCluster.nodes.foreach { node => 
//      println("port: "+node.redisConnection.port +", idx:" + node.idx +", " + node.startSlot +"~"+node.endSlot)
//    }

    val port = KeyUtil.returnPort(location)
    val nodeIndex = redisCluster.checkNodes(port)
    val conn = redisCluster.nodes(nodeIndex).redisConnection.connect
    val pipeline = conn.pipelined()

    datakeys.foreach {
      datakey =>
//      logInfo( s"[WONKI] : key in scan = $datakey | port = $port")
      val commandArgsObject = new CommandArgsObject(datakey, KeyUtil.makeRequiredColumnIndice(table.id, prunedColumns))
    	 pipeline.fpscan(commandArgsObject)
    }
    // For getting String data, transform original(List[Object]) data
    // List[Object] -> List[ArrayList[String]] -> Buffer[ArrayList[String]] -> Append each String
    val values = {
      val buffer : ArrayBuffer[String] = ArrayBuffer[String]()
      val fpscanResult = pipeline.syncAndReturnAll.map(_.asInstanceOf[ArrayList[String]])
//      logInfo(s"fpscanResult = $fpscanResult")
//      logInfo(s"fpscanResult size = ${fpscanResult.size}")
      fpscanResult.foreach { 
        arrayList => arrayList.foreach ( content => buffer+=content )
       }
      buffer
    }
//    values.foreach { x => logInfo(s"values: $x") }
    val numRow = values.length / prunedColumns.length
//    println("value length = " + values.length)
//    println("pruned length = " + prunedColumns.length)
//    println("numRow = " + numRow)
    val columnList = Stream.continually(prunedColumns).take(numRow).flatten.toArray
//    columnList.foreach( x => logInfo(s"columnList $x"))
    val res = columnList.zip(values)

    conn.close

    val result = res.grouped(prunedColumns.length).map { 
      x =>
        x.foreach(x => s"### result : ${x._1} , ${x._2}")
        val columns: Map[String, String] = x.toMap
        new RedisRow(table, columns)
    }
   result
  }
  
//  def add(row: RedisRow): Unit = {
//    throw new RuntimeException(s"Unsupported method on this mode")
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
}
