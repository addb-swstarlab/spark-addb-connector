package kr.ac.yonsei.delab.addb_srconnector

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.collection.JavaConversions.mutableSeqAsJavaList
import scala.collection.JavaConverters._

import redis.clients.addb_jedis.Protocol
import redis.clients.addb_jedis.util.CommandArgsObject

import kr.ac.yonsei.delab.addb_srconnector.util.KeyUtil
import kr.ac.yonsei.delab.addb_srconnector.util.PartitionUtil
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

//class RedisRowBase(
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
  def getTablePartitions(
                          table: RedisTable, clearCachePeriod: Int, reloadPeriod: Int,
//                          extra: Any): Array[SourceInfo] = {
                        		  extra: Any): Unit = {

//    val tableInfo = handler.getTableInfo(table)
//    logInfo(s"Partitions for table: $tableInfo")
//
//    val stopWatch = new StopWatch()
//    stopWatch.start()
//
//    implicit val ec = ExecutionContext.global
//    val promise: Promise[Array[SourceInfo]] = Promise()
//    var sourceInfos: Array[SourceInfo] = Array[SourceInfo]()
//    try {
//      sessionManager.run { // throw Too many cluster redirections
//        session: Session => {
//          logInfo(s"[TJedisStore - getTablePartitions] session run")
//          val sessionUsageCount = sessionManager.getSessionUsage()
//          if (clearCachePeriod != SESSION_CLEAR_CACHE_PERIOD_DEFAULT && sessionUsageCount != 0 && sessionUsageCount % clearCachePeriod == 0) {
//            session.clearCache()
//            logInfo(s"session clearCache() with session usage: ${sessionUsageCount}, clear cache period ${clearCachePeriod}")
//          }
//          if (reloadPeriod != SESSION_RELOAD_PERIOD_DEFAULT && sessionUsageCount != 0 && sessionUsageCount % reloadPeriod == 0) {
//            session.clearCacheAndReload()
//            logInfo(s"session clearCacheAndReload() with session usage: ${sessionUsageCount}, reload period ${reloadPeriod}")
//          }
//          sessionManager.incrementSessionUsage()
//          sourceInfos = session.getSourceInfos(tableInfo, extra.asInstanceOf[FilterNode])
//          sourceInfos
//        }
//      }
//    } finally {
//      stopWatch.stop()
//      val elapsedTime = stopWatch.getInterval()
//      SR2GaugeSet.instance.recordTablePartitions(elapsedTime)
//      logInfo(s"getSourceInfos took: $elapsedTime, cnt: ${sourceInfos.size}")
//      //sourceInfos.foreach { source => logInfo(s"source info: ${source.toString}")}
//    }
  }

  /*
   * Add data to redis through jedis pipeline
   * Process INSERT(fpwrite) command according each node
   */
  def add(rows: Iterator[RedisRow]): Unit = {
    // 0) Convert iterator to array for generating datakey
    val rowForTableInfo = rows.toArray
    
    // 1) Generate datakey
    val keyRowPair:Map[String, RedisRow] = rowForTableInfo.map{ row => 
      // Generate partition:= (1) (index, name) -> (2) (index, value)
      val partitionIndexWithName = row.table.partitionColumnID.zip(row.table.partitionColumnNames)
      val partitionIndexWithValue = partitionIndexWithName.map(column => 
                               (column._1, row.columns.get(column._2).get))
      val dataKey = KeyUtil.generateDataKey(row.table.id, partitionIndexWithValue)
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
            logInfo(s"PartitionInfo: "+PartitionUtil.getPartitionInfo(row.table.partitionColumnID))
            // parameters: datakey, ColumnCount:String, partitionInfo, rowData
            val commandArgsObject = new CommandArgsObject(datakey, row.table.columnCount.toString, 
                 PartitionUtil.getPartitionInfo(row.table.partitionColumnID), data);
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
//    partitionIndices: Array[SourceInfo],
    prunedColumns: Array[String])
//    filter: FilterNode, query_task_limit: Int, redisRowType: Int, responseTimeout: Int): Iterator[RedisRowBase] 
    = {

//    logTrace(s"Source Info: $partitionIndices, pruned columns: $prunedColumns,  Filter: $filter")
//
//    val columnIndex = prunedColumns.map { columnName =>
//      "" + (table.columnNames.indexOf(columnName) + 1)
//    }
//    val columnIndexAsJavaList = mutableSeqAsJavaList(columnIndex)
//
//    val session = sessionManager.get();
//
//    // logInfo(s"# of source Infos: ${partitionIndices.length}")
//
//    val partitionSlices = partitionIndices.sliding(2, 2).toList
//
//    //    val flatMaps = Array.fill(partitionSlices.length)(Iterable[RedisRow]())
//    val flatMaps = scala.collection.mutable.ListBuffer[Iterable[RedisRowBase]]()
//
//    var totalRowCnt = 0
//    var sinfoGroupIndex = 0
//    var sinfoIndex = 0
//    val colCnt = table.columnNames.size;
//    val prunedColCnt = prunedColumns.size;
//    val taskRowCntMax = if (query_task_limit == QUERY_RESULT_TASK_ROW_CNT_LIMIT_DEFAULT) QUERY_RESULT_TASK_ROW_CNT_LIMIT_DEFAULT else query_task_limit * (colCnt / (if (prunedColCnt == 0) 1 else prunedColCnt))
//
//    logDebug(s"adjusted taskRowCntMax: ${taskRowCntMax}, with task row limit: ${query_task_limit}, " +
//      s"# of pruned cols: $prunedColCnt, # of total cols: $colCnt")
//
//    partitionSlices.foreach {
//      sourceInfos => {
//        sessionManager.run {
//          session: Session => {
//            sourceInfos.foreach {
//              sourceInfo => {
//                if (responseTimeout == QUERY_RESPONSE_TIMEOUT_DEFAULT) {
//                  session.prunedScan(sourceInfo, columnIndexAsJavaList, filter);
//                }
//                else {
//                  try {
//                    session.prunedScan(sourceInfo, columnIndexAsJavaList, filter, responseTimeout);
//                  }
//                  catch {
//                    case e: java.net.SocketException => {
//                      logInfo(s"prunedScan error with socket exception ${e.getMessage()}")
//                      throw e
//                    }
//                  }
//                }
//              }
//            }
//
//            val responses =
//              try {
//                if (responseTimeout == QUERY_RESPONSE_TIMEOUT_DEFAULT)
//                  JavaConversions.collectionAsScalaIterable(session.response());
//                else
//                  JavaConversions.collectionAsScalaIterable(session.responseWithTimeout(responseTimeout));
//              } catch {
//                case e: Exception => {
//                  logInfo(s"prunedScan get response error ${e.getMessage()}")
//                  throw e
//                }
//              }
//
//            logDebug(s"${partitionIndices.size} scan requested")
//
//            assert(sourceInfos.size == responses.size, s"Request: ${sourceInfos.size}, Response: ${responses.size}")
//            logDebug(s"Response: ${responses}")
//            val flatMap = responses.flatMap { response =>
//              logTrace(s"ScanResult: $response")
//              response match {
//                case result: ScanResult => {
//                  if (0 == result.getType()) {
//                    val nRow = result.getResult().size() / prunedColumns.size
//                    logTrace(s"Fetch $nRow row(s)")
//                    totalRowCnt += nRow
//                    collectionAsScalaIterable(result.getResult()).iterator.grouped(prunedColumns.size).map { values =>
//                      val columns = Map[String, String]()
//                      for (i <- 0 until prunedColumns.length) {
//                        columns.put(prunedColumns(i), if (values(i).equals("")) null else values(i))
//                      }
//                      val row = new RedisRowBase(columns)
//                      if (taskRowCntMax > 0 && totalRowCnt > taskRowCntMax)
//                        throw new UnsupportedQueryTypeException(s"totalRowCnt exceeds taskRowCntMax: ${taskRowCntMax}")
//                      row
//                    }
//                  } else {
//                    val count = result.getCount()
//                    logDebug(s"${sinfoGroupIndex},${sinfoIndex}th rowcount: $count")
//                    logTrace(s"Get type: $result.getType()")
//                    val map = Map[String, String]()
//                    val row = new RedisRowBase(map)
//                    totalRowCnt += count.toInt
//                    sinfoIndex = sinfoIndex + 1
//                    if (taskRowCntMax > 0 && totalRowCnt > taskRowCntMax)
//                      throw new UnsupportedQueryTypeException(s"totalRowCnt exceeds taskRowCntMax: ${taskRowCntMax}")
//                    Array.fill(count.toInt)(row).iterator
//                  }
//                }
//                case _ => {
//                  logError(s"Unknown type: $response")
//                  assert(false)
//                  Array().iterator
//                }
//              }
//            }
//            // flatMaps(sinfoGroupIndex) = flatMap
//            flatMaps += flatMap
//            sinfoGroupIndex = sinfoGroupIndex + 1
//            session.reset()
//          }
//        }
//      }
//    }
//
//    sinfoIndex = 0
//    val retFlatMap = flatMaps.flatMap {
//      flatMap => flatMap
//    }
//
//    logDebug(s"retFlatMap size: ${retFlatMap.size}, flashMaps size: ${flatMaps.size} totalRowCnt: ${totalRowCnt}")
//    retFlatMap.iterator
  }
}
