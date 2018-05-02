package kr.ac.yonsei.delab.addb_srconnector

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object ColumnType
extends Enumeration {
  type ColumnType = Value
  val StringType = Value( "String" )
  val NumericType = Value( "Numeric" )
}

//import ColumnType._
case class RedisColumn(
  val name: String,
  val columnType: ColumnType.Value ) { }

case class RedisTable (
    val name: String,
    val columns: Array[RedisColumn],
//  val indices: Array[RedisIndex],
    val partitionColumnNames: Array[String]) {
//  val scoreName: String) {
//	def buildTable():RedisTable = {
//	  this
//	}
}

//class RedisRowBase(
//  val columns: Map[String, String] )
//  extends Serializable {
//}

case class RedisRow(
		val table: RedisTable,
//  override val columns: Map[String, String])
		val columns: Map[String, String])
//  extends RedisRowBase(columns) {
extends Serializable {
	
}

class RedisStore
  (val redisConfig:RedisConfig)
  extends Configurable {
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

  def add(rows: Iterator[RedisRow]): Unit = {
//    sessionManager.start()
    try {
      rows.foreach { row => add(row) }
    } finally {
//      sessionManager.end()
    }
  }

  def add(
           row: RedisRow
         ): Unit = {
         
//    val sourceInfo = handler.rowIdOf(row)
//    sessionManager.run { session: Session =>
//      session.nvwrite(sourceInfo, mutableSeqAsJavaList(row.table.columnNames.map { columnName => row.columns(columnName) }))
//    }
  }

  def get(
           key: String
         ): Iterator[RedisRow] = {
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
