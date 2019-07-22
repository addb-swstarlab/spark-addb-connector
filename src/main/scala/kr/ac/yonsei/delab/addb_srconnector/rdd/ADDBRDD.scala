package kr.ac.yonsei.delab.addb_srconnector.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext
import org.apache.spark.SparkContext
import org.apache.spark.Partition
import scala.math.BigDecimal
import scala.reflect.ClassTag
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import kr.ac.yonsei.delab.addb_srconnector._
import kr.ac.yonsei.delab.addb_srconnector.partition._
import kr.ac.yonsei.delab.addb_srconnector.util
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import java.text.NumberFormat
import java.sql.Date

class ADDBRDD (
    @transient val sc: SparkContext,
    val redisConfig: RedisConfig,
    val redisTable: RedisTable,
    val requiredColumns: Array[String],
    val filter: Array[Filter]
    ) extends RDD[RedisRow] (sc, Seq.empty)
  {
  
  override protected def getPreferredLocations(split: Partition): Seq[String] = {
//    logInfo( s"[WONKI] : getPreferredLocations called ${split.asInstanceOf[RedisPartition].location}")
    Seq(split.asInstanceOf[RedisPartition].location)
  }
  
  override protected def getPartitions: Array[Partition] = {
    logDebug( s"[ADDB] getPartitions called")
    val redisStore = redisConfig.getRedisStore()
    val sourceinfos = redisStore.getTablePartitions(redisTable, filter) // get partition key
    var i = 0
    sourceinfos.map { mem =>
      val loc = mem._1
      logDebug( s"[ADDB] : getPartitions mem 1 : ${mem._1}")
      val sources : Array[String] = mem._2
      val size = mem._2.size
      
      var res = new ArrayBuffer[Partition]
      logDebug(s"[ADDB] Total size: ${mem._2.size}")
      var partitioningSize = {
        if (size>=10) size/10
        else 1
      }
     mem._2.grouped(partitioningSize).foreach { 
        x => 
         res += new RedisPartition(i, redisConfig, loc, x) 
         i+= 1
      } 
//      sources.foreach { x => logInfo(s"RedisPartition-Partition : $x") }
//      logInfo( s"[WONKI] : getPartitions mem 2 : ${mem._2}")
//      val partition = new RedisPartition(i, redisConfig, loc, sources);
//      i += 1
//      partition
      res.toArray
    }.flatten.toArray // (RedisPartition1 , RedisPartition2, RedisPartition3)
    // TO DO, Need to balance (partition-node)
  }
  
  // Each RedisPartition from getPartitions is adapted to compute()
  // Thus, scan is called by each RedisPartitions
  override def compute(split: Partition, context: TaskContext) : Iterator[RedisRow] = {
//    logInfo( s"[WONKI] : compute called")
    val partition = split.asInstanceOf[RedisPartition]
//    logInfo( s"[WONKI] : partition : ${partition.index}")
    val redisStore = redisConfig.getRedisStore()
    redisStore.scan(redisTable, partition.location, partition.partition, requiredColumns)
  }  
}

// Convert RDD[RedisRow] to RDD[Row] (DataFrame)
class RedisRDDAdaptor(
  val prev: RDD[RedisRow],
  val requiredColumns: Array[StructField],
  val filters: Array[Filter],
  val schema: org.apache.spark.sql.types.StructType
) extends RDD[Row]( prev ) {

  def castToTarget(value: String, dataType: DataType) = {
    dataType match {
      case _: IntegerType => value.toInt
      case _: DoubleType => value.toDouble
      case _: StringType => value.toString
      case _: DecimalType => BigDecimal(value.toDouble)
      case _: DateType => Date.valueOf(value)
      case _ => value.toString
    }
  }

  override def getPartitions: Array[Partition] = prev.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    prev.compute(split, context).map {  // call ADDBRDD.compute
      redisRow =>
        val columns: Array[Any] = requiredColumns.map { column =>
          val value = redisRow.columns.getOrElse(column.name, null)
          castToTarget(value, column.dataType)
        }
        val row = Row.fromSeq(columns.toSeq)
        row
    }
  }
}

