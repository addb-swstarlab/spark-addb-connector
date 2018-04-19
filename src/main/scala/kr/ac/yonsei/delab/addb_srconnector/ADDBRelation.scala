package kr.ac.yonsei.delab.addb_srconnector

import scala.collection.JavaConversions._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

import redis.clients.addb_jedis.Protocol
import kr.ac.yonsei.delab.addb_srconnector.util.Logging

case class ADDBRelation (parameters: Map[String,String], 
                    userSchema: StructType) 
                   (@transient val sqlContext: SQLContext)
  extends BaseRelation 
//  with TableScan
//  with PrunedScan
//  with PrunedFilteredScan
  with InsertableRelation
  with Logging {
  
   val redisConfig: RedisConfig = {
    new RedisConfig({
        if ((parameters.keySet & Set("host", "port", "auth", "dbNum", "timeout")).size == 0) {
          new RedisConnection(sqlContext.sparkContext.getConf)
        } else {
          val host = parameters.getOrElse("host", Protocol.DEFAULT_HOST)
          val port = parameters.getOrElse("port", Protocol.DEFAULT_PORT.toString).toInt
          val auth = parameters.getOrElse("auth", "foobared")
          val dbNum = parameters.getOrElse("dbNum", Protocol.DEFAULT_DATABASE.toString).toInt
          val timeout = parameters.getOrElse("timeout", Protocol.DEFAULT_TIMEOUT.toString).toInt
          new RedisConnection(host, port, auth, dbNum, timeout)
        }
      }
    )
  }
  val schema = userSchema
  
  // TableScan
//  override def buildScan: RDD[Row] = {
  def buildScan: Unit = {
    logInfo(s"##[ADDB][ADDBRelation-(buildScan)] Command occurs")
//    logTrace(s"ADDBRelation-buildScan Command occurs")
    parameters.foreach(p => println("Parameter key=" + p._1 + ", value=" + p._2))
    
  }

  // PrunedScan
//  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
//    
//  }

  // PrunedFilteredScan
//  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
//    
//  }
  
  // InsertableRelation
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    logInfo(s"##[ADDB][ADDBRelation-(insert)] Command occurs")
//    logTrace(s"ADDBRelation-insert Command occurs")
    if (overwrite) {
      // overwrite old data to new data
    } else {
      // append to old data
    }
  }
}