package kr.ac.yonsei.delab.addb_srconnector

import kr.ac.yonsei.delab.addb_srconnector.util.Logging
import scala.collection.immutable.HashMap
import kr.ac.yonsei.delab.addb_srconnector.ConfigurationConstants.{TABLE_KEY, INDICES_KEY, PARTITION_COLUMN_KEY}

/*
 *  RedisStore, ADDBRelation is configurable
 *  Configuration distinguish each RedisStore / each ADDBRelation
 */
trait Configurable 
  extends Logging{
  var configuration: Configuration = _
  
  def configure(conf:Configuration):Unit = {
    this.configuration = conf
    logInfo(s"[ADDB] $conf is configured")
  }
}

/*
 * Configuration class
 * parameters := From CREATE TABLE OPTIONS
 */
case class Configuration (
    parameters:HashMap[String,String])
  extends Serializable
  with Logging {
  // Since options are already checked in createRelation function, do not change to default value  
  def get(key:String): String = {
    parameters.get(key).get.toString
  }
  def getOrElse(key:String, defaultValue:String):String = {
    if ( parameters.get(key).isEmpty && defaultValue == null ) {
      null
    } else {
      parameters.getOrElse(key, defaultValue).toString
    }
  }
}

object ConfigurationConstants {  
  val TABLE_KEY = "table"
  val INDICES_KEY = "indices"
  val PARTITION_COLUMN_KEY = "partitions"
}