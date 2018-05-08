package kr.ac.yonsei.delab.addb_srconnector

import kr.ac.yonsei.delab.addb_srconnector.util.Logging
import scala.collection.immutable.HashMap

trait Configurable 
  extends Logging{
  var configuration: Configuration = _
  
  def configure(conf:Configuration):Unit = {
    this.configuration = conf
    logInfo(s"$conf is configured")
  }
}

case class Configuration (
    parameters:HashMap[String,Any])
  extends Serializable
  with Logging {
  // Since options are already checked, do not change to default value  
  def get(key:String): String = {
//    parameters.getOrElse(name, defaultValue)
    parameters.get(key).get.toString
  }
  def getOrElse(key:String, defaultValue:String):String = {
    if ( parameters.get(key).isEmpty && defaultValue == null ) {
      null
    } else {
      parameters.getOrElse(key, defaultValue).toString
    }
  }
  def gets(key:String):Array[String] = {
    parameters.map(_._2.toString()).toArray
  }
}

object ConfigurationConstants {  
  val TABLE_KEY = "table"
  val INDICES_KEY = "indices"
  val PARTITION_COLUMN_KEY = "partition"
  
}