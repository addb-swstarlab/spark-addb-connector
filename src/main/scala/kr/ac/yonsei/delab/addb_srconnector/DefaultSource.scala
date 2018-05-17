package kr.ac.yonsei.delab.addb_srconnector

import scala.collection.JavaConversions._
//import java.util.HashMap
import scala.collection.immutable.HashMap

import org.apache.spark.sql.sources._
import org.apache.spark.sql.{SQLContext, SaveMode, DataFrame}
import org.apache.spark.sql.types.StructType

import kr.ac.yonsei.delab.addb_srconnector.ConfigurationConstants.{TABLE_KEY, INDICES_KEY, PARTITION_COLUMN_KEY}
import kr.ac.yonsei.delab.addb_srconnector.util.Logging

// When user defines relation by using SQL Statement,
// DefaultSource
class DefaultSource
  extends RelationProvider
  with SchemaRelationProvider
  with CreatableRelationProvider
  with DataSourceRegister with Logging{
  
  // DataSourceRegister
  override def shortName(): String = "addb"
  
  // Check OPTIONS := tableID, partitionInfo, indexInfo
  def checkOptions(configuration:Configuration, schema:StructType):Unit = {
		 // 1) Check table name
    try {
      val tableID = configuration.get(TABLE_KEY).toInt
    } catch {
      case e : NumberFormatException => throw new IllegalArgumentException(s"[ERROR] table option should be numeric.")
    }
    // 2) partition info
    // Partition can be multiple columns while OPTIONS must get 1 'partitions' key
    val partitionInfo = configuration.get(PARTITION_COLUMN_KEY).split(",").map(x => x.trim)
    // Check empty
    if (partitionInfo.isEmpty) {      
      throw new IllegalArgumentException( s"[ERROR] At least, one partition column is required" )
     }
    // Check proper partition column name
    val schemaColumns = schema.fieldNames
    partitionInfo.foreach { partitionColumn =>  
      if (!(schemaColumns.contains(partitionColumn))){
    	   throw new IllegalArgumentException( s"[ERROR] Mismatch between schema and partition column name" )
       }
     }
    // 3) index info
    
    
  }
  
  // RelationProvider := do not specify schema
  override def createRelation (sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    logInfo ( s"##[ADDB][DefaultSource-(Relation)] Please enter relation schema info!" )
//    logTrace ( s"Please enter relation schema info!" )
    createRelation(sqlContext, parameters, null)
  }
  // SchemaRelationProvider := specified schema by user
  override def createRelation (sqlContext: SQLContext, parameters: Map[String, String], schema:StructType): BaseRelation = {
    logInfo ( s"##[ADDB][DefaultSource-(SchemaRelation)]" )
    /*
     * test code
    // get schema Info
    val schemaFields = schema.fields;
    schemaFields.foreach { x => println(x.name + ", " + x.dataType) }
    println(schemaFields.length)
    // get parameters
    val auth = parameters.getOrElse("AUTO", "foobared");    
    // test zipWithIndex
    val cols = schema.fields.zipWithIndex
    cols.foreach(x => println(x._1.toString() + " " + x._2))
    logInfo ( s"##[ADDB][printAll zipWithIndex]" )
		*/    
    val param:HashMap[String, String] = HashMap(parameters.toSeq:_*)
    val configuration = Configuration(param)
    
    checkOptions(configuration, schema)
    
    val addbRelation = ADDBRelation(parameters, schema)(sqlContext)
    addbRelation.configure(configuration)
    addbRelation
  }
  // CreatableRelationProvider := When save DataFrame to data source
  // SaveMode => Overwrite, Append, ErrorIfExists, Ignore
  override def createRelation (sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    logInfo ( s"##[ADDB][DefaultSource-(creatableRelation)] Mode:= $mode Please enter relation schema info!" )
    createRelation(sqlContext, parameters, data.schema)
  } 
}