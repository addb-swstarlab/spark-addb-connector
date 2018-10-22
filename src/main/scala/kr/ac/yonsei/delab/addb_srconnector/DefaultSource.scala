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
      case e : NumberFormatException => throw new IllegalArgumentException(s"[ADDB][ERROR] table option should be numeric.")
    }
    // 2) partition info
    // Partition can be multiple columns 
    val partitionInfo = configuration.get(PARTITION_COLUMN_KEY).split(",").map(x => x.trim)
    // Check empty
    if (partitionInfo.isEmpty) {      
      throw new IllegalArgumentException( s"[ADDB][ERROR] At least, one partition column is required" )
     }
    // Check whether partition column names are proper
    val schemaColumns = schema.fieldNames
    partitionInfo.foreach { 
      partitionColumn =>  
        if (!(schemaColumns.contains(partitionColumn))) {
          throw new IllegalArgumentException( s"[ADDB][ERROR] Mismatch between schema and partition column name" )
         }
     }
    // TO DO, index
    // 3) index info
    
    
  }
  
  // RelationProvider := do not specify schema
  override def createRelation (sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }
  // SchemaRelationProvider := specified schema by user
  override def createRelation (sqlContext: SQLContext, parameters: Map[String, String], schema:StructType): BaseRelation = {
//    logInfo ( s"##[ADDB] createRelation with user schema" )
    
    // Check user schema because addb does not provide schema-inference feature
    if (schema == null) {
      throw new IllegalArgumentException( s"[ADDB][ERROR] No schema. Please enter relation schema!" )
    }

    // Set configuration based on parameters
    val param:HashMap[String, String] = HashMap(parameters.toSeq:_*)
    val configuration = Configuration(param)
    
    checkOptions(configuration, schema)
    
    // Return and store addbRelation about create relation
    val addbRelation = ADDBRelation(parameters, schema)(sqlContext)
    addbRelation.configure(configuration)
    addbRelation
  }
  // CreatableRelationProvider := When save DataFrame to data source
  // SaveMode => Overwrite, Append, ErrorIfExists, Ignore
  override def createRelation (sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    createRelation(sqlContext, parameters, data.schema)
  } 
}