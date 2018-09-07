package kr.ac.yonsei.delab.addb_srconnector

import scala.collection.JavaConversions._
import scala.collection.immutable.ListMap
import scala.collection.mutable.Stack


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StructType, ByteType, ShortType, 
                      IntegerType, LongType, FloatType, DoubleType}

import redis.clients.addb_jedis.Protocol
import kr.ac.yonsei.delab.addb_srconnector.util.{Logging, KeyUtil}
import kr.ac.yonsei.delab.addb_srconnector.ColumnType.{NumericType, StringType}
import kr.ac.yonsei.delab.addb_srconnector.ConfigurationConstants.{TABLE_KEY, INDICES_KEY, PARTITION_COLUMN_KEY}
import kr.ac.yonsei.delab.addb_srconnector.rdd._

/* 
 * ADDB Relation class
 * After CREATE TABLE in SparkSQL, [DefaultSource]
 * When INSERT and SELECT statement are called, insert and buildScan function operate. [ADDBRelation]
 */
case class ADDBRelation (parameters: Map[String,String], 
                    schema: StructType) 
                   (@transient val sqlContext: SQLContext)
  extends BaseRelation 
  with TableScan
  with PrunedScan
  with PrunedFilteredScan
  with Configurable
  with InsertableRelation
  with Logging {
  
  def getRedisConfig( configuration: Configuration ): RedisConfig = {
    RedisConfigPool.get( configuration );
  }
  
  def buildRedisTable: RedisTable = {
    val tableID = configuration.get(TABLE_KEY).toInt
    // Check whether current table is in the RedisTableList
    if (RedisTableList.checkList(tableID)) {
      // Return stored RedisTable
      RedisTableList.list.get(tableID).get
    }
    else {
    	def buildNewRedisTable: RedisTable = {
    			val columns: ListMap[String, RedisColumn] = ListMap( schema.fields.map { 
    			  field=> // ListMap 타입
    			    ( field.name, new RedisColumn( field.name, field.dataType match { // column type 단순화. Column type은 RedisTable에 NumericType or StringType으로만 구분해놓음
    			      case _@ (ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType) => NumericType
    			      case _ => StringType
    			        } 
    			     ) ) 
    			  }.toSeq:_* )
    			// ex) { col1 -> RedisColumn(col1, string) }
//    			logInfo( s"Columns: $columns" )

      		// Partition can be multiple columns while OPTIONS must get 1 'partitions' key
      		val partitionColumnNames = configuration.get( PARTITION_COLUMN_KEY ).split(",").map(x => x.trim)

      		logInfo( s"Index is not implemented yet.." )
      		RedisTable(tableID, columns.values.toArray, partitionColumnNames);
    		}
    	// Build new RedisTable and insert it into RedisTableList
    	val newRedisTable = buildNewRedisTable
    	RedisTableList.insertTableList(tableID, newRedisTable)
    	newRedisTable
    }
  }



  /** ADDB
   *  WonKi Choi 2018-05-17
   *  implementation for Scan operation in SparkSQL
   *  build scan for returning RDD object.
   */
  
  // TableScan

  override def buildScan: RDD[Row] = {
    logInfo(s"buildScan: tableScan")
    buildScan(schema.fields.map( field => field.name ) )
  }

  // PrunedScan
  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    logInfo(s"buildScan: prunedScan")
    buildScan( requiredColumns, Array())
  }

  // PrunedFilteredScan
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
//    logInfo(s"buildScan: prunedFilterScan")
//    requiredColumns.foreach(x => logInfo(s"requiredColumns : $x"))
//    logInfo(s"filter size : ${filters.size}")
//    filters.foreach(x => logInfo(s"filters : $x"))

    val redisConfig = getRedisConfig( configuration )
    val redisTable = buildRedisTable
    val rdd = new ADDBRDD(sqlContext.sparkContext, redisConfig, redisTable, requiredColumns, filters)
    new RedisRDDAdaptor(rdd, requiredColumns.map{ columnName=> schema(columnName)}, filters, schema)
  }
   
  // InsertableRelation
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    logInfo(s"##[JH] insert function")
//    logInfo(s"[dataFrame]"+data.rdd.partitions.length) // return 8
    // check OVERWRITE command
    if (overwrite) {
      logInfo(s"Do not implement overwrite command. Thus, operate only append")
    }

    // insert RedisRow(RedisTable+Column) into RedisStore
    val redisTable = buildRedisTable
    val columnsWithIndex = schema.fields.zipWithIndex // ( (field1:StructField, 0) , (field2, 1) , (field3, 2) ... )        
    val rowRDD = data.rdd
    
    val redisConfig = getRedisConfig( configuration ) // get current ADDBRelation RedisConfig
    val redisStore = redisConfig.getRedisStore(); // ADDBRelationRedisConfig->RedisConfig->RedisStore

    		
    // call pipeline function based on dataKey
    // *[Critical]* Be executed in each node
    try {
    	rowRDD.foreachPartition { 
    		partition => // partition:Iterator[Row]
    		
    		logInfo(s"##[JH] start partition loop")
    		
    		// make each pipeline
    		val retainingJedisPool = new RetainingJedisPool()
    		val pipelinePool = new PipelinePool()
    		redisStore.redisCluster.nodes.foreach{
    			node =>
    			  val jedis = retainingJedisPool.add(node)
      			pipelinePool.add(node.redisConnection.host+":"+node.redisConnection.port.toString, jedis)
    		}

    		// Since datakey and partitionInfo are duplicated,
    		// make once only
//    		var datakey = new StringBuilder
//    		var partitionInfo = new StringBuilder
    		
    		// fpwrite all rows
    		partition.foreach{ 
    			
    			row => // row:Iterator[Row]
    			
    			  val columns = columnsWithIndex.map{ 
    				  pair=>
    				    val columnValue = row.get(pair._2) // Get column from existing row
    				    if ( columnValue == null ) {
    					    ( pair._1.name, null )
    				    } else {
    					    ( pair._1.name, columnValue.toString() )
    				    }
    			  }.toMap
//    			if (datakey.size == 0 && partitionInfo.size == 0) {
//    					// Generate partition:= (1) (index, name) -> (2) (index, value)
//    					val partitionIndexWithName = redisTable.partitionColumnID.zip(redisTable.partitionColumnNames)
//    					val partitionIndexWithValue = partitionIndexWithName.map{
//    					    	column => (column._1, columns.get(column._2).get)}
//    					val (key, partition) = KeyUtil.generateDataKey(redisTable.id, partitionIndexWithValue)
//    					partitionInfo.append(partition)
//    					datakey.append(key)
//    			}
//    			redisStore.add(RedisRow(redisTable, columns), pipelinePool, datakey.toString, partitionInfo.toString)
    			  redisStore.add(RedisRow(redisTable, columns), pipelinePool)
    		}
    		// synchronize all pipeline
    		// close all jedis connection
    		redisStore.redisCluster.nodes.foreach{
    			node =>
      			val jedis = retainingJedisPool.get(node)
      			val pipeline = pipelinePool.get(node.redisConnection.host+":"+node.redisConnection.port.toString)
      			pipeline.sync
    		  	jedis.close
    		}
    	}
    } catch {
      case e : Exception => throw e
//      case _ : Throwable => logError(s"## ADDB Insert Error!")
    } finally {
//          
    }    
   }
}