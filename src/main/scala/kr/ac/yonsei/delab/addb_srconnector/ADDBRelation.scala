package kr.ac.yonsei.delab.addb_srconnector

import scala.collection.JavaConversions._
import scala.collection.immutable.ListMap


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StructType, ByteType, ShortType, 
                      IntegerType, LongType, FloatType, DoubleType}

import redis.clients.addb_jedis.Protocol
import kr.ac.yonsei.delab.addb_srconnector.util.Logging
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
    // check whether current table is in the RedisTableList
    if (RedisTableList.checkList(tableID)) {
      // return stored RedisTable
      RedisTableList.list.get(tableID).get
    }
    else {
    	def buildNewRedisTable: RedisTable = {
    			val columns: ListMap[String, RedisColumn] = ListMap( schema.fields.map{ field=> // ListMap 타입
    			( field.name, new RedisColumn( field.name, field.dataType match { // column type 단순화. Column type은 RedisTable에 NumericType or StringType으로만 구분해놓음
    			case _@ (ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType) => NumericType
    			case _ => StringType
    			} ) ) }.toSeq:_* )
    					// ex) { col1 -> RedisColumn(col1, string) }
    					logInfo( s"Columns: $columns" )

    					// indexColumn, partitionColumn, scoreColumnName을 configuration으로 부터 가져옴
    					//    val indexColumnInformations = configuration.gets( INDEX_COLUMNS_KEY )
    					
    					// Partition can be multiple columns while OPTIONS must get 1 'partitions' key
    					val partitionColumnNames = configuration.get( PARTITION_COLUMN_KEY ).split(",").map(x => x.trim)

    					// redis Index information을 토대로 RedisIndex객체 생성
    					// ~~(##) 와 같은 형식에서
    					// indexName = ~~
    					// indexTypeString = ## (uppercase)
    					// ex)  testIndex(EQUALTYPE)
    					//    val indices = indexColumnInformations.map{ info=>
    					//      val typeStartIndex = info.indexOf( '(' )
    					//      val typeEndIndex = info.indexOf( ')' )
    					//      val ( indexName, indexTypeString ) = if ( 0 <= typeStartIndex && typeStartIndex < typeEndIndex ) {
    					//        ( info.substring( 0, typeStartIndex ), info.substring( typeStartIndex + 1, typeEndIndex ).map(_.toUpper).trim() )
    					//      } else {
    					//        ( info, INDEX_TYPE_DEFAULT ) // INDEX_TYPE_DEFAULT = EQUAL
    					//      }
    					// RedisColumn, IndexType(EQUAL|RANGE|PARTUK)
    					// columns(indexName) -> 해당 indexName을 갖는 column 찾아서 redisColumn객체 생성
    					//      new RedisIndex( columns( indexName ) , withName( indexTypeString ) )
    					//    }
    					//    logInfo( s"Index information: $indices" )
    					logInfo( s"Index is not implemented yet.." )
    					RedisTable(tableID, columns.values.toArray, partitionColumnNames);
      }
      // build new RedisTable and insert it into RedisTableList
      val newRedisTable = buildNewRedisTable
      RedisTableList.insertTableList(tableID, newRedisTable)
      newRedisTable
    }
  }



  /** ADDB
   *  WonKi Choi 2018-05-17
   *  implementation for Scan operation in SparkSQL
   *  build scan for retuning RDD object.
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
    logInfo(s"buildScan: prunedFilterScan")
    requiredColumns.foreach(x => logInfo(s"requiredColumns : $x"))
    logInfo(s"filter size : ${filters.size}")
    filters.foreach(x => logInfo(s"filters : $x"))
    val redisConfig = getRedisConfig( configuration )
    val redisTable = buildRedisTable
    val rdd = new ADDBRDD(sqlContext.sparkContext, redisConfig, redisTable, requiredColumns, filters)
    new RedisRDDAdaptor(rdd, requiredColumns.map{ columnName=> schema(columnName)}, filters, schema)
  }
  
  // InsertableRelation
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    logInfo(s"##[ADDB][ADDBRelation-(insert)] Command occurs")
//    logInfo(s"[dataFrame]"+data.rdd.partitions.length) // return 8
    // check OVERWRITE command
    if (overwrite) {
      logInfo(s"Do not implement overwrite command. Thus, operate only append")
    }
    // deal with RDD[Row]
    val rowRDD = data.rdd
    // make RedisTable
    val redisTable = buildRedisTable
    // insert RedisRow(RedisTable+Column) into RedisStore
    rowRDD.foreachPartition { partition => // rdd마다. 즉, Row를 배분받은 각 파티션 마다. partition:Iterator[Row]
      val redisConfig = getRedisConfig( configuration ) // get current ADDBRelation RedisConfig
      val redisStore = redisConfig.getRedisStore(); // ADDBRelationRedisConfig->RedisConfig->RedisStore
      val columnsWithIndex = schema.fields.zipWithIndex // ( (field1:StructField, 0) , (field2, 1) , (field3, 2) ... )
      try {
        val redisRow = partition.map{ row =>
          val columns = columnsWithIndex.map{ pair=>
            val columnValue = row.get(pair._2) // 기존의 row에서 index 위치를 활용하여 값을 가져온다.
            if ( columnValue == null ) {
              ( pair._1.name, null )
            } else {
              ( pair._1.name, columnValue.toString() )
              }
          }.toMap
          RedisRow(redisTable, columns)
          }
        redisStore.add(redisRow)
      } finally {
//      	redisStore.sessionManager.end()
      }
    }
  }
}