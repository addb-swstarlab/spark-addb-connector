package kr.ac.yonsei.delab.addb_srconnector

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import redis.clients.addb_jedis.{Jedis, Pipeline}

/*
	In Spark Cluster mode, making all pipeline object in each node is better than serialization
 */
class PipelinePool {
  @transient private lazy val pipelinePools: ConcurrentHashMap[String, Pipeline] = new ConcurrentHashMap[String, Pipeline]()
  def add(hostAndPort:String, jedis:Jedis) = {
		  pipelinePools.getOrElseUpdate(hostAndPort, jedis.pipelined)
  }
  def get(hostAndPort:String):Pipeline = {
		  pipelinePools.get(hostAndPort)
  }
}
class RetainingJedisPool {
	  @transient private lazy val jedisPools: ConcurrentHashMap[RedisNode, Jedis] = new ConcurrentHashMap[RedisNode, Jedis]()  
	  def add(redisNode:RedisNode):Jedis = {
	    jedisPools.getOrElseUpdate(redisNode, redisNode.connect)
	  }
	  def get(redisNode:RedisNode):Jedis = {
	    jedisPools.get(redisNode)
	  }
}