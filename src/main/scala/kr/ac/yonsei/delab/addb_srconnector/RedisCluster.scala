package kr.ac.yonsei.delab.addb_srconnector

import scala.collection.JavaConversions._
import redis.clients.addb_jedis.util.SafeEncoder
import kr.ac.yonsei.delab.addb_srconnector.util.Logging

class RedisCluster(val host: RedisConnection) 
  extends Serializable
  with Logging {
  
  val hosts = getHosts(host)
  
	private def checkClusterEnabled(redisConnection: RedisConnection): Boolean = {
		val conn = redisConnection.connect()
		val info = conn.info.split("\n")
		val version = info.filter(_.contains("redis_version:"))(0)
		val clusterEnable = info.filter(_.contains("cluster_enabled:"))
		val mainVersion = version.substring(14, version.indexOf(".")).toInt
		val res = mainVersion>2 && clusterEnable.length>0 && clusterEnable(0).contains("1")
		conn.close
		res
  }
	private def getClusterNodes(redisConnection: RedisConnection): Array[RedisNode] = {
    val conn = redisConnection.connect()
    val res = conn.clusterSlots().flatMap {
      slotInfoObj => {
        val slotInfo = slotInfoObj.asInstanceOf[java.util.List[java.lang.Object]]
        val sPos = slotInfo.get(0).toString.toInt
        val ePos = slotInfo.get(1).toString.toInt
        /*
         * We will get all the nodes with the slots range [sPos, ePos],
         * and create RedisNode for each nodes, the total field of all
         * RedisNode are the number of the nodes whose slots range is
         * as above, and the idx field is just an index for each node
         * which will be used for adding support for slaves and so on.
         * And the idx of a master is always 0, we rely on this fact to
         * filter master.
         */
        (0 until (slotInfo.size - 2)).map(i => {
        	val node = slotInfo(i + 2).asInstanceOf[java.util.List[java.lang.Object]]
        			val host = SafeEncoder.encode(node.get(0).asInstanceOf[Array[scala.Byte]])
        			val port = node.get(1).toString.toInt
        			RedisNode(new RedisConnection(host, port, redisConnection.auth, 
        					redisConnection.dbNum, redisConnection.timeout),
        					sPos, ePos, i, slotInfo.size - 2)
        })
      }
    }.toArray
    conn.close()
    res
  }
  def getNodes(redisConnection: RedisConnection): Array[RedisNode] = { 
    if (!checkClusterEnabled(redisConnection)) {
    	logInfo(s"ADDB must be operated in cluster modes")
    	// Should add throw exception
    }
    getClusterNodes(redisConnection)
  }
	def getHosts(redisConnection: RedisConnection): Array[RedisNode] = {
			getNodes(redisConnection).filter { _.idx == 0 }
	}
  
}