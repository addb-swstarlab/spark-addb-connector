package kr.ac.yonsei.delab.addb_srconnector

import org.apache.spark.SparkConf
import redis.clients.addb_jedis.{Jedis, JedisPoolConfig, JedisPool, Protocol}
import redis.clients.addb_jedis.util.{JedisURIHelper, SafeEncoder, JedisClusterCRC16}
import redis.clients.addb_jedis.exceptions.JedisConnectionException
import kr.ac.yonsei.delab.addb_srconnector.util.Logging

import scala.collection.JavaConversions._
import java.util.concurrent.ConcurrentHashMap
import java.net.URI

// Redis Connection 
case class RedisConnection (val host: String = Protocol.DEFAULT_HOST,
                        val port: Int = Protocol.DEFAULT_PORT,
                        val auth: String = null,
                        val dbNum: Int = Protocol.DEFAULT_DATABASE,
                        val timeout: Int = Protocol.DEFAULT_TIMEOUT)
  extends Serializable 
  with Logging {
  
  /**
    * Constructor from spark config. set params with redis.host, redis.port, redis.auth and redis.db
    *
    * @param conf spark context config
    */
  def this(conf: SparkConf) {
      this(
        conf.get("redis.host", Protocol.DEFAULT_HOST),
        conf.getInt("redis.port", Protocol.DEFAULT_PORT),
        conf.get("redis.auth", null),
        conf.getInt("redis.db", Protocol.DEFAULT_DATABASE),
        conf.getInt("redis.timeout", Protocol.DEFAULT_TIMEOUT)
      )
  }

  /**
    * Constructor with Jedis URI
    *
    * @param uri connection URI in the form of redis://:$password@$host:$port/[dbnum]
    */
  def this(uri: URI) {
    this(uri.getHost, uri.getPort, JedisURIHelper.getPassword(uri), JedisURIHelper.getDBIndex(uri))
  }

  /**
    * Constructor with Jedis URI from String
    *
    * @param uri connection URI in the form of redis://:$password@$host:$port/[dbnum]
    */
  def this(uri :String) {
    this(URI.create(uri))
  }

  /**
    * Connect tries to open a connection to the redis endpoint,
    * optionally authenticating and selecting a db
    *
    * @return a new Jedis instance
    */
  def connect(): Jedis = {
    RedisConnectionPool.connect(this)
  } 
}
// Redis cluster node
case class RedisNode(val redisConnection: RedisConnection,
                     val startSlot: Int,
                     val endSlot: Int,
                     val idx: Int,
                     val total: Int)
                     extends Serializable 
                     with Logging {
  def connect(): Jedis = {
    logDebug(s"[ADDB] Redisd Node connect")
    redisConnection.connect()
  }
}

object RedisConnectionPool {
  @transient private lazy val pools: ConcurrentHashMap[RedisConnection, JedisPool] = new ConcurrentHashMap[RedisConnection, JedisPool]()
  // Get jedis resource from jedis pool
  def connect(redisConnection: RedisConnection): Jedis = {
    val pool = pools.getOrElseUpdate(redisConnection,
      {
        val poolConfig: JedisPoolConfig = new JedisPoolConfig();
        // Configuration setting
        poolConfig.setMaxTotal(1000)
        poolConfig.setMaxWaitMillis(300000)
//        poolConfig.setMaxIdle(32)
//        poolConfig.setTestOnBorrow(false)
//        poolConfig.setTestOnReturn(false)
//        poolConfig.setTestWhileIdle(false)
//        poolConfig.setMinEvictableIdleTimeMillis(60000)
//        poolConfig.setTimeBetweenEvictionRunsMillis(30000)
//        poolConfig.setNumTestsPerEvictionRun(-1)
//        if (redisConnection.auth == "null") {
//          new JedisPool(poolConfig, redisConnection.host, redisConnection.port, 
//            redisConnection.timeout, null, redisConnection.dbNum)
//        } else {
//          	new JedisPool(poolConfig, redisConnection.host, redisConnection.port, 
//        			redisConnection.timeout, redisConnection.auth, redisConnection.dbNum)
          	new JedisPool(poolConfig, redisConnection.host, redisConnection.port, 
          			30000000, redisConnection.auth, redisConnection.dbNum) // 30000sec
//          			300000, redisConnection.auth, redisConnection.dbNum) // 300sec
//         }
      }
    )
    var sleepTime: Int = 4
    var conn: Jedis = null
    while (conn == null) {
      try {
        conn = pool.getResource
      }
      catch {
        case e: JedisConnectionException if e.getCause.toString.
          contains("ERR max number of clients reached") => {
          if (sleepTime < 500) sleepTime *= 2
          Thread.sleep(sleepTime)
        }
        case e: Exception => throw e
      }
    }
    conn
  }
}
