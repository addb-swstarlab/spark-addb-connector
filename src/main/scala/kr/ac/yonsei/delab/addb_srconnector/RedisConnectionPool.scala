package kr.ac.yonsei.delab.addb_srconnector

import redis.clients.addb_jedis.{Jedis, JedisPoolConfig, JedisPool}
import redis.clients.addb_jedis.exceptions.JedisConnectionException

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConversions._

object RedisConnectionPool {
  @transient private lazy val pools: ConcurrentHashMap[RedisConnection, JedisPool] = new ConcurrentHashMap[RedisConnection, JedisPool]()
  def connect(redisConnection: RedisConnection): Jedis = {
    val pool = pools.getOrElseUpdate(redisConnection,
      {
        val poolConfig: JedisPoolConfig = new JedisPoolConfig();
        // Configuration setting
        poolConfig.setMaxTotal(100)
//        poolConfig.setMaxIdle(32)
//        poolConfig.setTestOnBorrow(false)
//        poolConfig.setTestOnReturn(false)
//        poolConfig.setTestWhileIdle(false)
//        poolConfig.setMinEvictableIdleTimeMillis(60000)
//        poolConfig.setTimeBetweenEvictionRunsMillis(30000)
//        poolConfig.setNumTestsPerEvictionRun(-1)
        new JedisPool(poolConfig, redisConnection.host, redisConnection.port, 
            redisConnection.timeout, redisConnection.auth, redisConnection.dbNum)
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