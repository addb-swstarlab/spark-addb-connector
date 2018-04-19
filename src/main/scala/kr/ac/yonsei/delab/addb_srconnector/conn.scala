package kr.ac.yonsei.delab.addb_srconnector

//import org.apache.spark.sql.SQLContext
import redis.clients.addb_jedis.{JedisPool, JedisPoolConfig, Jedis}
import redis.clients.addb_jedis.Protocol

object conn {
  def connect():Unit = {
    val poolConfig: JedisPoolConfig = new JedisPoolConfig()
    var pool:JedisPool = new JedisPool(poolConfig, Protocol.DEFAULT_HOST, Protocol.DEFAULT_PORT, 
        Protocol.DEFAULT_TIMEOUT, "foobared", Protocol.DEFAULT_DATABASE)
    val jedis: Jedis = pool.getResource
    var key = "John"
    var value = "Lee"
    jedis.set(key, value);
    println(""+jedis.fpread(key))
    jedis.close()
  }
}