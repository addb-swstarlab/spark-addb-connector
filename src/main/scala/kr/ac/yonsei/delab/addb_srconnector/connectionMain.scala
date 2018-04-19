package kr.ac.yonsei.delab.addb_srconnector
//package redis.clients.addb_jedis;

//import redis.clients.addb_jedis.Jedis;
//import org.apache.spark.SparkConf
import redis.clients.addb_jedis.{JedisPool, JedisPoolConfig, Jedis}
import redis.clients.addb_jedis.Protocol

object connectionMain {
  
  @transient private var pool: JedisPool = null
  
  def connect() :Unit = {
    println("Hello world!!")
//    var jedis:Jedis = null
//    try {
//      jedis = JedisFactory.getInstance().getJedisPool().getResource()
//    } catch (JedisConnectionException e) {
//      println(e)
//    } finally {
//      println("Done...")
//    }
    val poolConfig: JedisPoolConfig = new JedisPoolConfig()
    val pool:JedisPool = new JedisPool(poolConfig, Protocol.DEFAULT_HOST, Protocol.DEFAULT_PORT, 
        Protocol.DEFAULT_TIMEOUT, "foobared", Protocol.DEFAULT_DATABASE)
    val jedis: Jedis = pool.getResource
    var key = "John"
    var value = "Lee"
    jedis.set(key, value);
    println(""+jedis.fpread(key))
    jedis.close()
    
  }
}