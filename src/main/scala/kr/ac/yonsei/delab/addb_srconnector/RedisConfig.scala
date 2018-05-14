package kr.ac.yonsei.delab.addb_srconnector

import kr.ac.yonsei.delab.addb_srconnector.util.Logging

/* 
 * RedisConfig class manage RedisStore
 * Each RedisStore is created by configuration
 */
class RedisConfig (val configuration:Configuration)
  extends Serializable 
  with Logging {
//  val initialAddr = configuration.get("host").toString()

  @transient private var redisStore: RedisStore = null

  def createRedisStore(): RedisStore = {
    val store = new RedisStore( this )
    store.configure( configuration )
    logInfo( s"$store created" )
    store
  }
  
  def getRedisStore(): RedisStore = {
    logInfo( s"get RedisStore object" )
    this.synchronized {
    	if ( redisStore == null ) {
    		redisStore = createRedisStore()
    		redisStore
    	} else {
    		redisStore
    	}
    }
  }
}
/*
 * For preventing repeated creation of RedisConfig,
 * maintain RedisConfigPool
 */
object RedisConfigPool {
  val pool = scala.collection.mutable.Map[Configuration, RedisConfig]()
  def get( configuration: Configuration ): RedisConfig = {
    synchronized {
      // check whether mutable Map includes RedisConfig object
      // if not, add and return new RedisConfig
      val res = pool.get(configuration)
      if (res == None ) {
        val newRedisConfig = new RedisConfig(configuration)
        pool += (configuration -> newRedisConfig)
        newRedisConfig
      } else {
    	  res.get
      }
    }
  }
}
