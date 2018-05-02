package kr.ac.yonsei.delab.addb_srconnector

//import kr.ac.yonsei.delab.addb_srconnector.util.Logging

//import org.apache.spark.SparkConf
//import redis.clients.addb_jedis.{Jedis, JedisPoolConfig, JedisPool, Protocol}
//import redis.clients.addb_jedis.util.{JedisURIHelper, SafeEncoder, JedisClusterCRC16}
//import redis.clients.addb_jedis.exceptions.JedisConnectionException
import kr.ac.yonsei.delab.addb_srconnector.util.Logging

//import scala.collection.JavaConversions._
//import java.net.URI

//// Redis Connection 
//case class RedisConnection (val host: String = Protocol.DEFAULT_HOST,
//                        val port: Int = Protocol.DEFAULT_PORT,
//                        val auth: String = null,
//                        val dbNum: Int = Protocol.DEFAULT_DATABASE,
//                        val timeout: Int = Protocol.DEFAULT_TIMEOUT)
//  extends Serializable 
//  with Logging {
//  
//  /**
//    * Constructor from spark config. set params with redis.host, redis.port, redis.auth and redis.db
//    *
//    * @param conf spark context config
//    */
//  def this(conf: SparkConf) {
//      this(
//        conf.get("redis.host", Protocol.DEFAULT_HOST),
//        conf.getInt("redis.port", Protocol.DEFAULT_PORT),
//        conf.get("redis.auth", null),
//        conf.getInt("redis.db", Protocol.DEFAULT_DATABASE),
//        conf.getInt("redis.timeout", Protocol.DEFAULT_TIMEOUT)
//      )
//  }
//
//  /**
//    * Constructor with Jedis URI
//    *
//    * @param uri connection URI in the form of redis://:$password@$host:$port/[dbnum]
//    */
//  def this(uri: URI) {
//    this(uri.getHost, uri.getPort, JedisURIHelper.getPassword(uri), JedisURIHelper.getDBIndex(uri))
//  }
//
//  /**
//    * Constructor with Jedis URI from String
//    *
//    * @param uri connection URI in the form of redis://:$password@$host:$port/[dbnum]
//    */
//  def this(uri :String) {
//    this(URI.create(uri))
//  }
//
//  /**
//    * Connect tries to open a connection to the redis endpoint,
//    * optionally authenticating and selecting a db
//    *
//    * @return a new Jedis instance
//    */
//  def connect(): Jedis = {
//    RedisConnectionPool.connect(this)
//  }
//}
//// Redis cluster node
//case class RedisNode(val redisConnection: RedisConnection,
//                     val startSlot: Int,
//                     val endSlot: Int,
//                     val idx: Int,
//                     val total: Int) {
//  def connect(): Jedis = {
////    logInfo(s"Redisd Node connect")
//    redisConnection.connect()
//  }
//}

// Redis cluster status
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
//  val hosts = getHosts(configuration.get("host").toString())
//  val nodes = getNodes(initialHost)
//
//  /**
//    * @return initialHost's auth
//    */
//  def getAuth: String = {
//    initialHost.auth
//  }
//
//  /**
//    * @return selected db number
//    */
//  def getDB :Int = {
//    initialHost.dbNum
//  }
//
//  def getRandomNode(): RedisNode = {
//    val rnd = scala.util.Random.nextInt().abs % hosts.length
//    hosts(rnd)
//  }
//
//  /**
//    * @param sPos start slot number
//    * @param ePos end slot number
//    * @return a list of RedisNode whose slots union [sPos, ePos] is not null
//    */
//  def getNodesBySlots(sPos: Int, ePos: Int): Array[RedisNode] = {
//    /* This function judges if [sPos1, ePos1] union [sPos2, ePos2] is not null */
//    def inter(sPos1: Int, ePos1: Int, sPos2: Int, ePos2: Int) =
//      if (sPos1 <= sPos2) ePos1 >= sPos2 else ePos2 >= sPos1
//
//    nodes.filter(node => inter(sPos, ePos, node.startSlot, node.endSlot)).
//      filter(_.idx == 0) //master only now
//  }
//
//  /**
//    * @param key
//    * *IMPORTANT* Please remember to close after using
//    * @return jedis who is a connection for a given key
//    */
//  def connectionForKey(key: String): Jedis = {
//    getHost(key).connect
//  }
//
//  /**
//    * @param initialHost any redis endpoint of a cluster or a single server
//    * @return true if the target server is in cluster mode
//    */
//  private def clusterEnabled(initialHost: RedisConnection): Boolean = {
//    val conn = initialHost.connect()
//    val info = conn.info.split("\n")
//    val version = info.filter(_.contains("redis_version:"))(0)
//    val clusterEnable = info.filter(_.contains("cluster_enabled:"))
//    val mainVersion = version.substring(14, version.indexOf(".")).toInt
//    val res = mainVersion>2 && clusterEnable.length>0 && clusterEnable(0).contains("1")
//    conn.close
//    res
//  }
//
//  /**
//    * @param key
//    * @return host whose slots should involve key
//    */
//  def getHost(key: String): RedisNode = {
//    val slot = JedisClusterCRC16.getSlot(key)
//    hosts.filter(host => {
//      host.startSlot <= slot && host.endSlot >= slot
//    })(0)
//  }
//
//
//  /**
//    * @param initialHost any redis endpoint of a cluster or a single server
//    * @return list of host nodes
//    */
//  private def getHosts(initialHost: RedisConnection): Array[RedisNode] = {
//    getNodes(initialHost).filter(_.idx == 0)
//  }
//
//  /**
//    * @param initialHost any redis endpoint of a single server
//    * @return list of nodes
//    */
//  private def getNonClusterNodes(initialHost: RedisConnection): Array[RedisNode] = {
//    val master = (initialHost.host, initialHost.port)
//    val conn = initialHost.connect()
//
//    val replinfo = conn.info("Replication").split("\n")
//    conn.close
//
//    // If  this node is a slave, we need to extract the slaves from its master
//    if (replinfo.filter(_.contains("role:slave")).length != 0) {
//      val host = replinfo.filter(_.contains("master_host:"))(0).trim.substring(12)
//      val port = replinfo.filter(_.contains("master_port:"))(0).trim.substring(12).toInt
//
//      //simply re-enter this function witht he master host/port
//      getNonClusterNodes(initialHost = new RedisConnection(host, port,
//        initialHost.auth, initialHost.dbNum))
//
//    } else {
//      //this is a master - take its slaves
//
//      val slaves = replinfo.filter(x => (x.contains("slave") && x.contains("online"))).map(rl => {
//        val content = rl.substring(rl.indexOf(':') + 1).split(",")
//        val ip = content(0)
//        val port = content(1)
//        (ip.substring(ip.indexOf('=') + 1), port.substring(port.indexOf('=') + 1).toInt)
//      })
//
//      val nodes = master +: slaves
//      val range = nodes.size
//      (0 until range).map(i =>
//        RedisNode(new RedisConnection(nodes(i)._1, nodes(i)._2, initialHost.auth, initialHost.dbNum,
//                    initialHost.timeout),
//          0, 16383, i, range)).toArray
//    }
//  }
//
//  /**
//    * @param initialHost any redis endpoint of a cluster server
//    * @return list of nodes
//    */
//  private def getClusterNodes(initialHost: RedisConnection): Array[RedisNode] = {
//    val conn = initialHost.connect()
//    val res = conn.clusterSlots().flatMap {
//      slotInfoObj => {
//        val slotInfo = slotInfoObj.asInstanceOf[java.util.List[java.lang.Object]]
//        val sPos = slotInfo.get(0).toString.toInt
//        val ePos = slotInfo.get(1).toString.toInt
//        /*
//         * We will get all the nodes with the slots range [sPos, ePos],
//         * and create RedisNode for each nodes, the total field of all
//         * RedisNode are the number of the nodes whose slots range is
//         * as above, and the idx field is just an index for each node
//         * which will be used for adding support for slaves and so on.
//         * And the idx of a master is always 0, we rely on this fact to
//         * filter master.
//         */
//        (0 until (slotInfo.size - 2)).map(i => {
//          val node = slotInfo(i + 2).asInstanceOf[java.util.List[java.lang.Object]]
//          val host = SafeEncoder.encode(node.get(0).asInstanceOf[Array[scala.Byte]])
//          val port = node.get(1).toString.toInt
//          RedisNode(new RedisConnection(host, port, initialHost.auth, initialHost.dbNum,
//                      initialHost.timeout),
//                    sPos,
//                    ePos,
//                    i,
//                    slotInfo.size - 2)
//        })
//      }
//    }.toArray
//    conn.close()
//    res
//  }
//
//  /**
//    * @param initialHost any redis endpoint of a cluster or a single server
//    * @return list of nodes
//    */
//  def getNodes(initialHost: RedisConnection): Array[RedisNode] = {
//    if (clusterEnabled(initialHost)) {
//      getClusterNodes(initialHost)
//    } else {
//      getNonClusterNodes(initialHost)
//    }
//  }
}
//object RedisConnectionPool {
//  @transient private lazy val pools: ConcurrentHashMap[RedisConnection, JedisPool] = new ConcurrentHashMap[RedisConnection, JedisPool]()
//  def connect(redisConnection: RedisConnection): Jedis = {
//    val pool = pools.getOrElseUpdate(redisConnection,
//      {
//        val poolConfig: JedisPoolConfig = new JedisPoolConfig();
//        // Configuration setting
//        poolConfig.setMaxTotal(100)
////        poolConfig.setMaxIdle(32)
////        poolConfig.setTestOnBorrow(false)
////        poolConfig.setTestOnReturn(false)
////        poolConfig.setTestWhileIdle(false)
////        poolConfig.setMinEvictableIdleTimeMillis(60000)
////        poolConfig.setTimeBetweenEvictionRunsMillis(30000)
////        poolConfig.setNumTestsPerEvictionRun(-1)
//        new JedisPool(poolConfig, redisConnection.host, redisConnection.port, 
//            redisConnection.timeout, redisConnection.auth, redisConnection.dbNum)
//      }
//    )
//    var sleepTime: Int = 4
//    var conn: Jedis = null
//    while (conn == null) {
//      try {
//        conn = pool.getResource
//      }
//      catch {
//        case e: JedisConnectionException if e.getCause.toString.
//          contains("ERR max number of clients reached") => {
//          if (sleepTime < 500) sleepTime *= 2
//          Thread.sleep(sleepTime)
//        }
//        case e: Exception => throw e
//      }
//    }
//    conn
//  }
//}

object RedisConfigPool {
  val pool = Map[Configuration, RedisConfig]()
  def get( configuration: Configuration ): RedisConfig = {
    synchronized {
      pool.getOrElse( configuration, new RedisConfig( configuration ) )
    }
  }
}
