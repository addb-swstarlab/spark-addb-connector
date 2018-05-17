package kr.ac.yonsei.delab.addb_srconnector.partition
import org.apache.spark.Partition
import kr.ac.yonsei.delab.addb_srconnector.RedisConfig
import kr.ac.yonsei.delab.addb_srconnector._

class RedisPartition(
    override val index: Int,
    val redisConfig: RedisConfig,
    val location: String,
    val partition: Array[String]
    )
    extends Partition {
}
