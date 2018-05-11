package kr.ac.yonsei.delab.addb_srconnector.util

import scala.collection.mutable.StringBuilder
import redis.clients.addb_jedis.util.JedisClusterCRC16
import kr.ac.yonsei.delab.addb_srconnector.RedisNode

object KeyUtil {
  // 1) partition is not implemented
  // When partition is implemented, this function will be deleted..
  def generateDataKey(tableID:Int):String = {
    var buf:StringBuilder = new StringBuilder
    // tableInfo
    buf.append("D:{").append(tableID+":").append("}")
    buf.toString()
  }
  // 2) partition is implemented
  def generateDataKey(tableID:Int, partitionColumnInfo:Array[(Int, String)]):String = {
    var buf:StringBuilder = new StringBuilder
    // tableInfo
    buf.append("D:{").append(tableID+":")
    // partitionInfo
    if (partitionColumnInfo != null && partitionColumnInfo.size > 0) {
      // make 1:3142  :  2:4124
      // start partitionID from 1.
      var partition = partitionColumnInfo.map(column => ((column._1)+":"+column._2).toString()) 
                    .mkString(":")
      buf.append(partition)
    }
    buf.append("}")
    buf.toString
  }
  /**
    * @param nodes list of RedisNode
    * @param keys list of keys
    * return (node: (key1, key2, ...), node2: (key3, key4,...), ...)
    */
  def groupKeysByNode(nodes: Array[RedisNode], keys: Iterator[String]): // 각 노드에 속하는 key들로 이루어지도록 구성.
  Array[(RedisNode, Array[String])] = {
    def getNode(key: String): RedisNode = { // key를 입력하고, 그 key가 어떤 node에 속하는지 찾아내어주는 함수
      val slot = JedisClusterCRC16.getSlot(key)
      /* Master only */
      nodes.filter(node => { node.startSlot <= slot && node.endSlot >= slot }).filter(_.idx == 0)(0)
    }
    keys.map(key => (getNode(key), key)).toArray.groupBy(_._1). // ???  (1, List(1, 2, 3)), 2 => List (4, 5, 6)
      map(x => (x._1, x._2.map(_._2))).toArray
  }
}
object PartitionUtil {
  def getPartitionInfo(partitionColumnIndex:Array[Int]):String = {
    val buf:StringBuilder = new StringBuilder
    val partitionColumnCount = partitionColumnIndex.size
    buf.append(partitionColumnCount)
    partitionColumnIndex.foreach { index => 
      buf.append(":")
      buf.append(index)
     }
    buf.toString
  }
}