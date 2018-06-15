package kr.ac.yonsei.delab.addb_srconnector.util

import scala.collection.mutable.{StringBuilder, ArrayBuffer}
import redis.clients.addb_jedis.util.JedisClusterCRC16
import kr.ac.yonsei.delab.addb_srconnector.RedisNode
import org.apache.spark.sql.sources._
import scala.collection.mutable.Stack


/*
 * generate full datakey :=  "D:{TableInfo:PartitionInfo}"
 */
object KeyUtil {
  def makeSourceString(host:String, port: Int):String = {
    var buf:StringBuilder = new StringBuilder
    buf.append(host).append(":").append(port.toString())
    buf.toString()
  }
  
  def returnHost(SourceString : String):String = {
    var buf:StringBuilder = new StringBuilder
    buf.append (SourceString.substring(0, SourceString.indexOf(":")))
    buf.toString()
  }
  
  def returnPort(SourceString : String): Int = {
    var buf:StringBuilder = new StringBuilder
    buf.append (SourceString.substring(SourceString.indexOf(":") + 1, SourceString.size))
    buf.toString().toInt
  }
  
  def generateKeyForMeta(tableID:Int):String = {
    var buf:StringBuilder = new StringBuilder
    buf.append("M:{").append(tableID+":").append("*").append("}")
    buf.toString()
  }
  
  def getPartitionFromMeta(MetaKey:String):String = {
    var buf:StringBuilder = new StringBuilder
    buf.append(MetaKey.substring(MetaKey.indexOf(":", MetaKey.indexOf("{")) + 1, MetaKey.indexOf("}")))
    buf.toString()
  }
  
  def generateDataKey(tableID: Int, partitions: Array[String]): Array[String] = {
		 var buf: StringBuilder = null
		 val res : ArrayBuffer[String] = ArrayBuffer[String]()   
    partitions.foreach { partition => 
      buf = new StringBuilder
      buf.append("D:{").append(tableID + ":").append(partition).append("}")
      res += buf.toString
    }
    res.toArray
  }

  // return datakey and partitionInfo
  def generateDataKey(tableID:Int, partitionColumnInfo:Array[(Int, String)]):(String, String) = {
    var buf:StringBuilder = new StringBuilder
    var partition = ""
    // tableInfo
    buf.append("D:{").append(tableID+":")
    // partitionInfo
    if (partitionColumnInfo != null && partitionColumnInfo.size > 0) {
      // make 1:3142  :  2:4124
      // start partitionID from 1.
      partition = partitionColumnInfo.map(column => ((column._1)+":"+column._2).toString()) 
                    .mkString(":")
      buf.append(partition)
    }
    buf.append("}")
    (buf.toString, partition)
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

  def groupKeysByNode(nodes: Array[RedisNode], keys: Array[String]): // 각 노드에 속하는 key들로 이루어지도록 구성.
  Array[(String, Array[String])] = {
    def getNode(key: String): RedisNode = { // key를 입력하고, 그 key가 어떤 node에 속하는지 찾아내어주는 함수
      val slot = JedisClusterCRC16.getSlot(key)
      /* Master only */
      nodes.filter(node => { node.startSlot <= slot && node.endSlot >= slot }).filter(_.idx == 0)(0)
    }
    keys.map(key => (getNode(key), key)).toArray.groupBy(_._1). // ???  (1, List(1, 2, 3)), 2 => List (4, 5, 6)
      map{x => (makeSourceString(x._1.redisConnection.host, x._1.redisConnection.port), x._2.map(_._2)) 
    }.toArray
  }

  def getNodeForKey(nodes: Array[RedisNode], key: String): RedisNode = { // key를 입력하고, 그 key가 어떤 node에 속하는지 찾아내어주는 함수
    val slot = JedisClusterCRC16.getSlot(key)
    /* Master only */
    nodes.filter(node => { node.startSlot <= slot && node.endSlot >= slot }).filter(_.idx == 0)(0)
  }
  
  
}