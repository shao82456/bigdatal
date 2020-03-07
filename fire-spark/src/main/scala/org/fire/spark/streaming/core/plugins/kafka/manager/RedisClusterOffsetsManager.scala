package org.fire.spark.streaming.core.plugins.kafka.manager

import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.fire.spark.streaming.core.plugins.redis.RedisClusterUtil

import scala.collection.JavaConversions._

/**
  * Offset 存储到Redis-Cluster
  * @param sparkConf
  */
private[kafka] class RedisClusterOffsetsManager(val sparkConf: SparkConf) extends OffsetsManager {

  private val jedis = RedisClusterUtil.connect(redisEndpoint)

  override def getOffsets(groupId: String, topics: Set[String]): Map[TopicPartition, Long] = {
    val offsets = topics.flatMap(topic => {
      jedis.hgetAll(generateKey(groupId,topic)).map {
        case (partition,offset) => new TopicPartition(topic,partition.toInt) -> offset.toLong
      }
    })
    logInfo(s"getOffsets [$groupId,${offsets.mkString(",")}] ")

    offsets.toMap
  }


  override def updateOffsets(groupId: String, offsetInfos: Map[TopicPartition, Long]): Unit = {
    offsetInfos.foreach { case (tp, offset) =>
      jedis.hset(generateKey(groupId, tp.topic), tp.partition().toString, offset.toString)
    }
    logInfo(s"updateOffsets [ $groupId,${offsetInfos.mkString(",")} ]")
  }

  override def delOffsets(groupId: String, topics: Set[String]): Unit = {
    topics.foreach(x => jedis.del(generateKey(groupId, x)))
    logInfo(s"delOffsets [ $groupId,${topics.mkString(",")} ]")
  }
}
