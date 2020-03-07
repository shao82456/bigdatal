package org.fire.spark.streaming.core.plugins.kafka

import java.lang.reflect.Constructor
import java.util.concurrent.ConcurrentHashMap

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange, SakuraKafkaUtils}
import org.fire.spark.streaming.core.Logging
import org.fire.spark.streaming.core.kit.Utils
import org.fire.spark.streaming.core.plugins.kafka.manager.{DefaultOffsetsManager, HbaseOffsetsManager, KafkaManager, OffsetsManager, RedisClusterOffsetsManager, RedisOffsetsManager}
import org.fire.spark.streaming.core.sources.Source

import scala.language.postfixOps
import scala.reflect.ClassTag

/**
 * Created by guoning on 2017/5/25.
 * 封装 Kafka Direct Api
 *
 * @param ssc
 * @param specialKafkaParams 指定 Kafka 配置,可以覆盖配置文件
 */
class SakuraKafkaDirectSource[K: ClassTag, V: ClassTag](@transient val ssc: StreamingContext,
                                                        specialKafkaParams: Map[String, String] = Map.empty[String, String])
  extends Source with Logging {

  private val prefix: String = "spark.source.kafka.consumer."
  override val paramPrefix: String = "spark.source.kafka.consume."

  // 保存 offset
  private lazy val offsetRanges: java.util.Map[Long, Array[OffsetRange]] = new ConcurrentHashMap[Long, Array[OffsetRange]]

  private var canCommitOffsets: CanCommitOffsets = _

  // 分区数
  private lazy val repartition: Int = sparkConf.get(s"$paramPrefix.repartition", "0").toInt

  //兼容consumer和consume的差异
  private lazy val globalParams = if (param.isEmpty) {
    sparkConf.getAllWithPrefix(prefix).toMap
  } else {
    param
  }

  // 组装 Kafka 参数
  private lazy val kafkaParams: Map[String, String] = globalParams ++ specialKafkaParams ++ Map("enable.auto.commit" -> "false")

  // kafka 消费 topic
  private lazy val topicSet: Set[String] = kafkaParams("topics").split(",").map(_.trim).toSet

  private lazy val groupId = kafkaParams.get("group.id")

  val km = new KafkaManager(ssc.sparkContext.getConf)

  private lazy val offsetsManager = {
    sparkConf.get("spark.source.kafka.offset.store.class", "none").trim match {
      case "none" =>
        sparkConf.get("spark.source.kafka.offset.store.type", "none").trim.toLowerCase match {
          case "redis" => new RedisOffsetsManager(sparkConf)
          case "redis-cluster" => new RedisClusterOffsetsManager(sparkConf)
          case "hbase" => new HbaseOffsetsManager(sparkConf)
          case "kafka" => new DefaultOffsetsManager(sparkConf)
          case "none" => new DefaultOffsetsManager(sparkConf)
        }
      case clazz =>

        logInfo(s"Custom offset management class $clazz")
        val constructors = {
          val offsetsManagerClass = Utils.classForName(clazz)
          offsetsManagerClass
            .getConstructors
            .asInstanceOf[Array[Constructor[_ <: SparkConf]]]
        }
        val constructorTakingSparkConf = constructors.find { c =>
          c.getParameterTypes.sameElements(Array(classOf[SparkConf]))
        }
        constructorTakingSparkConf.get.newInstance(sparkConf).asInstanceOf[OffsetsManager]
    }
  }


  override type SourceType = ConsumerRecord[K, V]

  /**
   * 获取DStream 流
   *
   * @return
   */
  override def getDStream[R: ClassTag](messageHandler: ConsumerRecord[K, V] => R): DStream[R] = {

    var consumerOffsets = Map.empty[TopicPartition, Long]

    kafkaParams.get("group.id") match {
      case Some(groupId) =>

        logInfo(s"createDirectStream witch group.id $groupId topics ${topicSet.mkString(",")}")

        consumerOffsets = offsetsManager.getOffsets(groupId.toString, topicSet)

      case _ =>
        logInfo(s"createDirectStream witchout group.id topics ${topicSet.mkString(",")}")
    }

    logInfo(s"read topics ==[$topicSet]== from offsets ==[$consumerOffsets]==")
    val stream= SakuraKafkaUtils.createDirectStream[K, V](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[K, V](topicSet, kafkaParams, consumerOffsets))

    /*val stream = km.createDirectStream[K, V](ssc, kafkaParams, topicSet)*/
    canCommitOffsets = stream.asInstanceOf[CanCommitOffsets]
    val res = stream.transform((rdd, time) => {
      offsetRanges.put(time.milliseconds, rdd.asInstanceOf[HasOffsetRanges].offsetRanges)
      rdd
    }).map(messageHandler)
    res
  }

  /**
   * 更新Offset 操作 一定要放在所有逻辑代码的最后
   * 这样才能保证,只有action执行成功后才更新offset
   */
  def updateOffsets(time: Long): Unit = {
    // 更新 offset
    if (groupId.isDefined) {
      logger.info(s"updateOffsets with ${km.offsetManagerType} for time $time offsetRanges: $offsetRanges")
      val offset = offsetRanges.get(time)
      km.offsetManagerType match {
        case "kafka" => canCommitOffsets.commitAsync(offset)
        case _ => km.updateOffsets(groupId.get, offset)
      }
    }
    offsetRanges.remove(time)
  }
}