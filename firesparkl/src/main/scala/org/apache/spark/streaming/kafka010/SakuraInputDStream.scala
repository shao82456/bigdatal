package org.apache.spark.streaming.kafka010

import org.apache.spark.streaming.{StreamingContext, Time}

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * Author: shaoff
 * Date: 2020/2/28 13:08
 * Package: org.apache.spark.streaming.kafka010
 * Description:
 *
 */

class SakuraInputDStream[K, V](
                                 _ssc: StreamingContext,
                                 locationStrategy: LocationStrategy,
                                 consumerStrategy: ConsumerStrategy[K, V],
                                 ppc: PerPartitionConfig
                               ) extends DirectKafkaInputDStream[K, V](_ssc, locationStrategy, consumerStrategy, ppc) {
  override def compute(validTime: Time): Option[KafkaRDD[K, V]] = {
    val res = super.compute(validTime)
    val byteBuffers = for (_ <- 1 to Random.nextInt(10)) yield {
      new Array[Byte](1024 * 1024 * 25)
    }
    println(byteBuffers.size)
    res
  }
}
