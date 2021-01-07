package streaming

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.mutable

/**
 * Author: shaoff
 * Date: 2020/9/22 17:03
 * Package: streaming
 * Description:
 *
 */
object KafkaStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafkaStreaming").setMaster("local[4]")
    val streamingContext = new StreamingContext(conf, Seconds(10))

    val kafkaProperties = new Properties()
    val kafkaParams = new mutable.HashMap[String, String]()
    for (en <- kafkaProperties.entrySet().asScala) {
      kafkaParams.put(en.getKey.toString, en.getValue.toString)
    }

    val topics = List("test_rule")
    val lines = KafkaUtils.createDirectStream[String, String](streamingContext, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topics, kafkaParams))


    lines.foreachRDD((rdd, time) => {
      rdd.foreach(println(_))
    })
  }

}
