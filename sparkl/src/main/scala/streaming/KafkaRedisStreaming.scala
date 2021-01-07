package streaming

import java.util.Properties

import jedisl.connect.JedisUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.mutable
import scala.reflect.runtime.universe
import scala.tools.scalap.scalax.rules.scalasig.Method

/**
 * Author: shaoff
 * Date: 2020/9/22 17:03
 * Package: streaming
 * Description:
 * java -javaagent:/Users/sakura/projects/realtime-alert/spark-instrument-agent/target/spark-instrument-agent-1.0.jar
 */
object KafkaRedisStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafkaStreaming").setMaster("local[4]")

//    conf.set("spark.metrics.namespace","${spark.app.name}")

    conf.set("spark.metrics.conf.*.source.KafkaConsumer.class","org.apache.spark.metrics.source.KafkaConsumerMetricSource")
    conf.set("spark.metrics.conf.*.source.Jedis.class","org.apache.spark.metrics.source.JedisMetricSource")
    conf.set("spark.metrics.conf.*.sink.console.class", "org.apache.spark.metrics.sink.Slf4jSink")
    conf.set("spark.metrics.conf.*.sink.kafka.metrics-name-capture-regex","(.*driver_|.*_[0-9]d+_)(.+)")
    conf.set("spark.metrics.conf.*.sink.kafka.metrics-name-replacement","$2")
//    conf.set("spark.metrics.conf.*.sink.kafka.class", "org.apache.spark.metrics.sink.KafkaMetricSink")
    conf.set("spark.metrics.conf.*.sink.kafka.topic", "test_metric")
    conf.set("spark.metrics.conf.*.sink.kafka.bootstrap-servers", "localhost:9092")

    val streamingContext = new StreamingContext(conf, Seconds(3))

    val kafkaProperties = new Properties()
    kafkaProperties.setProperty("group.id", "test.t2")
    kafkaProperties.setProperty("auto.offset.reset", "earliest")
    kafkaProperties.setProperty("bootstrap.servers", "localhost:9092")
    kafkaProperties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProperties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    kafkaProperties.setProperty("metric.reporters", "org.apache.spark.metrics.source.SparkSinkMetricReporter")

    val kafkaParams = new mutable.HashMap[String, String]()
    for (en <- kafkaProperties.entrySet().asScala) {
      kafkaParams.put(en.getKey.toString, en.getValue.toString)
    }

    val topics = List("test_rule")
    val lines = KafkaUtils.createDirectStream[String, String](streamingContext, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    lines.foreachRDD((rdd, time) => {
      rdd.map(_.value()).foreach{key=>

        JedisUtil.safeClose(jedis=> {
          jedis.hset("KafkaRedisStreaming",key, key.hashCode+"")
          println(jedis.hget("KafkaRedisStreaming",key))
        })(JedisUtil.connect("localhost:6379"))
      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
