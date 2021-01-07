package streaming

import java.util.Properties

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
 *
 */
object ReflectKafkaStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafkaStreaming").setMaster("local[4]")

    conf.set("spark.metrics.namespace","${spark.app.name}")
//    conf.set("spark.metrics.conf.*.sink.console.class", "org.apache.spark.metrics.sink.Slf4jSink")
//    conf.set("spark.metrics.conf.*.source.KafkaConsumer.class","org.apache.spark.metrics.source.KafkaConsumerSource")

    val streamingContext = new StreamingContext(conf, Seconds(3))

    val kafkaProperties = new Properties()
    kafkaProperties.setProperty("group.id", "test.t1")
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

      import scala.reflect.runtime.universe

      val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
      val module = runtimeMirror.staticModule("org.apache.spark.streaming.kafka010.CachedKafkaConsumer")
      val obj = runtimeMirror.reflectModule(module)
      val methods = runtimeMirror.reflectModule(module)
      val objMirror = runtimeMirror.reflect(methods.instance)
      val method = methods.symbol.typeSignature.member(universe.TermName("cache")).asMethod

      import scala.collection.JavaConverters._
      val cacheValue = objMirror.reflectMethod(method)()
      if(cacheValue!=null){
        val cache =cacheValue.asInstanceOf[java.util.LinkedHashMap[_,_]].asScala
        val ins = cache.head._2
        val ckcClazz = Thread.currentThread().getContextClassLoader.loadClass("org.apache.spark.streaming.kafka010.CachedKafkaConsumer")
        val consumer=ScalaReflectUtil.invokeMethod(ins, ckcClazz, "consumer", Array.empty)
        println(consumer)
      }

      //      cacheField.setAccessible(true)
      //      val cache = cacheField.invoke(null)
      rdd.foreach(println(_))
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }



}
