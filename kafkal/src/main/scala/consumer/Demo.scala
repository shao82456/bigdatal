package consumer

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._

/**
 * KafkaConsumer线程不安全，但是也不能一个线程中有多个KafkaConsumer吗
 */
object Demo {
  def initProp(group: String): Properties = {
    /*主要配置介绍 https://developer.51cto.com/art/201911/606894.htm
    * */
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", group)
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", "true");
    props.put("auto.commit.interval.ms", "10000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("max.partition.fetch.bytes", "1048576")
    props.put("max.poll.records", "100")
    props
  }

  def main(args: Array[String]): Unit = {
    val topic = "test_input"
    val group = "tst.t1"
    val props = initProp(group)
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)

    //    val p2 = props.clone().asInstanceOf[Properties]
    //    val consumer2: KafkaConsumer[String, String] = new KafkaConsumer[String, String](p2)

    consumer.subscribe(util.Arrays.asList(topic))
    while(true){
      val records1 = consumer.poll(300)
      records1.asScala.foreach(record=>println(record.value()))
      Thread.sleep(1000*30)
//      println("once")
    }
  }
}