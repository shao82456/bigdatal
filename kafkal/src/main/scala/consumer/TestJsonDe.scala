package consumer

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._

/**
 * KafkaConsumer线程不安全，但是也不能一个线程中有多个KafkaConsumer吗
 */
object TestJsonDe {
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
      val records1 = consumer.poll(30)
      records1.asScala.foreach(record=>println(record.value()))
      println("once")
    }
    val records1 = consumer.poll(10)
    //    val records2 = consumer.poll(5000)
    //    val records3 = consumer.poll(5000)

    println(records1.count())
    val records2 = consumer.poll(5000)
    println(records2.count())

    Thread.sleep(1000 * 3000)
    val records = records1.asScala
    //    ++records2.asScala
    //    ++records3.asScala
    //    ++ records2.asScala
    println(records.size)
    //    val tps = consumer.assignment()
    //    val m1 = tps.asScala.map(tp => {
    //      tp -> consumer.position(tp)
    //    }).toMap
    //    Thread.sleep(1000*3)
    //    consumer.seekToEnd(tps)
    //    val m2 = tps.asScala.map(tp => {
    //        tp -> consumer.position(tp)
    //    }).toMap
    //    println(m1)
    //    println(m2)
  }
}