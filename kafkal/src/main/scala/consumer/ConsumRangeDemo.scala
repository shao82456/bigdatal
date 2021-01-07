package consumer

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

/**
 * KafkaConsumer线程不安全，但是也不能一个线程中有多个KafkaConsumer吗
 */
object ConsumRangeDemo {
  def initProp(group: String): Properties = {
    /*主要配置介绍 https://developer.51cto.com/art/201911/606894.htm
    * */
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", group)
    props.put("auto.offset.reset", "none")
    props.put("enable.auto.commit", "false");
    props.put("auto.commit.interval.ms", "10000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("max.partition.fetch.bytes", "1048576")
    props.put("max.poll.records", "100")
    props
  }

  def main(args: Array[String]): Unit = {
    val topic = "test_rule"
    val group = "spark-executor-t1"
    val partitionId = 0

    val props = initProp(group)
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)

    val topicPartition = new TopicPartition(topic, partitionId)
    //    val p2 = props.clone().asInstanceOf[Properties]
    //    val consumer2: KafkaConsumer[String, String] = new KafkaConsumer[String, String](p2)

    consumer.assign(util.Arrays.asList(topicPartition))
    val offsetRange = (0 until 100)
    val res: Array[ConsumerRecord[_, _]] = new Array(100)
    var nextOffset = 0
    var buffer: Iterator[ConsumerRecord[_, _]] = null
    while (nextOffset != 100) {
      if (buffer == null) {
        consumer.seek(topicPartition, nextOffset)
        buffer = consumer.poll(1000 * 120).iterator().asScala

        if (!buffer.hasNext) {
          consumer.seek(topicPartition, nextOffset)
          buffer = consumer.poll(1000 * 120).iterator().asScala
        }
      }

      assert(buffer.hasNext, s"Failed to get records for $topic $topicPartition $nextOffset after polling")
      var record = buffer.next()
      if (record.offset() != nextOffset) {
        consumer.seek(topicPartition, nextOffset)
        buffer = consumer.poll(1000 * 120).iterator().asScala
        assert(buffer.hasNext,
          s"Failed to get records for $topic $topicPartition $nextOffset after polling")
        record = buffer.next()
        assert(record.offset == nextOffset,
          s"Got wrong record for $topic $topicPartition even after seeking to offset $nextOffset")
      }
      res(nextOffset) = record
      nextOffset += 1
    }

    consumer.metrics().asScala.foreach(p=>println(p._1.name()))
    println(res.length)
    println(res.take(10))
    Thread.sleep(1000 * 30)
  }


}