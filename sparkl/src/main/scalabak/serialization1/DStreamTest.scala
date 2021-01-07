package serialization1

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * Author: shaoff
 * Date: 2020/5/16 10:01
 * Package: serialization1
 * Description:
 *
 */
class SparkStreamJob extends Serializable{
  var groupId: String = "test"
  var jobCode: String = "test"

  private def setMap(ds: DStream[collection.mutable.Map[String,String]]): DStream[collection.mutable.Map[String,String]] = {
//    val jobCode = this.jobCode //将类中的属性变为闭包内的对象
    ds.map(x => {
      x("jobCode")=jobCode
      x
    })
  }

  def setTransform(): Unit = {
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "127.0.0.1:9092",
      "group.id" -> "test",
      "key.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer",
      "auto.offset.reset" -> "latest"
    )
    val sparkConf=new SparkConf(true)
    sparkConf.setAppName("DStreamTest")
    sparkConf.setMaster("local[3]")
    val streamingContext = new StreamingContext(sparkConf, Seconds(30))
    val stream: DStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe(List("test_shao"), kafkaParams))
    setMap(stream.map(x => collection.mutable.Map(groupId->x.value())))
  }
}

object DStreamTest {
  def main(args: Array[String]): Unit = {
    val sparkJob =new SparkStreamJob()
    sparkJob.setTransform()
//    sparkJob.execute()
  }
}
