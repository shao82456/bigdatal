package sakura.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.fire.spark.streaming.core.Logging

object Test extends Logging {

  case class DS(f1: String, f2: String)

  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("TraceTest").setMaster("local[3]")
    sparkConf.set("spark.source.kafka.consumer.topics", "test")
    sparkConf.set("spark.source.kafka.consumer.group.id", "test.t1")
    sparkConf.set("spark.source.kafka.consumer.bootstrap.servers", "localhost:9092")
    sparkConf.set("spark.source.kafka.consumer.auto.offset.reset", "latest")
    sparkConf.set("spark.source.kafka.consumer.key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    sparkConf.set("spark.source.kafka.consumer.value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    sparkConf.set("spark.source.kafka.consumer.max.partition.fetch.bytes", "10485760")
    sparkConf.set("spark.alert.ding", "https://oapi.dingtalk.com/robot/send?access_token=dec3b1a3afb0b9f48985c35f0edc8ab373948e50d9d3630382a893f6f99fff03")
    sparkConf.set("spark.alert.mail", "shaofengfeng@zuoyebang.com")
    sparkConf.set("spark.alert.mobile", "17150012018")
    sparkConf.set("spark.run.main", "KafkaTest")


    val sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val rdd = sc.parallelize(List(
      ("a", 1), ("a", 2), ("b", 2), ("c", 3), ("c", 4), ("d", 5), ("a", 6))
    ).map(pair => DS(pair._1, pair._2.toString))

    import spark.implicits._
    val df = rdd.toDF
    df.write.csv("file:///tmp/test")
    //    rdd2.foreach(println)
  }

}
