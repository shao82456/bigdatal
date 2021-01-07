package streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

import scala.reflect.ClassTag

/**
 * Author: shaoff
 * Date: 2020/5/25 17:19
 * Package: streaming
 * Description:
 *
 */
class DStreamByDB[T:ClassTag](collection:Seq[T], ssc: StreamingContext) extends InputDStream[T](ssc) {
  override def start(): Unit = {
  }
  override def stop(): Unit = {
  }

  override def compute(validTime: Time): Option[RDD[T]] = {
    None
//    Option(ssc.sparkContext.parallelize(collection))
  }
}

object TestDStreamByDB {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("TestDStreamByDB")
    val ssc = new StreamingContext(conf, Seconds(10))
    val stream1: DStream[Int] = new DStreamByDB[Int]((1 to 10).toList,ssc).map(_+1)
    stream1.foreachRDD((rdd, time) => {
      println(rdd.collect().mkString(","))
    })


    val stream2: DStream[Int] = new DStreamByDB[Int]((10 to 15).toList,ssc).map(_+1)
    stream2.foreachRDD((rdd, time) => {
      println(rdd.collect().mkString(","))
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
