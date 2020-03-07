package sakura.test.structstreaming

import java.util.UUID

import com.homework.da.util.DateTimeUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.functions._
import scala.util.Random

/**
 * Author: shaoff
 * Date: 2020/3/5 16:32
 * Package: sakura.test.structstreaming
 * Description:
 *
 */
object DataFrameAPI {

  case class Student(id: String, name: String, grade: Int, class1: String)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TraceTest").setMaster("local[3]")
    val ssc = new StreamingContext(sparkConf, Seconds(30))
    val conf = ssc.sparkContext.getConf
    conf.set("spark.trace.hive.output", "hdfs://localhost:9000/tmp/trace")

    val lines: DStream[Student] = ssc.socketTextStream("localhost", 8890, StorageLevel.MEMORY_AND_DISK_SER)
      .map(Student(UUID.randomUUID().toString, _, Random.nextInt(8) + 1, (Random.nextInt(5) + 'A').toChar.toString))
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    lines.foreachRDD(rdd => {
      val df: DataFrame = rdd.toDF().withColumn("date",lit( DateTimeUtil.currentDateStr()))
      df.write.mode(SaveMode.Append)
        .option("path", conf.get("spark.trace.hive.output"))
        .partitionBy("date","class1")
        .option("delimiter","\t")
        .format("csv").save()
//      df.show()
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
