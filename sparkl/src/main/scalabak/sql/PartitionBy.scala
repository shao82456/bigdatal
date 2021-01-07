package sql

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import scala.util.Random

/**
 * Author: shaoff
 * Date: 2020/9/22 14:05
 * Package: sql
 * Description:
 *
 */

case class TraceData(id:Int,data:String,datatype:String,dt:String)

object PartitionBy {

  def main(args: Array[String]): Unit = {

    val conf= new SparkConf().setMaster("local[5]").setAppName("PartitionByDemo")

    val spark=SparkSession.builder().config(conf).getOrCreate()


    val data=(1 to 500).map(i=>TraceData(i,UUID.randomUUID().toString,(Random.nextInt(3)+65).toChar.toString,
    "2020091"+ Random.nextInt(3)))

    val output ="file:///Users/sakura/tmp/output"

    val targetPartitions = conf.get("spark.usr.targetPartitions", "3").toInt

    val schema: StructType = StructType(Seq(
      StructField("id", StringType, nullable = true),
      StructField("data", StringType, nullable = true),
      StructField("key1", StringType, nullable = true),
      StructField("key2", StringType, nullable = true),
      StructField("key3", StringType, nullable = true),
      StructField("key4", StringType, nullable = true),
      StructField("key5", StringType, nullable = true),
      StructField("ts", LongType, nullable = true),
      StructField("datatype", StringType, nullable = true)
    ))


    import spark.implicits._
    val source =spark.sparkContext.parallelize(data).toDF()

    source.repartition('dt,'datatype)
      .write.mode(SaveMode.Overwrite)
      .partitionBy("dt", "datatype")
      .option("path", output)
      .option("delimiter", "\t")
      .format("csv").save()

    Thread.sleep(1000*60*2)
  }
}
