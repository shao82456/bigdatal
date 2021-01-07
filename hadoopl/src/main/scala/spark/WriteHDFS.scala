//package spark
//
//import org.apache.spark.sql.{SaveMode, SparkSession}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.functions._
//import util.DateTimeUtil
///**
// * Author: shaoff
// * Date: 2020/3/6 12:49
// * Package: spark
// * Description:
// * 了解如何在spark中写数据到hdfs
// * 依赖：只需要有spark依赖即可,spark-core中依赖了hadoop-client
// */
//object WriteHDFS {
//
//  case class DS(f1: String, f2: String)
//
//  def main(args: Array[String]): Unit = {
//    val sparkConf = new SparkConf().setAppName("WriteHDFS").setMaster("local[3]")
//
//    val sc = new SparkContext(sparkConf)
//    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
//    val rdd = sc.parallelize(List(
//      ("a", 1), ("a", 2), ("b", 2), ("c", 3), ("c", 4), ("d", 5), ("a", 6))
//    ).map(pair => DS(pair._1, pair._2.toString))
//
//
//    import spark.implicits._
//    val df = rdd.toDF.withColumn("date",lit(DateTimeUtil.currentDateStr()))
//
//    df.write.mode(SaveMode.Append)
//      .partitionBy("date","f1").csv("hdfs://localhost:9000/user/sakura/test/output")
//  }
//}
