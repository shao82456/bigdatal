//package es.read
//
//
//import core.common.SparkLocalApp
//import org.apache.spark.SparkConf
//import org.apache.spark.rdd.RDD
//import util.{HDFSUtil, JsonUtil}
//
//
///**
// * Author: shaoff
// * Date: 2020/7/17 14:10
// * Package: es.read
// * Description:
// * spark 读取 es
// */
//object LoadES extends common.SparkLocalApp {
//
//  import org.elasticsearch.spark.rdd.EsSpark._
//
//  override def conf(args: Array[String]): SparkConf = {
//    super.conf(args).set("spark.es.nodes","localhost")
//      .set("spark.es.port","9200")
//  }
//
//  override def __main(args: Array[String]): Unit = {
//    val conf = sc.getConf
//    val leadsRdd = load("trace_demo_output").map(JsonUtil.toJson)
//
////    val outputPath = conf.get("spark.usr.output")
////    HDFSUtil.delete_if_exist(outputPath)
////    leadsRdd.saveAsTextFile(outputPath)
//    leadsRdd.foreach(println)
//  }
//
//
//  import org.elasticsearch.spark._
//
//  def load(index: String, query: String): RDD[collection.Map[String, AnyRef]] = {
//    sc.esRDD(s"$index/_doc", query).map(_._2)
//  }
//
//  def loadJson(index: String, query: String): RDD[String] = {
//    sc.esJsonRDD(s"$index/_doc", query).map(_._2)
//  }
//
//  def load(index: String): RDD[collection.Map[String, AnyRef]] = {
//    sc.esRDD(s"$index/_doc").map(_._2)
//  }
//
//  //  val rdd = spark.sparkContext.esRDD(s"$esIndex/_doc", query).map(_._2)
//
//}
