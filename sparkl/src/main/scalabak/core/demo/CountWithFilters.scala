package core.demo

import java.util

import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.Random

/**
 * Author: shaoff
 * Date: 2020/5/29 22:14
 * Package: core.demo
 * Description:
 * val rdd1=sourceRdd.filter("xxx1").count
 * val rdd2=sourceRdd.filter("xxx2").count
 * 想做到上述方案，直接做法不可能，RDD不能直接拆分两个
 */
object CountWithFilters {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[3]")
    conf.setAppName("CountWithFilters")
    val sc = SparkContext.getOrCreate(conf)

    val rdd = sc.parallelize(DataGenerator.prepareData(30))
    val pairedRdd=rdd.map(st=>(st.grade,1))

    val countedRdd: ShuffledRDD[Any,Any,Any] =pairedRdd.reduceByKey(_+_).asInstanceOf[ShuffledRDD[Any,Any,Any]]
    countedRdd.getDependencies.foreach(println(_))
    val resRdd=countedRdd.filter(_._1==3)
    val res=resRdd.collect()
    res.foreach(println(_))
  }
}
