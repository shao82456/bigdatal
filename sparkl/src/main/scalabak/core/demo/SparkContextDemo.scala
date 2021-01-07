package core.demo

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: shaoff
 * Date: 2020/5/29 22:14
 * Package: core.demo
 * Description:
 * val rdd1=sourceRdd.filter("xxx1").count
 * val rdd2=sourceRdd.filter("xxx2").count
 * 想做到上述方案，直接做法不可能，RDD不能直接拆分两个
 */

object SparkContextDemo {

  def testOtherMaster(): Unit ={
    val conf = new SparkConf()
      .setMaster("myDummyLocalExternalClusterManager")
      .setAppName("test-executor-allocation-manager")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.testing", "true")

    val sparkContext=new SparkContext(conf)
//    val rdd = sparkContext.textFile("file:/Users/sakura/sh").flatMap{line=>
//      line.split("\\s+")
//    }
  }

  def main(args: Array[String]): Unit = {
    testOtherMaster()
    Thread.sleep(1000*60*3)
  }
}
