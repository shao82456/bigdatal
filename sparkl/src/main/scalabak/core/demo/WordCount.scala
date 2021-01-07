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
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("myMaster")
    conf.setAppName("wordcount")
    val sc = SparkContext.getOrCreate(conf)

    val rdd = sc.textFile("file:/Users/sakura/sh").flatMap{line=>
      line.split("\\s+")
    }.cache()

    rdd.count()

//   println( System.getProperty("java.io.tmpdir"))
//
//
//    val pairedRdd=rdd.map((_,1))
    val shuffledRDD=pairedRdd.reduceByKey(_+_)
//
//    shuffledRDD.persist(StorageLevel.DISK_ONLY)
//
//    println("count:"+shuffledRDD.count())
//    println("size:"+shuffledRDD.collect().size)
//
    Thread.sleep(1000*60*3)
  }
}
