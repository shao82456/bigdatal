package common

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: shaoff
 * Date: 2020/7/17 14:13
 * Package:
 * Description:
 *
 */
trait SparkLocalApp {
  var spark: SparkSession = _
  var sc: SparkContext = _

  def conf(args: Array[String]): SparkConf = {
    val conf = new SparkConf()
    if (!conf.contains("spark.app.name")) {
      conf.setAppName(this.getClass.getName)
      conf.setMaster("local[4]")
    }
    conf
  }

  def main(args: Array[String]): Unit = {
    spark = SparkSession.builder().config(conf(args)).getOrCreate()
    sc = spark.sparkContext
    __main(args)
    keepAlive()
  }

  def __main(args: Array[String]): Unit

  def keepAlive(): Unit = {
    Thread.sleep(sc.getConf.get("spark.keepAlive.sec", "0").toInt * 1000)
  }
}
