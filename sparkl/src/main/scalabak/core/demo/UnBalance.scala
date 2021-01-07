package core.demo

import core.SparkApp
import org.apache.spark.storage.StorageLevel

import scala.util.Random
import util.JsonUtil

/**
 * Author: shaoff
 * Date: 2020/7/30 09:40
 * Package: core.demo
 * Description:
 * 数据倾斜问题
 */
object UnBalance extends SparkApp {
  override def __main(args: Array[String]): Unit = {
    val conf = sc.getConf
    val input = conf.get("spark.usr.input.path", "/user/bigdata/shaofengfeng/test_data")

    val sourceRdd = sc.textFile(input).filter(line=>Random.nextBoolean()).persist(StorageLevel.OFF_HEAP)

    val unBalanceRdd = sourceRdd.map { line =>
      (randomKey(), line)
    }

    //聚合场景
    val res=unBalanceRdd.reduceByKey { (a, b) =>
      val ma = JsonUtil.toAny[Map[String, Any]](a)
      val mb = JsonUtil.toAny[Map[String, Any]](b)

      if (ma("update_time").toString.toLong > mb("update_time").toString.toLong) {
        JsonUtil.toJson(ma)
      } else {
        JsonUtil.toJson(mb)
      }
    }

    res.foreach(println)
  }

  def randomKey(): Int = {
    val key1 = Random.nextInt(50)
    val key2 = Random.nextInt(3)
    if (Random.nextBoolean()) {
      key1
    } else {
      key2
    }
  }
}
