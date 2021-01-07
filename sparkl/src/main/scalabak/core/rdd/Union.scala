package core.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: shaoff
 * Date: 2020/9/18 18:07
 * Package: core.rdd
 * Description:
 *
 */
object Union {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[3]")
    conf.setAppName("Union Demo")

    val sparkContext= SparkContext.getOrCreate(conf)

    val rdd1=sparkContext.parallelize(List(1,2),2)
    val rdd2=sparkContext.parallelize(List(7,8,9),3)

    val rdd=rdd1.union(rdd2)
    assert(5 == rdd.getNumPartitions)
  }

}
