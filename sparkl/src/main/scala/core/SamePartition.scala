package core

import common.SparkLocalApp


/**
 * Author: shaoff
 * Date: 2020/7/28 15:56
 * Package: core.demo
 * Description:
 *
 */


object SamePartition extends SparkLocalApp {

  case class Student(name: String, score: Double, grade: Int)

  override def __main(args: Array[String]): Unit = {
    val data = List(Student("wang", 90, 1),
      Student("lee", 30, 2),
      Student("yang", 60, 1),
      Student("zhang", 60, 3),
      Student("huang", 60, 1)
    )

    val rdd1=sc.parallelize(List(1, 2, 3))
    val rdd2=sc.parallelize(List(4,5)).mapPartitions(it=>{
      println("once")
      it
    })

    rdd1.cartesian(rdd2).count()
//    val rdd = sc.parallelize(data).map(st => (st.grade, st))
//    val mapRdd = shuffledRDD.reduceByKey(shuffledRDD.partitioner.get, _ ++ _)
//    println(mapRdd)
  }
}
