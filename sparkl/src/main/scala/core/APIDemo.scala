package core

import core.SamePartition.{Student, sc}
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * Author: shaoff
 * Date: 2020/11/23 05:33
 * Package: core
 * Description:
 *
 */
object APIDemo {
  def main(args: Array[String]): Unit = {
    val data = List(Student("wang", 90, 1),
      Student("lee", 30, 2),
      Student("yang", 60, 1),
      Student("zhang", 60, 3),
      Student("huang", 60, 1),
        Student("shao", 60, 1)
    )

    val conf = new SparkConf()
    conf.setMaster("local[3]")
    conf.setAppName("APIDemo")
    val sc = SparkContext.getOrCreate(conf)

    val rdd = sc.parallelize(data).map(st => (st.grade, st))

    val createCombiner= (s:Student)=> {
      println("createCombiner called")
      List(s)
    }

    val mergeValue= (ls:List[Student],s:Student)=> {
      println("mergeValue called")
      s::ls
    }
    val mergeCombiner = (ls1: List[Student], ls2: List[Student]) => {
      println("mergeCombiner called")
      ls1 ::: ls2
    }

    /*2:(1,Student(huang,60.0,1)),(1,Student(shao,60.0,1))
    1:(1,Student(yang,60.0,1)),(3,Student(zhang,60.0,3))
    0:(1,Student(wang,90.0,1)),(2,Student(lee,30.0,2))*/


    val res=rdd.combineByKeyWithClassTag(createCombiner,mergeValue,mergeCombiner,Partitioner.defaultPartitioner(rdd),false)
//    res.count()
//    val shuffledRDD = rdd.groupByKey()
//    val mapRdd = shuffledRDD.reduceByKey(shuffledRDD.partitioner.get, _ ++ _)
//
//    mapRdd.foreach(println)


    val rdd1=sc.parallelize(List(1, 2, 3),3)
    val rdd2=sc.parallelize(List(4,5),2).mapPartitionsWithIndex((k,v)=>{
      println(k+",xonce")
      v
    })

    rdd1.cartesian(rdd2).count()
    Thread.sleep(1000*60)
  }
}
