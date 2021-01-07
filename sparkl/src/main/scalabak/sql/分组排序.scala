package sql

package core.demo

import java.util.PriorityQueue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.util.Random


/**
 * Author: shaoff
 * Date: 2020/3/27 00:51
 * Package: core.demo
 * Description:
 *
 * 使用spark进行分组排序
 */
object 分组排序 {

  case class Student(name: String, class1: String, score: Int) extends Comparable[Student] {
    override def compareTo(o: Student): Int = score - o.score
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("分组排序")

    val sc = new SparkContext(sparkConf)
    val data = generateData(300)
    val rdd = sc.parallelize(data)
    val pairRdd = rdd.map(st => (st.class1, st))


    val resRdd0 = solution1(pairRdd)
    resRdd0.foreach(println)
    Thread.sleep(1000*1000)

    /*timer{
      ()=>
      }
    }*/
  }

  def generateData(n: Int): List[Student] = {
    val class1s = Array("A", "B", "C", "D", "E")
    val rand = new Random()
    val data = for (i <- 0 until n) yield {
      Student(generateName(), class1s(rand.nextInt(5)), rand.nextInt(100))
    }
    data.toList
  }

  def generateName(): String = {
    val rand = new Random()
    (0 to rand.nextInt(2) + 3).map(_ => (rand.nextInt(26) + 'a').toChar).mkString
  }

  //利用groupByKey
  //groupByKey会取得key的所有数据，不会进行map端聚合
  def solution0(data: RDD[(String, Student)]): RDD[(String, List[Student])] = {
    data.groupByKey()
    val resRdd=data.groupByKey().map(
      pair => (pair._1, pair._2.toList.sortBy(st => st.score)(Ordering[Int].reverse).take(3))
    )
    resRdd
  }

  /*方式1，利用aggregateByKey聚合后排序
  * 但由于是还是先将各个组的数据都进行了汇聚，因此和groupByKey无太大差别*/
  def solution1(data: RDD[(String, Student)]): RDD[(String, List[Student])] = {
    val seqOp = (ls: List[Student], st: Student) => st :: ls
    val combineOp = (ls1: List[Student], ls2: List[Student]) => ls1 ::: ls2
    val resRdd = data.aggregateByKey(List.empty[Student])(seqOp, combineOp).map(
      pair => (pair._1, pair._2.sortBy(st => st.score)(Ordering[Int].reverse).take(3))
    )
    resRdd
  }

  /*方式2,利用aggregateByKey边聚合边排序,
  * 可以利用堆优化取topN的问题，复杂度O(n*logK)*/
  def solution2(data: RDD[(String, Student)]): RDD[(String, List[Student])] = {

    val seqOp = (ls: PriorityQueue[Student], st: Student) => {
      if (ls.size() < 3) {
        ls.offer(st)
      } else if (st.compareTo(ls.peek()) > 0) {
        ls.poll()
        ls.offer(st)
      }
      ls
    }

    val combineOp = (ls1: PriorityQueue[Student], ls2: PriorityQueue[Student]) => {
      if (ls1.isEmpty) {
        ls2
      } else {
        val it = ls2.iterator()
        while (it.hasNext) {
          val st: Student = it.next()
          if (ls1.size() < 3) {
            ls1.offer(st)
          } else if (st.compareTo(ls1.peek()) > 0) {
            ls1.poll()
            ls1.offer(st)
          }
        }
        ls1
      }
    }
    val resRdd = data.aggregateByKey(new PriorityQueue[Student](3))(seqOp, combineOp).map(pair =>
      (pair._1, pair._2.asScala.toList)
    )
    resRdd
  }

//  //利用shuffle的排序后取前3
//    def solution3(data:RDD[(String,Student)]):RDD[(String, List[Student])]={
//        data.map(p=>(p._2.class1+":"+p._1,p._2))
//          .repartitionAndSortWithinPartitions(new HashPartitioner(data.getNumPartitions){
//            override def getPartition(key: Any): Int = super.getPartition(key.toString.split(":")(0))
//          }).mapPartitions(it=>{
//          it.aggregate()
//        })
//    }
}
