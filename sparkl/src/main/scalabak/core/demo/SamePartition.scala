package core.demo

import core.SparkApp

/**
 * Author: shaoff
 * Date: 2020/7/28 15:56
 * Package: core.demo
 * Description:
 *
 */
object SamePartition extends SparkApp{
  case class Student(name:String,score:Double,grade:Int)
  override def __main(args: Array[String]): Unit = {
    val data=List(Student("wang",90,1),
      Student("lee",30,2),
      Student("yang",60,1)
    )
    val rdd=sc.parallelize(data).map(st=>(st.grade,st)

  }
}
