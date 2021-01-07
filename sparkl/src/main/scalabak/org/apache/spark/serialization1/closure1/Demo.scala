package org.apache.spark.serialization1.closure1

import org.apache.spark.serializer.JavaSerializerInstance
import org.apache.spark.util.ClosureCleaner

/**
 * Author: shaoff
 * Date: 2020/5/19 16:31
 * Package: org.apache.spark.serialization1.closure1
 * Description:
 *
 */

class Bs { }

class As extends Serializable {
  val p: Int = 200
  val xx: Long = 1000L
  val bs = new Bs

  def ns[T](b: T => Int): Unit = {
    b.getClass.getDeclaredFields.foreach(println)
    //val x = b.getClass.getDeclaredField("$outer")
    //x.setAccessible(true)
    //println(x + " : " + x.get(b))
    ClosureCleaner.clean(b, false)
    val jsi = new JavaSerializerInstance(100, true, Thread.currentThread().getContextClassLoader)
    val bx = jsi.serialize(b)
    //val y = b.getClass.getDeclaredField("$outer")
    //y.setAccessible(true)
    //println(y + " : " + y.get(b))
  }
  def d: Unit = nss(x => {
//    val f = p
    ns[Long](y => y.toInt + 1 + x)
  })

  def nf(x: Long): Int = x.toInt

  def f: Unit = ns[Long](nf)

  def nss(b: Int => Unit): Unit = {
    ClosureCleaner.clean(b, true)
    val jsi = new JavaSerializerInstance(100, true, Thread.currentThread().getContextClassLoader)
    val bx = jsi.serialize(b)
    //b(0)
  }
}
object Demo {
  def main(args: Array[String]): Unit = {
    val as = new As
    as.d
  }
}
