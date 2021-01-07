package org.apache.spark.serialization1.closure1

import java.io.{FileOutputStream, ObjectOutputStream}

import org.apache.spark.util.ClosureCleaner

/**
 * Author: shaoff
 * Date: 2020/5/15 21:52
 * Package: org.apache.spark.serialization1
 * Description:
 *
 */

class Outer {
  val ls = List(1, 2, 3)
  val c = 2

  val f1: Int => Int = (a: Int) => {
    val tmpc=c
    a + tmpc
  }
  val f2: Int => Int = (a: Int) => a + 1
}

class SomethingNotSerializable {
  def someValue = 1

  def scope(name: String)(body: => Unit) = body
}

object Cleaner {
  def testOuter(): Unit = {
    val outer = new Outer
    val fout = new ObjectOutputStream(new FileOutputStream("/Users/sakura/stuff/stuff-projects/scalal/src/data"))
    val f = outer.f1
    ClosureCleaner.clean(f, true)
    //    println(f == cleanedF)l;;;;;
        fout.writeObject(f)
  }

  def testDemo(): Unit = {
    val obj = new SomethingNotSerializable()
    //    val cleanedF = ClosureCleaner.clean(obj.someMethod, true)
  }

  def main(args: Array[String]): Unit = {
    testOuter()
  }

  def testOuter2(): Unit = {
    val obj = new Outer2()
//    val cleanedF = ClosureCleaner.clean(obj.function2)
  }

}

class Outer2 {
  val a=1
  def f1(a:Char): Char = {
    Character.toUpperCase(a)
  }

  def f2():Unit={
    val ls=List("abc","def","ghi")
    ls.map(word=>{
      word.map(a=>f1(a))
    })
  }
}