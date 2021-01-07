package org.apache.spark.serialization1.closure1

import java.io.{FileOutputStream, ObjectOutputStream}

import org.apache.spark.{SparkEnv, SparkException}

/**
 * Author: shaoff
 * Date: 2020/5/19 16:41
 * Package: org.apache.spark.serialization1.closure1
 * Description:
 *
 */
class SerializableValue extends Serializable{

}
class Outer5{
  val someSerializableValue = new SerializableValue()
  val test1=()=>{
    val localValue=someSerializableValue
    def getsomeSerializableValue: SerializableValue ={
      localValue
    }
    val inner1 = (x: Int) => x + localValue.hashCode()
    val inner2 = (x: Int) => x + getsomeSerializableValue.hashCode()

    Demo2.ensureSerializable(inner1)
    //need clean
     Demo2.ensureSerializable(inner2)
//    ClosureCleaner.clean(inner2, true)
//    Demo2.ensureSerializable(inner2)
  }
}

object Demo2{
  def main(args: Array[String]): Unit = {
    val outer=new Outer5
    outer.test1()
  }

  def ensureSerializable(func: AnyRef) {
    try {
      val fout=new ObjectOutputStream(new FileOutputStream("/dev/null"))
      fout.writeObject(func)
    } catch {
      case ex: Exception => throw new SparkException("Task not serializable", ex)
    }
  }
}
