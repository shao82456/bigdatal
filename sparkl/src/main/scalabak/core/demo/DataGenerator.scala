package core.demo


import scala.util.Random

/**
 * Author: shaoff
 * Date: 2020/5/29 22:16
 * Package: core.demo
 * Description:
 *
 */

case class Student(id: Int, name: String, birthday: String,grade:Int)

object DataGenerator {
  def prepareData(count: Int): Seq[Student] = {
    for (i <- 0 until count) yield {
      Student(i, randomName(),
        (Random.nextInt(100) + 1900) + "-" + (Random.nextInt(12) + 1) + "-" + (Random.nextInt(31) + 1),
        Random.nextInt(3)+1)
    }
  }

  def randomName(): String ={
    val chars=for(i<-0 to Random.nextInt(3)+3) yield (Random.nextInt(26)+97).toChar
    chars.mkString("")
  }

  def main(args: Array[String]): Unit = {
    prepareData(3).foreach(println)
  }
}
