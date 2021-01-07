package tmp

import scala.util.matching.Regex
/**
 * Author: shaoff
 * Date: 2020/9/27 20:03
 * Package: tmp
 * Description:
 *
 */
object TmpReg {

  def main(args: Array[String]): Unit = {
    val reg = new Regex("(.*driver_|.*_\\d+_)(.+)")

    val s1="application_1600933308957_0349_12_kafkaConsumer_test_rule-0_outgoing-byte-rate"
    val res=reg.replaceAllIn(s1,"$2")
    println(res)


  }
}
