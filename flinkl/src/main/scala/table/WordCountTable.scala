package table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._

/**
 * Author: shaoff
 * Date: 2020/2/20 15:47
 * Package: table
 * Description:
 *
 */
object WordCountTable {
  def main(args: Array[String]): Unit = {
    val env=ExecutionEnvironment.getExecutionEnvironment
    val tEnv=BatchTableEnvironment.create(env)

    val input=env.fromElements(WordCount("abc",1),WordCount("abc",1),WordCount("def",2))

    val res=input.toTable(tEnv)
      .groupBy('word)
      .select('word,'frequency.sum as 'frequency)
      .filter('frequency ===2)
      .toDataSet[WordCount]

    res.print()
  }

  case class WordCount(word:String,frequency: Int)
}
