package table

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction

/**
 * Author: shaoff
 * Date: 2020/2/20 15:47
 * Package: table
 * Description:
 *
 */

// must be defined in static/object context
class HashCode(factor: Int) extends ScalarFunction {
  def eval(s: String): Int = {
    s.hashCode() * factor
  }
}



object TimestampModifier extends ScalarFunction {
  def eval(t: Long): Long = {
    t % 1000
  }

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] = {
    Types.SQL_DATE()
  }
}



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
