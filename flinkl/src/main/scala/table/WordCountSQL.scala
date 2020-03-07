//package table
//
//import org.apache.flink.api.scala._
//import org.apache.flink.table.api.scala._
//
///**
// * Author: shaoff
// * Date: 2020/2/20 15:47
// * Package: table
// * Description:
// *
// */
//object WordCountSQL {
//  def main(args: Array[String]): Unit = {
//    val env = ExecutionEnvironment.getExecutionEnvironment
//    val tEnv = BatchTableEnvironment.create(env)
//
//    val input = env.fromElements(WordCount("abc", 1), WordCount("abc", 1), WordCount("def", 2))
//    tEnv.createTemporaryView("WordCount", input)
//
//    val res = tEnv
//      .sqlQuery("select word,sum(frequency) as frequency from WordCount group by word")
//
//    res.toDataSet[WordCount].print()
//  }
//
//  case class WordCount(word: String, frequency: Int)
//
//}
