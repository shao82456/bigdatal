//package table
//
//import org.apache.flink.api.java.utils.ParameterTool
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.api.scala._
//import org.apache.flink.table.api.EnvironmentSettings
//import org.apache.flink.table.api.scala.StreamTableEnvironment
//import org.apache.flink.table.functions.ScalarFunction
////import org.apache.flink.streaming.examples.wordcount.util.WordCountData
//
///**
// * Implements the "WordCount" program that computes a simple word occurrence
// * histogram over text files in a streaming fashion.
// *
// * The input is a plain text file with lines separated by newline characters.
// *
// * Usage:
// * {{{
// * WordCount --input <path> --output <path>
// * }}}
// *
// * If no parameters are provided, the program is run with default data from
// * {@link WordCountData}.
// *
// * This example shows how to:
// *
// *  - write a simple Flink Streaming program,
// *  - use tuple data types,
// *  - write and use transformation functions.
// *
// */
//
//object WordCount {
//  // must be defined in static/object context
//  class HashCode(factor: Int) extends ScalarFunction {
//    def eval(s: String): Int = {
//      s.hashCode() * factor
//    }
//  }
//
//  def main(args: Array[String]) {
//    // Checking input parameters
//    val params = ParameterTool.fromArgs(args)
//
//    // set up the execution environment
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
//    val tableEnv = StreamTableEnvironment.create(env, setting)
//
//
//    tableEnv.registerFunction("hashCode", new HashCode(10))
//    // make parameters available in the web interface
//    env.getConfig.setGlobalJobParameters(params)
//
//    // get input data
//    val text: DataStream[String] = env.socketTextStream("localhost",8890)
//
//    val counts: DataStream[(String, Int)] = text
//      // split up the lines in pairs (2-tuples) containing: (word,1)
//      .flatMap(_.toLowerCase.split("\\W+"))
//      .filter(_.nonEmpty)
//      .map((_, 1))
//      // group by the tuple field "0" and sum up tuple field "1"
//      .keyBy(0)
//      .sum(1)
////
//    // emit result
//    counts.print()
//    // execute program
//    env.execute("Streaming WordCount")
//  }
//}
