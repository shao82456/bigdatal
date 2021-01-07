package streaming

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
//import org.apache.flink.streaming.examples.wordcount.util.WordCountData

/**
 * Implements the "WordCount" program that computes a simple word occurrence
 * histogram over text files in a streaming fashion.
 *
 * The input is a plain text file with lines separated by newline characters.
 *
 * Usage:
 * {{{
 * WordCount --input <path> --output <path>
 * }}}
 *
 * If no parameters are provided, the program is run with default data from
 * {@link WordCountData}.
 *
 * This example shows how to:
 *
 *  - write a simple Flink Streaming program,
 *  - use tuple data types,
 *  - write and use transformation functions.
 *
 */

object NetSum {


  def main(args: Array[String]) {
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, setting)
    env.setParallelism(4)

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    // get input data
    val text: DataStream[String] = env.socketTextStream("localhost", 8890).flatMap(_.split("\\W+")).filter(_.isEmpty)

    text.print()
    val res: DataStream[Int] = text.map(_.toInt).keyBy(num => 0).sum(0)
    res.print()

    env.execute("StreamingTable WordCount")
  }
}
