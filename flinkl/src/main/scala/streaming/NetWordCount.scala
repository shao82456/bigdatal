package streaming

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
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

object NetWordCount {


  def main(args: Array[String]) {
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, setting)
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    // get input data
    val text: DataStream[String] = env.socketTextStream("localhost", 8890)
      .flatMap(_.split("\\s+"))
      .filter(_.nonEmpty)
      .map{ wt=>
        val Array(word,ts)=wt.split(":")
        (word,ts)
      }.assignAscendingTimestamps(_._2.toLong).map(_._1)

    val upped = text.map(_.toUpperCase)

    upped.print()

    val res1: DataStream[(String, Int)] = upped.map((_, 1)).keyBy(0)
      .window(slidingEventTimeWindows(Time.seconds(30), Time.seconds(15)))
      .sum(1)

 /*   val res2: DataStream[(String, Int)] = upped.map((_, 1)).keyBy(0)
//      .timeWindow(Time.seconds(10),Time.seconds(5))
      .window(slidingEventTimeWindows(Time.seconds(60), Time.seconds(10)))
      .sum(1)*/



    res1.print()

    env.execute("StreamingTable WordCount")
  }

  def slidingEventTimeWindows(size: Time, slide: Time) = {
     SlidingEventTimeWindows.of(size,slide)
//    new SlidingEventTimeWindows(size.toMilliseconds, slide.toMilliseconds, 0)
  }
}
