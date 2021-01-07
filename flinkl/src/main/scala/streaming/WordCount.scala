package streaming

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.sinks.CsvTableSink
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

object WordCount {


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
    val text: DataStream[String] = env.socketTextStream("localhost", 8890).flatMap(_.split("\\W+"))

    text.print()
    tableEnv.registerDataStream("tsource", text, 'word, 'proctime.proctime)

    val tmp1 = tableEnv.sqlQuery(
      """
        |select tsource.*,1 as freq from tsource
        |""".stripMargin)
    tableEnv.registerTable("tword", tmp1)

    val tmp2 = tableEnv.sqlQuery(
      """
        |select word,sum(freq) as freq
        |from tword
        |group by TUMBLE(proctime, INTERVAL '30' SECOND),word
        |""".stripMargin)
    tableEnv.registerTable("word_freq", tmp2)

    val fieldNames = Array("word", "freq", "created_time")
    val fieldTypes = Array[TypeInformation[_]](Types.STRING, Types.INT, Types.LOCAL_DATE_TIME)
    val csvSink = new CsvTableSink("/tmp/flink_demo/output", "|", -1, WriteMode.OVERWRITE).
      configure(fieldNames, fieldTypes)
    tableEnv.registerTableSink("tsink", csvSink)

    tableEnv.sqlUpdate(
      """
        |INSERT INTO tsink
        |SELECT word_freq.*,NOW() from word_freq
        |""".stripMargin)

    env.execute("StreamingTable WordCount")
  }
}
