package streaming

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._


/**
 * Author: shaoff
 * Date: 2020/6/22 17:53
 * Package: streaming
 * Description:
 *
 */
object Demo {

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

    env.execute("streaming demo")
  }
}