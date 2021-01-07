package metric

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
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

    val config = new Configuration()
    config.setString("metrics.reporter.jmx.factory.class", "org.apache.flink.metrics.jmx.JMXReporterFactory")
    config.setString("rest.port", "8999")
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)

    // set up the execution environment
    val setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, setting)
    env.setParallelism(4)

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    // get input data
    val text: DataStream[String] = env.socketTextStream("localhost", 8890)
      .flatMap(_.split("\\s+")).filter(_.nonEmpty)
      .map(_.toUpperCase)

    text.print()

    env.execute("streaming demo")
  }
}