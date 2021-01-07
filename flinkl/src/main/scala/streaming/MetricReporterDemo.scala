package streaming

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Author: shaoff
 * Date: 2020/9/1 14:45
 * Package: streaming
 * Description:
 *
 * 测试MetricReporter
 */
object MetricReporterDemo {
  def main(args: Array[String]): Unit = {
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val config = new Configuration()
    config.setString("metrics.reporters", "jmx,slf4j,flink_collector")
    config.setString("metrics.reporter.jmx.port", "8789")
    config.setString("metrics.reporter.jmx.factory.class", "org.apache.flink.metrics.jmx.JMXReporterFactory")

    config.setString("metrics.reporter.slf4j.class", "org.apache.flink.metrics.slf4j.Slf4jReporter")
    config.setString("metrics.reporter.slf4j.interval", "30 SECONDS")
    config.setString("rest.port", "8999")

    config.setString("metrics.reporter.flink_collector.class","org.apache.flink.metrics.kafka.KafkaCollectorReporter")
    config.setString("metrics.reporter.flink_collector.interval", "30 SECONDS")
    config.setString("metrics.reporter.flink_collector.topic", "test_rule")
    config.setString("metrics.reporter.flink_collector.bootstrap.servers", "localhost:9092")
    config.setString("metrics.reporter.flink_collector.randomJobNameSuffix", "true")
    config.setString("metrics.reporter.flink_collector.jobName","flink_local-")

    //    config.setString("metrics.reporter.flink_collector.topic", "test_rule")

    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)

    /*val setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, setting)
*/
    env.setParallelism(3)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setGlobalJobParameters(params)


    // get input data
    val text: DataStream[(String, Int)] = env.socketTextStream("localhost", 8890)
      .flatMap(_.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
      .sum(1)

    text.print()

    env.execute("metric_demo.shaoff")
  }
}
