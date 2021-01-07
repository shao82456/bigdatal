package streaming

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

/**
 * Author: shaoff
 * Date: 2020/9/1 14:45
 * Package: streaming
 * Description:
 *
 * 测试MetricReporter
 */
object kafkaStreamingDemo {
  def main(args: Array[String]): Unit = {
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val config = new Configuration()
    config.setString("rest.port", "8999")
    config.setString("metrics.reporters", "jmx,slf4j,flink_collector")
    config.setString("metrics.reporter.jmx.port", "8789")
    config.setString("metrics.reporter.jmx.factory.class", "org.apache.flink.metrics.jmx.JMXReporterFactory")

    config.setString("metrics.reporter.slf4j.class", "org.apache.flink.metrics.slf4j.Slf4jReporter")
    config.setString("metrics.reporter.slf4j.interval", "10 SECONDS")


//    config.setString("metrics.reporter.flink_collector.class", "org.apache.flink.metrics.kafka.KafkaCollectorReporter")
//    config.setString("metrics.reporter.flink_collector.interval", "30 SECONDS")
//    config.setString("metrics.reporter.flink_collector.topic", "test_rule")
//    config.setString("metrics.reporter.flink_collector.bootstrap.servers", "localhost:9092")
//    config.setString("metrics.reporter.flink_collector.key.serializer", "fc.shaded.org.apache.kafka.common.serialization.StringSerializer")
//    config.setString("metrics.reporter.flink_collector.value.serializer", "fc.shaded.org.apache.kafka.common.serialization.StringSerializer")


    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)

    /*val setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, setting)
*/
    env.setParallelism(3)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setGlobalJobParameters(params)

    import org.apache.flink.streaming.api.CheckpointingMode
    // start a checkpoint every 1000 ms// start a checkpoint every 1000 ms

    env.enableCheckpointing(1000)
    // advanced options:
    // set mode to exactly-once (this is the default)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)


    val kafkaProps = new Properties()
    kafkaProps.put("topic", "test_shao")
//    kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
//    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("bootstrap.servers", "localhost:9092")
    kafkaProps.put("group.id","test_shao.g1")
    kafkaProps.put("enable.auto.commit", "true")
    kafkaProps.put("auto.commit.interval.ms", "5000")

    val kafka: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010[String](kafkaProps.getProperty("topic"), new SimpleStringSchema(), kafkaProps)

    // get input data
    val text: DataStream[(String, Int)] = env.addSource[String](kafka)
      .flatMap(_.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
      .sum(1)

    text.print()

    env.execute("kafka streaming demo")
  }
}
