package streaming

import java.util.Properties

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis
import util.JedisUtil

/**
 * Author: shaoff
 * Date: 2020/5/28 17:33
 * Package: streaming
 * Description:
 *
 */
object KafkaSourceJoinJedis {
  private val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setGlobalJobParameters(params)

    // get input data
    val topic = params.get("topic", "test_rule")
    val brokers = params.get("brokers", "localhost:9092")
    log.info(s"Reading from $topic $brokers")

    val kafkaProps: Properties = new Properties
    kafkaProps.setProperty("bootstrap.servers", brokers)

    val kafka: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema(), kafkaProps)
    kafka.setStartFromLatest()

    val dataStream: DataStream[String] = env.addSource[String](kafka).flatMap(_.split("\\s+"))

    /*join redis*/
   /* val joinedStream = dataStream.map(new RichMapFunction[String, (String, String)] {
      var jedis: Jedis = _

      override def open(parameters: Configuration): Unit = {
        jedis = JedisUtil.connect(params.get("redis", "localhost:6379"))
      }

      override def map(value: String): (String, String) = {
        jedis.hset("test_len", value, value.length.toString)
        (value, jedis.hget("test_len", value))
      }
    })*/
dataStream.print()
//    joinedStream.writeAsText()
//    joinedStream.addSink(new PrintSinkFunction[(String,String)](true))
    /*dataStream.partitionCustom(new Partitioner[String] {
      override def partition(k: String, i: Int): Int = {
        if (k.length() == 0) 0 else k.charAt(0) % i
      }
    }, data => data)*/

    //get output data
//    joinedStream.writeAsText()

    //      dataStream.writeAsText("output", WriteMode.OVERWRITE)
    env.execute("kafka demo2")
  }
}
