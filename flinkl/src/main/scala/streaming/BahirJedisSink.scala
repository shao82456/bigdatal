package streaming

import java.util.Properties

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
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
object BahirJedisSink {
  private val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setGlobalJobParameters(params)

    // get input data
    val host = params.get("host", "localhost")
    val port = params.get("port", "8890").toInt
    log.info(s"Reading from $host:$port")

    val dataStream: DataStream[String] = env.socketTextStream(host,port).flatMap(_.split("\\s+"))

    val resStream=dataStream.map(a=>(a, a.toUpperCase))

  class RedisExampleMapper extends RedisMapper[(String, String)]{
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET, "HASH_NAME")
    }

    override def getKeyFromData(data: (String, String)): String = data._1

    override def getValueFromData(data: (String, String)): String = data._2
  }

    val redisSinkConfig = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build()
    resStream.addSink(new RedisSink[(String, String)](redisSinkConfig, new RedisExampleMapper))

    /*dataStream.partitionCustom(new Partitioner[String] {
      override def partition(k: String, i: Int): Int = {
        if (k.length() == 0) 0 else k.charAt(0) % i
      }
    }, data => data)*/

    //get output data
//    joinedStream.writeAsText()

    //      dataStream.writeAsText("output", WriteMode.OVERWRITE)
    env.execute("kafka demo")
  }
}
