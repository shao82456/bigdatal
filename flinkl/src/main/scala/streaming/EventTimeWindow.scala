package streaming

/**
 * Author: shaoff
 * Date: 2020/7/30 11:39
 * Package: streaming
 * Description:
 *
 * EventTimeWindow 测试
 */

import java.text.SimpleDateFormat

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment

class BoundedOutOfOrdernessGenerator extends AssignerWithPeriodicWatermarks[(String, Long)] {

  private val maxOutOfOrderness = 0L // 3.5 seconds
  private var currentMaxTimestamp: Long = 0L
  private val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  private var waterMark : Watermark = _

  override def extractTimestamp(t: (String, Long), previousElementTimestamp: Long): Long = {
    val timestamp = t._2
    currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
    println("timestamp:" + t._1 +","+ t._2 + "|" +format.format(t._2) +","+  currentMaxTimestamp + "|"+ format.format(currentMaxTimestamp) + ","+ waterMark.toString)
    timestamp
  }

  override def getCurrentWatermark: Watermark = {
    // return the watermark as current highest timestamp minus the out-of-orderness bound
    waterMark=new Watermark(currentMaxTimestamp - maxOutOfOrderness)
    waterMark
  }
}

object EventTimeWindow {

  def main(args: Array[String]) {
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val config = new Configuration()
    config.setString("metrics.reporter.jmx.factory.class", "org.apache.flink.metrics.jmx.JMXReporterFactory")
    config.setString("rest.port", "8999")
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)
    //    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, setting)

    env.getConfig
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    // get input data
    val text: DataStream[String] = env.socketTextStream("localhost", 8890)
      .flatMap(_.split("\\s+"))
      .filter(_.nonEmpty)
      .map { wt =>
        val Array(word, ts) = wt.split(":")
        (word, ts.toLong)
      }.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator()).map(_._1)

    val upped = text.map(_.toUpperCase)
    upped.print()

    /*
    * 如下窗口计算时间为[xx:xx:0-xx:xx:30),[xx:xx:15-xx:xx:45),[xx:xx:30-xx:xx:0),[xx:xx:45-xx:xx:15)
    * 输入数据
    * 1. zzz:1596080701000 2020/7/30 11:45:1 // 该数据落入窗口[11:44:45-11:45:15) 与 [11:45:0-11:45:30)中，处理后waterMark更新为 11:45:1
    * 2. def:1596080703000 同上
      3. xyz:1596080716000 2020-07-30 11:45:16 // 该数据落入窗口 [11:45:0-11:45:30) 与 [11:45:15-11:45:45)中，处理后waterMark更新为 11:45:16
      此时waterMark 11:45:16 大于窗口 [11:44:45-11:45:15) 边界，触发计算 (zzz,1) (def,1)
      4. zzz:1596080731000 2020-07-30 11:45:31 //数据落入窗口 [11:45:15-11:45:45) 与 [11:45:30-11:46:0) 中，watermark更新为 11:45:31
      此时waterMark 11:45:31 触发了窗口 [11:45:0-11:45:30) 计算 (zzz,1) (def,1) (xyz,1)
    * */
    val res0: DataStream[(String, Int)] = upped.map((_, 1)).keyBy(0)
      .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(15)))
      .sum(1)

    /*
    如下窗口计算时间为: [xx:xx:5]-[xx:xx:35), [xx:xx:35]-[xx:xx:05)
    窗口触发规则为：窗口有数据且当前task的water>windowEnd
    如输入数据:
    1. zzz:1596080701000 2020/7/30 11:45:1 // 该数据落入窗口[11:44:35-11:45:05)中，处理后waterMark更新为 11:45:1
    2. aaa:1596080706000 2020/7/30 11:45:6 // 该数据落入窗口[11:45:05:-11:45:35)中，处理后waterMark更新为 11:45:6
    此时watermark 11:45:6 大于[11:44:35-11:45:05) 的windowEnd,聚合为(zzz,1)
    3. ccc:1596080736000 2020/7/30 11:45:36 // 该数据落入窗口[11:45:35:-11:46:05)中，处理后waterMark更新为 11:45:36
    此时watermark 11:45:36 大于[11:45:05:-11:45:35)的windowEnd,聚合为(aaa,1)
    4. ccc:1596080737000
      ccc:1596080802000
    此时输出(ccc,2) */

    val res1: DataStream[(String, Int)] = upped.map((_, 1)).keyBy(0)
//      .window(TumblingEventTimeWindows.of(Time.seconds(30),Time.seconds(5)))
      .window(TumblingEventTimeWindows.of(Time.seconds(30)))
      .sum(1)
    /*   val res2: DataStream[(String, Int)] = upped.map((_, 1)).keyBy(0)
   //      .timeWindow(Time.seconds(10),Time.seconds(5))
         .window(slidingEventTimeWindows(Time.seconds(60), Time.seconds(10)))
         .sum(1)*/

    upped
    val res2: DataStream[(String, Int)] = upped.map((_, 1)).keyBy(0)
//      .window(TumblingEventTimeWindows.of(Time.seconds(60),Time.seconds(5)))
            .window(TumblingEventTimeWindows.of(Time.seconds(60)))

      .sum(1)

    res0.print()
    env.execute("StreamingTable WordCount")
  }

}
