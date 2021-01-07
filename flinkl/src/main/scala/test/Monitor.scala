package test

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{KeyedStream, _}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import test.RuleDuration.RuleDuration
import util.JsonUtil

import scala.collection.mutable.ListBuffer


object RuleDuration extends Enumeration {
  //这行是可选的，类型别名，在使用import语句的时候比较方便，建议加上
  type RuleDuration = Value
  //1分钟，3分钟，5分钟，告警计算仅定义3个聚合周期
  //绝大多数指标是15s上报一次，周期太短意义不大
  val ONE_M, THREE_M, FIVE_M,NONE = Value
}

case class Metric(name: String, value: Double, timestamp: Long, tags: Map[String, String])

case class GroupedMetrics(name: String, tags: Map[String, String], var duration: RuleDuration, var values: List[Double])

/*class ToListAggregate extends AggregateFunction[Metric, GroupedMetrics, GroupedMetrics] {
  override def createAccumulator(): GroupedMetrics = {
    null
  }

  override def add(m: Metric, accumulator: GroupedMetrics): GroupedMetrics = {
    if(accumulator==null){
      GroupedMetrics(m.name,m.tags)
    }
  }

  override def getResult(accumulator: List[Double]): List[Double] = {
    accumulator
  }

  override def merge(a: List[Double], b: List[Double]): List[Double] = {
    a ::: b
  }
}*/


object Monitor {


  def main(args: Array[String]) {
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, setting)
    env.setParallelism(1)

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    // get input data
    val text: DataStream[String] = env.socketTextStream("localhost", 8890).flatMap(_.split("\n"))
      .filter(_.nonEmpty)

    val projects = List("lec", "lpc")
    val metricStream: DataStream[GroupedMetrics] = text.map(JsonUtil.toAny[Metric](_))
      .filter(metric => projects.contains(metric.tags("project")))
      .assignAscendingTimestamps(_.timestamp)
      .map(metric => GroupedMetrics(metric.name, metric.tags, RuleDuration.NONE, List(metric.value)))

    metricStream.print()

    /*val keyedStream: KeyedStream[GroupedMetrics, String] = metricStream
      .keyBy(m => {
        m.name + "_" + m.tags.values.toList.sorted.mkString(",")
      }).window(slidingEventTimeWindows(Time.seconds(30), Time.seconds(10)))
      .reduce { (a, b) =>
        a.values = a.values ::: b.values
        a
      }.map { g =>
      g.duration = RuleDuration.THREE_M
      g
    }*/

    metricStream
      .keyBy(m => {
        m.name + "_" + m.tags.values.toList.sorted.mkString(",")
      }).window(slidingEventTimeWindows(Time.seconds(30), Time.seconds(3)))
      .reduce { (a, b) =>
        a.values = a.values ::: b.values
        a
      }.map { g =>
      g.duration = RuleDuration.THREE_M
      g
    }.print()

//    val res1: DataStream[GroupedMetrics] = aggToList(keyedStream,RuleDuration.ONE_M)
//    val res2: DataStream[GroupedMetrics] = aggToList(keyedStream,RuleDuration.THREE_M)
//    val res3: DataStream[GroupedMetrics] = aggToList(keyedStream,RuleDuration.FIVE_M)

//    val groupedMetric=res1.union(res2).union(res3)
//    groupedMetric.print()
//    res2.print()
    //规则判断
//    groupedMetric.join()
    env.execute("StreamingTable WordCount")
  }

  def slidingEventTimeWindows(size: Time, slide: Time) = {
    SlidingEventTimeWindows.of(size, slide)
    //    new SlidingEventTimeWindows(size.toMilliseconds, slide.toMilliseconds, 0)
  }

  def aggToList(keyedStream: KeyedStream[GroupedMetrics, String], ruleDuration: RuleDuration): DataStream[GroupedMetrics] = {
    val seconds = ruleDuration match {
      case RuleDuration.ONE_M => 10
      case RuleDuration.THREE_M => 30
      case RuleDuration.FIVE_M => 50
    }
    keyedStream
      .window(slidingEventTimeWindows(Time.seconds(30), Time.seconds(10)))
      .reduce { (a, b) =>
        a.values = a.values ::: b.values
        a
      }.map { g =>
      g.duration = ruleDuration
      g
    }
  }



}
