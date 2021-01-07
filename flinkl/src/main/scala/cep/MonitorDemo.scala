package cep

import java.util

import org.apache.flink.api.scala._
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * Author: shaoff
 * Date: 2020/7/10 11:08
 * Package: cep
 * Description:
 *
 */

object MonitorDemo {

  case class AlertEvent(type1: String, msg: String, createdTime: Long = System.currentTimeMillis())

  case class BatchInfo(appId: String,
                       batchTime: Long,
                       submissionTime: Long,
                       numRecords: Long,
                       processingStartTime: Option[Long],
                       processingEndTime: Option[Long])

  def batchCongestion(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val line = env.socketTextStream("localhost", 8890)
    val input: DataStream[Int] = line.flatMap(_.split("\\W+")).filter(_.nonEmpty).map(_.toInt)

    //    val pattern = Pattern.begin[Int]("start").where(_ > 1).times(60).consecutive()
    val pattern1 = Pattern.begin[Int]("start").where(_ > 60)
    val pattern2 = Pattern.begin[Int]("start").where(_ > 600)

    /*.next("middle")
    .subtype(classOf[SubEvent]).where(_.getVolume >= 10.0)
    .followedBy("end").where(_.getName == "end")*/

    val patternStream1 = CEP.pattern(input, pattern1)
    val patternStream2 = CEP.pattern(input, pattern2)


    val result: DataStream[Int] = patternStream1.process(
      new PatternProcessFunction[Int, Int]() {
        override def processMatch(
                                   m: util.Map[String, util.List[Int]],
                                   ctx: PatternProcessFunction.Context,
                                   out: Collector[Int]): Unit = {
          println(m)
          out.collect(m.get("start").get(0))
        }
      })

    result.print()
    env.execute("cep batchInfo")
  }

  def batchInfoCongestion(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val line = env.socketTextStream("localhost", 8890)
    val input: DataStream[String] = line.flatMap(_.split("\\W+")).filter(_.nonEmpty)

    //    val pattern = Pattern.begin[Int]("start").where(_ > 1).times(60).consecutive()


    val start: Pattern[String, _] = Pattern.begin("start").where(_.startsWith("s"))
    val submitPat = Pattern.begin[String]("submit").where(_.startsWith("s"))
    val finishPat = Pattern.begin[String]("finish").where(_.startsWith("f"))

    //S F S S S F S S
    val congetstionPat = submitPat.oneOrMore
      .where((value, ctx) => {
        value.startsWith("s") && {
          val submitted = ctx.getEventsForPattern("submit").size
          val finished = ctx.getEventsForPattern("finish").size
          println(submitted - finished)
          submitted - finished > 3
        }
      })


    val patternStream1 = CEP.pattern(input, congetstionPat)
    //    val patternStream2 = CEP.pattern(input, pattern2)


    val result: DataStream[String] = patternStream1.process(
      new PatternProcessFunction[String, String]() {
        override def processMatch(
                                   m: util.Map[String, util.List[String]],
                                   ctx: PatternProcessFunction.Context,
                                   out: Collector[String]): Unit = {
          println(m)
          out.collect(m.get("start").get(0))
        }
      })

    result.print()
    env.execute("cep batchInfo")
  }

  def main(args: Array[String]): Unit = {
    batchInfoCongestion()
  }

}


