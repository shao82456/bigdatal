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
 * Date: 2020/7/2 15:05
 * Package: cep
 * Description:
 *
 * 批次积压
 */


object Demo {

  case class BatchInfo(appId: String,
                       batchTime: Long,
                       submissionTime: Long,
                       processingStartTime: Option[Long],
                       processingEndTime: Option[Long])

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val line = env.socketTextStream("localhost", 8890)
    val input: DataStream[Int] = line.flatMap(_.split("\\W+")).filter(_.nonEmpty).map(_.toInt)

    val pattern = Pattern.begin[Int]("start").where(_ > 1).times(10)
      .consecutive()

    /*.next("middle")
    .subtype(classOf[SubEvent]).where(_.getVolume >= 10.0)
    .followedBy("end").where(_.getName == "end")*/

    val patternStream = CEP.pattern(input, pattern)

    val result: DataStream[Int] = patternStream.process(
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


}
