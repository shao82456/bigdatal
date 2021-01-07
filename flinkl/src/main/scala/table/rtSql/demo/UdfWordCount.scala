package table.rtSql.demo

import metric.MetricsContainer
import metric.interceptors.HistogramTimerMI
import net.sf.cglib.proxy.Enhancer
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.scala._
import org.apache.flink.metrics.Histogram
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.{FunctionContext, TableFunction}
import table.rtSql.BaseLauncher
import table.rtSql.util.MetricUtil
import table.sink.PrintSink

/**
 * Author: shaoff
 * Date: 2020/6/22 16:54
 * Package: table.rtSql.demo
 * Description:
 * 测试flink udf
 */

object UdfWordCount extends BaseLauncher {

  // must be defined in static/object context
  class Split(separator: String) extends TableFunction[String] {
    var durationHistogram: Histogram = _
    var fcontext: FunctionContext = _

    def eval(str: String): Unit = {
      val duration = MetricUtil.timer {
        str.split(separator).foreach(x => collect(x))
      }
      MetricsContainer.getMetric(fcontext.getMetricGroup, "duration")
        .foreach(m => m.asInstanceOf[Histogram].update(duration))
    }

    def eval0(str: String): Unit = {
      str.split(separator).foreach(x => collect(x))
    }

    override def open(context: FunctionContext): Unit = {
      fcontext = context
      MetricsContainer.registerHistogram(context.getMetricGroup, "duration")
    }
  }

  override def jobName: String = "UdfWordCount"

  override def registerUdf(): Unit = {
    tableEnv.registerFunction("my_split", new Split("\\W+"))
  }

  override def addSource(): Unit = {
    val text: DataStream[String] = env.socketTextStream("localhost", 8891)
    tableEnv.registerDataStream("tsource", text, 'line, 'proctime.proctime)
  }


  override def addSink(): Unit = {
    tableEnv.registerTableSink("tprint", new PrintSink().configure(
      Array("word", "count"), Array(Types.STRING, Types.INT)
    ))
  }

  override def handle(args: Array[String]): Unit = {

    query("twords",
      """
        |select word,1 as freq,proctime from tsource,
        |LATERAL TABLE(my_split(line)) as T(word)
        |""".stripMargin)

    query("word_freq",
      """
        |select word,sum(freq) as freq
        |from twords
        |group by TUMBLE(proctime,INTERVAL '30' SECOND),word
        |""".stripMargin)

    update(
      """
        |insert into tprint
        |select * from word_freq
        |""".stripMargin)
  }
}
