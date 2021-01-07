package table.rtSql.util

import com.codahale.metrics.ExponentiallyDecayingReservoir
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper
import org.apache.flink.metrics.{Histogram, MetricGroup}

/**
 * Author: shaoff
 * Date: 2020/6/17 11:19
 * Package: com.zyb.bigdata.rtsql.util
 * Description:
 *
 */
object MetricUtil {

  /**
   * 代码段耗时统计
   *
   * @param f 代码段
   * @return duration, ms
   */
  def timer(f: => Unit): Long = {
    val st = System.currentTimeMillis()
    f
    System.currentTimeMillis() - st
  }

  //just for clean code
  def registerHistogram(context: RuntimeContext, metricName: String): Histogram = {
    registerHistogram(context.getMetricGroup,metricName)
  }

  //just for clean code
  def registerHistogram(metricGroup: MetricGroup, metricName: String): Histogram = {
    metricGroup.histogram(
      metricName,
      new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(
          new ExponentiallyDecayingReservoir())))
  }

  /*def registerGauge[T](context: RuntimeContext, metricName: String, _gauge: Gauge[T]): Gauge[T] = {
    context.getMetricGroup.gauge[T,Gauge[T]](
      metricName, _gauge)
  }*/


  def main(args: Array[String]): Unit = {
    val res = MetricUtil.timer {
      Thread.sleep(1000)
    }
    println(res)
  }



}
