package metric.interceptors

import java.lang.reflect.Method

import org.apache.flink.metrics.Histogram

/**
 * Author: shaoff
 * Date: 2020/6/23 22:04
 * Package: metric.interceptors
 * Description:
 *
 */
class HistogramTimerMI(metric: Histogram) extends TimerMI {
  override def update(duration: Long): Unit = {
    metric.update(duration)
  }

  override def whetherMetric(method: Method): Boolean = {
     method.getName == "eval"
  }
}
