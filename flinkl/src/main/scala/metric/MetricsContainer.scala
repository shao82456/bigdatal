package metric

import com.codahale.metrics.ExponentiallyDecayingReservoir
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper
import org.apache.flink.metrics.{Metric, MetricGroup}

import scala.collection.mutable

/**
 * Author: shaoff
 * Date: 2020/6/23 20:05
 * Package: metrics
 * Description:
 * 将定义的指标引用统一放在这里
 * every metric is unique within the metricGroup which belongs to
 */

object MetricsContainer {
  val metrics = new mutable.HashMap[MetricGroup, mutable.HashMap[String, Metric]]()

  private def register(metricGroup: MetricGroup, name: String, metric: Metric): Unit = MetricsContainer.synchronized {
    val metricsInGroup = metrics.getOrElseUpdate(metricGroup, new mutable.HashMap[String, Metric]())
    assert(!metricsInGroup.contains(name), "metric not unique in metricGroup")
    metricsInGroup(name) = metric
  }

  def getMetric(metricGroup: MetricGroup, metricName: String): Option[Metric] = {
    metrics.get(metricGroup).flatMap(_.get(metricName))
  }

  //just for clean code
  def registerHistogram(metricGroup: MetricGroup, metricName: String): Unit = {
    val metric = metricGroup.histogram(
      metricName,
      new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(
          new ExponentiallyDecayingReservoir())))
    register(metricGroup, metricName, metric)
  }

}
