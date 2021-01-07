package metric

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

object Container {
  val metrics = new mutable.HashMap[MetricGroup, mutable.HashMap[String, Metric]]()
}
