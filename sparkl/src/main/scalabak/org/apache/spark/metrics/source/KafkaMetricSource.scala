package org.apache.spark.metrics.source

import java.util

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.apache.kafka.common.metrics.{KafkaMetric, MetricsReporter}
import org.apache.spark.SparkEnv
import org.apache.spark.metrics.source1.KafkaMetricWrapper

import scala.collection.JavaConverters._

/**
 * Author: shaoff
 * Date: 2020/9/22 15:55
 * Package: 
 * Description:
 *
 */


class KafkaMetricSource extends Source {
  override def sourceName: String = "KafkaConsumer"

  override def metricRegistry: MetricRegistry = new MetricRegistry()
}

class KafkaMetricWrapper(metric: KafkaMetric) extends Gauge[Double] {
  override def getValue: Double = metric.value()
}


class SparkMetricReporter extends MetricsReporter {

  override def init(metrics: util.List[KafkaMetric]): Unit = {
    val source = SparkEnv.get.metricsSystem.getSourcesByName("KafkaConsumer").head
    val metricRegistry = source.metricRegistry
    for (metric <- metrics.asScala) {
      metricRegistry.register(metric.metricName().name(), new KafkaMetricWrapper(metric))
    }
  }

  override def metricChange(metric: KafkaMetric): Unit = {

  }

  override def metricRemoval(metric: KafkaMetric): Unit = {

  }

  override def close(): Unit = {

  }

  override def configure(configs: util.Map[String, _]): Unit = {

  }
}
