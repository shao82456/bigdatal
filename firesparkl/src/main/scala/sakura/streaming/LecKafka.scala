package sakura.streaming

import com.homework.da.format.DataTable
import com.homework.da.util.JsonUtil
import org.apache.spark.streaming.StreamingContext
import org.fire.spark.streaming.core.plugins.kafka.KafkaDirectSource
import org.fire.spark.streaming.core.{FireStreaming, Logging}

object LecKafka extends FireStreaming with Logging {
  override def handle(ssc: StreamingContext): Unit = {
    val source: KafkaDirectSource[String, String] = new KafkaDirectSource[String, String](ssc)
    val lines = source.getDStream(_.value())

    lines.foreachRDD((rdd, time) => {
      rdd.foreachPartition(iter => compute(iter))
      source.updateOffsets(time.milliseconds)
    })
  }

  def compute(tables: Iterator[String]) = {
    log.info("compute begin")
    val total = tables.size
    val ts = 6 * 1000
    Thread.sleep(ts)
    log.info("compute {}", total)
  }
}
