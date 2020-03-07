//package sakura.streaming
//
//import com.homework.da.format.DataTable
//import com.homework.da.log.{ElasticTracing, TracedData}
//import org.apache.spark.streaming.StreamingContext
//import org.fire.spark.streaming.core.FireStreaming
//import org.fire.spark.streaming.core.plugins.kafka.KafkaDirectSource
//
//
//object TraceDemo extends FireStreaming {
//
//  override def handle(ssc: StreamingContext): Unit = {
//    val source = new KafkaDirectSource[String, String](ssc)
//    val lines = source.getDStream(_.value()).map(DataTable.str2dt)
//      .filter(dt => dt.TYPE == "I" || dt.TYPE == "U")
//    //driver中创建一个esTrace
//    val esTrace = new ElasticTracing(ssc.sparkContext.getConf)
//
//    lines.foreachRDD((rdd, time) => {
//      //记录一个数据集时先转成TracedData类型
//      val rdd1 = rdd.map(dataTable2TracedData(_))
//
//      //调用trace记录每条数据，用foreach循环或是map循环自行决定
////      rdd1.foreach(esTrace.trace(_, "index-name"))
//
//      //程序结束前需要flush
//      source.updateOffsets(time.milliseconds)
////      esTrace.flush
//    })
//  }
//
//  def dataTable2TracedData(dt: DataTable): TracedData = {
//    TracedData(dt.getNewValues().getOrElse("id", "0A").toString, dt, "key1", "key2", "key3", "", "")
//  }
//}
