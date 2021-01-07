package table.rtSql.demo

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.DataStream
import table.rtSql.BaseLauncher
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.sinks.CsvTableSink
/**
 * Author: shaoff
 * Date: 2020/6/22 16:38
 * Package: table.rtSql.demo
 * Description:
 *
 */
object SocketWordCount extends BaseLauncher {
  override def jobName: String = "SocketWordCount"

  override def registerUdf(): Unit = {
    //no udf used
  }

  override def addSource(): Unit = {
    val text: DataStream[String] = env.socketTextStream("localhost", 8890).flatMap(_.split("\\W+"))
    tableEnv.registerDataStream("tsource", text, 'word, 'proctime.proctime)
  }

  override def addSink(): Unit = {
    val fieldNames = Array("word","freq","created_time")
    val fieldTypes = Array[TypeInformation[_]](Types.STRING,Types.INT,Types.LOCAL_DATE_TIME)
    val csvSink = new CsvTableSink("/tmp/flink_demo/output", "|", -1, WriteMode.OVERWRITE).
      configure(fieldNames, fieldTypes)
    tableEnv.registerTableSink("tsink", csvSink)
  }

  override def handle(args: Array[String]): Unit = {
    env.setParallelism(4)
    query("tword",
      """
        |select *,1 as freq from tsource
        |""".stripMargin)

    query("word_freq",
      """
        |select word,sum(freq) as freq
        |from tword
        |group by TUMBLE(proctime,INTERVAL '30' SECOND),word
        |""".stripMargin)

    update(
      """
        |insert into tsink
        |select *,NOW() from word_freq
        |""".stripMargin)
  }
}
