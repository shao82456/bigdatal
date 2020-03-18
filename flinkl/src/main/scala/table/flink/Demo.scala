package table.flink


import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.sinks.{CsvTableSink, TableSink}
import org.apache.flink.table.sources.{CsvTableSource, TableSource}
import org.apache.flink.types.Row

import scala.io.Source

/**
 * Author: shaoff
 * Date: 2020/2/24 14:53
 * Package: table.flink
 * Description:
 *
 */
object Demo {
  def createTemporalTable(ddl: String, tEnv: StreamTableEnvironment): Unit = {
    tEnv.sqlUpdate(ddl)
  }

  def addSource(tEnv: StreamTableEnvironment): Unit = {
    // create a TableSource
    val fieldNames: Array[String] = Array("actor_id", "first_name", "last_name")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.INT, Types.STRING, Types.STRING)
    val csvSource: TableSource[Row] = new CsvTableSource("/Users/sakura/stuff/flinkl/src/main/scala/table/flink/data", fieldNames, fieldTypes)

    // register the TableSource as table "CsvTable"
    tEnv.registerTableSource("CsvTable", csvSource)
  }

  def addKafkaSource(tEnv: StreamTableEnvironment):Unit={
    val ddl=Source.fromFile("/Users/sakura/stuff/bigdatal/flinkl/src/main/scala/table/flink/KafkaSource.ddl").mkString
    tEnv.sqlUpdate(ddl)
  }

  def addSink2(tEnv: StreamTableEnvironment): Unit = {
    val fieldNames: Array[String] = Array("actor_id", "first_name", "last_name")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.INT, Types.STRING, Types.STRING)

    val csvSink: TableSink[Row] = new CsvTableSink(
      "/Users/sakura/stuff/flinkl/src/main/scala/table/flink/res", "|", 1, FileSystem.WriteMode.OVERWRITE)

    tEnv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, csvSink)
  }

  def addSink(tEnv: StreamTableEnvironment): Unit = {
    val ddl =
      s"""
         |CREATE TABLE actor2(
         |  `actor_id` INT,
         |  `first_name` VARCHAR,
         |  `last_name` VARCHAR
         |) WITH (
         |  'connector.type' = 'filesystem',
         |  'connector.path' = 'file:///Users/sakura/stuff/bigdatal/flinkl/src/main/scala/table/flink/res',
         |  'format.type'='csv',
         |  'update-mode' = 'append',
         |  'format.fields.0.name' = 'actor_id',
         |  'format.fields.1.name' = 'first_name',
         |  'format.fields.2.name' = 'last_name',
         |  'format.fields.0.type' = 'INT',
         |  'format.fields.1.type' = 'VARCHAR',
         |  'format.fields.2.type' = 'VARCHAR'
         |)
         |""".stripMargin

    tEnv.sqlUpdate(ddl)
  }

  def main(args: Array[String]): Unit = {

    val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(fsEnv, bsSettings)

    addKafkaSource(tEnv)
    addSink(tEnv)
    createTemporalTable(Source.fromFile("/Users/sakura/stuff/bigdatal/flinkl/src/main/scala/table/flink/TemporalTable.ddl").mkString, tEnv)

    tEnv.sqlUpdate(
      s"""
         |insert into actor2
         |select a.actor_id,a.first_name,r.film_info
         |from (select *,PROCTIME() as proctime from actor)  AS a
         |JOIN actor_info FOR SYSTEM_TIME AS OF a.proctime AS r
         |ON r.actor_id = a.actor_id
         |""".stripMargin)

    /*tEnv.sqlUpdate(
      s"""
         |INSERT into actor2
         |select * from actor
         |""".stripMargin)*/
    fsEnv.execute()
  }
}
