package table.rtSql

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._

/**
 * Author: shaoff
 * Date: 2020/6/22 15:11
 * Package: table.rtSql
 * Description:
 * 模拟rtSQL，进行本地测试等
 *
 */
trait BaseLauncher {


  // set up the execution environment
  protected val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  protected val setting: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
  protected val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, setting)

  def jobName: String

  def registerUdf(): Unit

  def addSource(): Unit

  def addSink(): Unit

  def handle(args: Array[String]): Unit

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    env.getConfig.setGlobalJobParameters(params)

    //模版调用
    addSource()
    addSink()
    registerUdf()
    handle(args)

    env.execute(jobName)
  }

  def query(tableName: String, sql: String): Unit = {
    tableEnv.registerTable(tableName, tableEnv.sqlQuery(sql))
  }

  def update(sql: String): Unit = {
    tableEnv.sqlUpdate(sql)
  }
}
