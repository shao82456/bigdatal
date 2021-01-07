package table.rtSql.demo
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.functions.{ScalarFunction, TableFunction}
import table.rtSql.BaseLauncher
import table.rtSql.demo.SocketWordCount.{env, tableEnv}
import table.sink.PrintSink

import scala.util.Try

/**
 * Author: shaoff
 * Date: 2020/6/22 16:54
 * Package: table.rtSql.demo
 * Description:
 * 测试flink udf
 */

case class Person(name:String,gender:String,age:Int)

object UdfDemo extends BaseLauncher{
  // must be defined in static/object context
  class HashCode(bucket: Int) extends ScalarFunction {
    def eval(s: String): Int = {
      s.hashCode() % bucket
    }
  }

  class Split(separator: String) extends TableFunction[(String, Int)] {
    def eval(str: String): Unit = {
      // use collect(...) to emit a row.
      str.split(separator).foreach(x => collect((x, x.length)))
    }
  }

  override def jobName: String = "UdfDemo"

  override def registerUdf(): Unit = {
    tableEnv.registerFunction("hash",new HashCode(2))
  }

  override def addSource(): Unit = {
    val text: DataStream[Person] = env.socketTextStream("localhost", 8890).map{line=>
        val fields=line.split(",")
        Try{
          Some(Person(fields(0),fields(1),fields(2).toInt))
        }.getOrElse(None)
    }.filter(_.isDefined).map(_.get)
    tableEnv.registerDataStream("tsource", text)
  }

  override def addSink(): Unit = {
    val fieldNames = Array("name","gender","age")
    val fieldTypes = Array[TypeInformation[_]](Types.STRING,Types.INT,Types.INT)
    tableEnv.registerTableSink("tprint",new PrintSink().configure(fieldNames,fieldTypes))
  }

  override def handle(args: Array[String]): Unit = {
    update(
      """
        |insert into tprint
        |select name,hash(gender) as gender,age from tsource
        |""".stripMargin)
  }
}
