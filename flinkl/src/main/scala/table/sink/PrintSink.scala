package table.sink

import java.io.PrintStream

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.scala._
import org.apache.flink.table.sinks.{AppendStreamTableSink, TableSink}
import org.apache.flink.types.Row

/**
 * Author: shaoff
 * Date: 2020/6/22 17:07
 * Package: table.sink
 * Description:
 *
 */

class PrintSink(toErr:Boolean=false) extends AppendStreamTableSink[Row]{
  var fieldNames:Array[String]=Array("dummy")
  var fieldTypes:Array[TypeInformation[_]]=Array(Types.STRING)


  override def consumeDataStream(dataStream: datastream.DataStream[Row]): DataStreamSink[_] = {
    val printFunction = new PrintSinkFunction[Row]
    dataStream.addSink(printFunction).name("Print to Std. Out")
  }

  override def getOutputType: TypeInformation[Row] = {
    new RowTypeInfo(getFieldTypes, getFieldNames)
  }

  override def getFieldNames: Array[String] = {
    fieldNames
  }

  override def getFieldTypes: Array[TypeInformation[_]] = {
    fieldTypes
  }

  override def emitDataStream(dataStream: datastream.DataStream[Row]): Unit = {
    consumeDataStream(dataStream)
  }

  override def configure(fieldNames: Array[String], fieldTypes: Array[TypeInformation[_]]): TableSink[Row] = {
    val csvSink=new PrintSink()
    csvSink.fieldNames=fieldNames
    csvSink.fieldTypes=fieldTypes
    csvSink
  }
}

