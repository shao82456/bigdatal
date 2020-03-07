package sakura.streaming

import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * Created by cloud on 2019/03/06.
 */
object ConstantConfig {

  /*lec相关库名-表名*/
  val tbl_leads = ("homework_zhibo_laxincore", "tblleads")
  val tbl_leadsbatch = ("homework_zhibo_leads", "tblleadsbatch")
  val tbl_leadschannel = ("homework_zhibo_leads", "tblleadschannel")
  val tbl_leadslesson = ("homework_zhibo_tmk", "tblleadslesson")
  val tbl_tmkleads = ("homework_zhibo_tmk", "tbltmkleads")

  //仅包含需要的字段
  val leadsColumn = Array("tmk_uid",
    "batch_name",
    "leads_id",
    "tmk_alloc_time")

  val batchColumn = Array(
    "second_channel_id",
    "batch_name")

  val leadsTmkColumn=Array(
    "tmk_uid",
    "yueke_time",
    "attend_time",
    "status",
    "expire_time",
    "batch_name",
    "leads_id",
    "subscribe_time",
    "weixin_time",
    "trans_time",
    "create_time"
  )

  val leadsLessonColumn=Array(
    "tmk_uid",
    "start_time",
    "batch_name",
    "leads_id"
  )

  val callRecordColumn=Array(
    "id",
    "sale_uid",
    "start_time",
    "create_time",
    "duration",
    "leads_id",
    "role",
    "call_type"
  )

  val  dfSchema=(column:Array[String])=>
    StructType(column.map(fn => StructField(fn, StringType, nullable = true)))

  val leadsSchema = StructType(leadsColumn.map(fn => StructField(fn, StringType, nullable = true)))
  val batchSchema = StructType(batchColumn.map(fn => StructField(fn, StringType, nullable = true)))
  val leadsTmkSchema=StructType(leadsTmkColumn.map(fn => StructField(fn, StringType, nullable = true)))
  val leadsLessonSchema=StructType(leadsLessonColumn.map(fn => StructField(fn, StringType, nullable = true)))

  val leadsIndex = "tmk_leads"
  val leadsLessonIndex = "tmk_leads_lesson"
  val batchIndex = "tmk_batch"
  val leadsTmkIndex = "tmk_leads_tmk"
  val callRecordIndex = "tmk_call"


}
