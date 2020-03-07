package sakura.streaming

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}

import com.homework.da.format.DataTable
import com.homework.da.util.ESClientDAO._
import com.homework.da.util.ESConnectionPool._
import com.homework.da.util.{DateTimeUtil, JsonUtil}
import com.sksamuel.elastic4s.bulk.BulkCompatibleRequest
import com.sksamuel.elastic4s.http.{ElasticClient, ElasticProperties}
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.fire.spark.streaming.core.FireStreaming
import org.fire.spark.streaming.core.plugins.kafka.KafkaDirectSource

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

/**
 * Created by shaofengfeng on 2019/10/10.
 * 将业务表
 * homework_zhibo_laxincore_tblleads
 * leads_id,unique,例子先插入,再分配tmk,发生tmk调配时更新相应的tmk_uid
 * +----------------+----------------------+------+-----+---------+----------------+
 * | Field          | Type                 | Null | Key | Default | Extra          |
 * +----------------+----------------------+------+-----+---------+----------------+
 * | id             | int(10) unsigned     | NO   | PRI | NULL    | auto_increment |
 * | leads_id       | int(10) unsigned     | NO   | UNI | 0       |                |
 * | src_leads_id   | int(10) unsigned     | NO   |     | 0       |                |
 * | custom_uid     | bigint(20) unsigned  | NO   |     | 0       |                |
 * | source         | smallint(5) unsigned | NO   |     | 0       |                |
 * | sale_mode      | tinyint(1) unsigned  | NO   |     | 1       |                |
 * | origin         | varchar(20)          | NO   |     |         |                |
 * | batch_name     | varchar(100)         | NO   |     |         |                |
 * | grade          | smallint(5) unsigned | NO   |     | 0       |                |
 * | tmk_uid        | bigint(20) unsigned  | NO   |     | 0       |                |
 * | tmk_group_id   | int(10) unsigned     | NO   |     | 0       |                |
 * | tmk_alloc_mode | tinyint(3) unsigned  | NO   |     | 0       |                |
 * | tmk_alloc_time | int(10) unsigned     | NO   |     | 0       |                |
 * | sc_uid         | bigint(20) unsigned  | NO   |     | 0       |                |
 * | sc_group_id    | int(10) unsigned     | NO   |     | 0       |                |
 * | sc_alloc_mode  | tinyint(3) unsigned  | NO   |     | 0       |                |
 * | sc_alloc_time  | int(10) unsigned     | NO   | MUL | 0       |                |
 * | expire_time    | int(10) unsigned     | NO   | MUL | 0       |                |
 * | trans_time     | int(10) unsigned     | NO   |     | 0       |                |
 * | trans_fee      | int(10) unsigned     | NO   |     | 0       |                |
 * | status         | tinyint(3) unsigned  | NO   |     | 0       |                |
 * | create_time    | int(10) unsigned     | NO   |     | 0       |                |
 * | update_time    | int(10) unsigned     | NO   |     | 0       |                |
 * | ext_flag       | bigint(20) unsigned  | NO   |     | 0       |                |
 * | ext_data       | varchar(10000)       | NO   |     | []      |                |
 * +----------------+----------------------+------+-----+---------+----------------+
 * *
 * mysql> desc tblLeadsBatch;
 * batch_name unique
 * +-------------------+---------------------+------+-----+---------+----------------+
 * | Field             | Type                | Null | Key | Default | Extra          |
 * +-------------------+---------------------+------+-----+---------+----------------+
 * | id                | bigint(20) unsigned | NO   | PRI | NULL    | auto_increment |
 * | batch_name        | varchar(100)        | NO   | UNI |         |                |
 * | description       | varchar(100)        | NO   |     |         |                |
 * | source            | int(10) unsigned    | NO   |     | 0       |                |
 * | sale_mode         | tinyint(1) unsigned | NO   |     | 0       |                |
 * | grades            | varchar(255)        | NO   |     |         |                |
 * | filter_subjects   | varchar(255)        | NO   |     |         |                |
 * | filter_rules      | varchar(500)        | NO   |     |         |                |
 * | strategy_id       | int(10)             | NO   |     | 0       |                |
 * | type              | tinyint(3) unsigned | NO   |     | 0       |                |
 * | batch_quality     | tinyint(1) unsigned | NO   | MUL | 0       |                |
 * | system_id         | tinyint(3)          | NO   |     | 0       |                |
 * | second_channel_id | bigint(20) unsigned | NO   | MUL | 0       |                |
 * | status            | tinyint(3) unsigned | NO   |     | 0       |                |
 * | course_type       | tinyint(3) unsigned | NO   | MUL | 0       |                |
 * | creater_uid       | bigint(20) unsigned | NO   |     | 0       |                |
 * | create_time       | int(10) unsigned    | NO   |     | 0       |                |
 * | update_time       | int(10) unsigned    | NO   |     | 0       |                |
 * +-------------------+---------------------+------+-----+---------+----------------+
 * *
 * mysql> desc tblLeadsChannel;
 * 目前的逻辑中没有用到这个表,不同步数据
 * +-------------------+---------------------+------+-----+---------+----------------+
 * | Field             | Type                | Null | Key | Default | Extra          |
 * +-------------------+---------------------+------+-----+---------+----------------+
 * | id                | bigint(20) unsigned | NO   | PRI | NULL    | auto_increment |
 * | name              | varchar(100)        | NO   |     |         |                |
 * | parent_channel_id | bigint(20) unsigned | NO   | MUL | 0       |                |
 * | batch_num         | int(10) unsigned    | NO   |     | 0       |                |
 * | create_time       | int(10) unsigned    | NO   | MUL | 0       |                |
 * | update_time       | int(10) unsigned    | NO   |     | 0       |                |
 * | creater_uid       | bigint(20) unsigned | NO   |     | 0       |                |
 * +-------------------+---------------------+------+-----+---------+----------------+
 * *
 * mysql> desc tblTmkLeads0;
 * 这个tmk的例子表, 也是LEC核心表,
 * 存贮的分给tmk的所有的例子, 包括例子跟进的状态,约课情况,到课情况 等工作信息
 * 如果调配会出现一个leadsId 对应多个tmkUid\
 * 例子从A调配给B后
 * 原来的记录A会标记删除(status = 失效), 再插入一条新记录B
 * +------------------+---------------------+------+-----+------------+----------------+
 * | Field            | Type                | Null | Key | Default    | Extra          |
 * +------------------+---------------------+------+-----+------------+----------------+
 * | id               | int(10) unsigned    | NO   | PRI | NULL       | auto_increment |
 * | tmk_uid          | bigint(20) unsigned | NO   | MUL | 0          |                |
 * | leads_id         | int(10) unsigned    | NO   |     | 0          |                |
 * | intention        | tinyint(3) unsigned | NO   |     | 0          |                |
 * | call_cnt         | int(10) unsigned    | NO   |     | 0          |                |
 * | next_call_time   | int(10) unsigned    | NO   | MUL | 0          |                |
 * | expire_time      | int(10) unsigned    | NO   |     | 0          |                |
 * | yueke_time       | int(10) unsigned    | NO   | MUL | NULL       |                |
 * | weight           | float(10,8)         | NO   |     | 0.00000000 |                |
 * | status           | tinyint(3) unsigned | NO   |     | 0          |                |
 * | attend_time      | int(10) unsigned    | NO   | MUL | 0          |                |
 * | create_time      | int(10) unsigned    | NO   | MUL | 0          |                |
 * | weixin_time      | int(10) unsigned    | NO   |     | 0          |                |
 * | update_time      | int(10) unsigned    | NO   |     | 0          |                |
 * | trans_time       | int(10) unsigned    | NO   |     | 0          |                |
 * | subscribe_time   | int(10) unsigned    | NO   | MUL | 0          |                |
 * | ext_flag         | bigint(20) unsigned | NO   |     | 0          |                |
 * | ext_data         | varchar(10000)      | NO   |     | []         |                |
 * | batch_name       | varchar(100)        | NO   |     |            |                |
 * | color_mark       | varchar(100)        | NO   |     |            |                |
 * | last_call_time   | int(10) unsigned    | NO   |     | 0          |                |
 * | weixin_intention | tinyint(3) unsigned | NO   |     | 0          |                |
 * +------------------+---------------------+------+-----+------------+----------------+
 * *
 * mysql> desc tblLeadsLesson;
 * (tmk_uid,leads_id,course_id,lesson_id) unique , 一个例子有多个章节
 * +-----------------+----------------------+------+-----+---------+----------------+
 * | Field           | Type                 | Null | Key | Default | Extra          |
 * +-----------------+----------------------+------+-----+---------+----------------+
 * | id              | int(10) unsigned     | NO   | PRI | NULL    | auto_increment |
 * | tmk_uid         | bigint(20) unsigned  | NO   | MUL | 0       |                |
 * | leads_id        | int(10) unsigned     | NO   |     | 0       |                |
 * | course_id       | int(10) unsigned     | NO   | MUL | 0       |                |
 * | lesson_id       | int(10) unsigned     | NO   |     | 0       |                |
 * | start_time      | int(10) unsigned     | YES  | MUL | 0       |                |
 * | stop_time       | int(10) unsigned     | YES  |     | 0       |                |
 * | class_attend    | tinyint(3) unsigned  | NO   |     | 0       |                |
 * | stu_uid         | bigint(20) unsigned  | NO   | MUL | 0       |                |
 * | batch_name      | varchar(100)         | NO   |     |         |                |
 * | source          | smallint(5) unsigned | NO   |     | 0       |                |
 * | in_class        | tinyint(3)           | NO   |     | 0       |                |
 * | create_time     | int(10) unsigned     | YES  |     | 0       |                |
 * | attend_duration | int(10) unsigned     | NO   |     | 0       |                |
 * +-----------------+----------------------+------+-----+---------+----------------+
 * *
 * mysql> desc tblCallRecord0;
 * +-------------+---------------------+------+-----+----------+----------------+
 * | Field       | Type                | Null | Key | Default  | Extra          |
 * +-------------+---------------------+------+-----+----------+----------------+
 * | id          | bigint(20) unsigned | NO   | PRI | NULL     | auto_increment |
 * | product_id  | varchar(20)         | NO   |     | dianxiao |                |
 * | sale_uid    | bigint(20) unsigned | NO   |     | 0        |                |
 * | call_id     | bigint(20)          | NO   | UNI | 0        |                |
 * | custom_uid  | bigint(20) unsigned | NO   |     | 0        |                |
 * | phone       | varchar(20)         | NO   |     |          |                |
 * | create_time | int(10)             | NO   |     | 0        |                |
 * | update_time | int(10)             | NO   |     | 0        |                |
 * | start_time  | int(10) unsigned    | NO   | MUL | 0        |                |
 * | end_time    | int(10) unsigned    | NO   |     | 0        |                |
 * | duration    | int(10) unsigned    | NO   |     | 0        |                |
 * | record_file | varchar(200)        | NO   |     |          |                |
 * | ext_data    | varchar(10000)      | YES  |     | NULL     |                |
 * | call_type   | tinyint(3)          | NO   |     | 0        |                |
 * | role        | tinyint(3)          | NO   |     | 0        |                |
 * | leads_id    | bigint(20) unsigned | NO   |     | 0        |                |
 * | bit         | bigint(20)          | NO   |     | 0        |                |
 * +-------------+---------------------+------+-----+----------+----------------+
 * *
 * homework_zhibo_laxincore_tblleads
 * homework_zhibo_leads_tblleadsbatch
 * homework_zhibo_tmk_tblleadslesson
 * homework_zhibo_tmk_tbltmkleads
 * homework_zhibo_laxincall_tblcallrecord
 * *
 * homework_zhibo_laxincore_tblleads:leads_id主键
 * homework_zhibo_leads_tblleadsbatch:batch_name主键
 * homework_zhibo_tmk_tbltmkleads:tmk_uid+leads_id主键
 * homework_zhibo_tmk_tblleadslesson:tmk_uid+leads_id+course_id+lesson_id主键
 * homework_zhibo_laxincall_tblcallrecord:call_id为主键,tmk_uid+leads_id不唯一
 * *
 *
 * tbleads,tbltmkleads,tblleadslesson,tblcallrecord 依赖tblleadsbatch,tblleadsbatch数据先于它们
 * tblleadslesson 依赖tbltmkleads, tbltmkleads先于tblleadslesson
 * tblcallrecord 依赖tblleads
 */
object Assemble2ES extends FireStreaming with Logging {

  val tblLeadsReg = "tblLeads\\d*" r
  val tblLeadsLessonReg = "tblLeadsLesson\\d*" r
  val tblTmkLeadsReg = "tblTmkLeads\\d*" r
  val tblLeadsBatchReg = "tblLeadsBatch" r
  val tblLeadsChannelReg = "tblLeadsChannel" r
  val tblCallRecordReg = "tblCallRecord\\d*" r

  val tblReg2db = Map(
    tblLeadsReg -> "homework_zhibo_laxincore",
    tblLeadsLessonReg -> "homework_zhibo_tmk",
    tblTmkLeadsReg -> "homework_zhibo_tmk",
    tblLeadsBatchReg -> "homework_zhibo_leads",
    tblLeadsChannelReg -> "homework_zhibo_leads",
    tblCallRecordReg -> "homework_zhibo_laxincall"
  )

  val leadsIndex = "tmk_leads"
  val leadsLessonIndex = "tmk_leads_lesson"
  val batchIndex = "tmk_batch"
  val leadsTmkIndex = "tmk_leads_tmk"
  val callRecordIndex = "tmk_call"

  private val _formatter = DateTimeFormatter.ofPattern("yyyyMMdd")

  private val bulkSize = 1000
  /*override def main(args: Array[String]): Unit = {
    println("...")
    implicit  val es=ElasticClient(ElasticProperties("http://192.168.145.132:10200"))
    updateTmkLesson(Map("min_expire_time"->"1573401600","batch_name"->"app_ykzc_1","second_channel_id"->10012), "43556229","2375775612")
  }*/

  override def handle(ssc: StreamingContext): Unit = {
    /*两张es宽表对应的index*/
    val conf = ssc.sparkContext.getConf
    implicit val esProps: ElasticProperties = parseClientURI(conf)
    val source = new KafkaDirectSource[String, String](ssc)
    val lines = source.getDStream(_.value()).map(JsonUtil.toDataTable)
      .filter(dt => dt.TYPE == "I" || dt.TYPE == "U")

    lines.foreachRDD((rdd, time) => {
      val filteredRdd = rdd.filter(dt => matchTblDB(dt, tblLeadsReg, tblReg2db) ||
        matchTblDB(dt, tblLeadsBatchReg, tblReg2db) ||
        matchTblDB(dt, tblTmkLeadsReg, tblReg2db) ||
        matchTblDB(dt, tblLeadsLessonReg, tblReg2db) ||
        matchTblDB(dt, tblCallRecordReg, tblReg2db)
      )
      val preparedRdd = filteredRdd.map(dt => (s"${dt.DATABASE}_${dt.TABLE}_${dt.getNewValues()("id")}", dt)).reduceByKey((dt1, dt2) => {
        val v1 = dt1.TIME + "_" + dt1.BINLOG_POS
        val v2 = dt2.TIME + "_" + dt2.BINLOG_POS
        if (v1 < v2) dt2 else dt1
      }).map(_._2)
      preparedRdd.foreachPartition(iter => compute(iter)(connect))
      source.updateOffsets(time.milliseconds)
    })
  }

  def compute(iter: Iterator[DataTable])(implicit esClient: ElasticClient): Unit = {
    //esClient后续可以封装为高层client(es/redis)
    val bulkArr = ArrayBuffer[BulkCompatibleRequest]()
    iter.foreach {
      case dt if matchTblDB(dt, tblLeadsReg, tblReg2db) => dealLeads(dt, bulkArr)
      case dt if matchTblDB(dt, tblLeadsBatchReg, tblReg2db) => dealLeadsBatch(dt, bulkArr)
      case dt if matchTblDB(dt, tblTmkLeadsReg, tblReg2db) => dealTmkLeads(dt, bulkArr)
      case dt if matchTblDB(dt, tblLeadsLessonReg, tblReg2db) => dealLeadsLesson(dt, bulkArr)
      case dt if matchTblDB(dt, tblCallRecordReg, tblReg2db) => dealCallRecord(dt, bulkArr)
      case tbl =>
        //do nothing
        log.debug("no case match for {}", tbl)
    }
    executeBulkSync(bulkArr)
  }

  def matchTblDB(dt: DataTable, tblReg: Regex, tblReg2db: Map[Regex, String]): Boolean = {
    tblReg.pattern.matcher(dt.TABLE).matches() && dt.DATABASE.equals(tblReg2db(tblReg))
  }

  /*插入leads或是为leads分配,调配tmk*/
  /*插入时拉取对应的batch信息,batch缓存更新时进行回填*/
  def dealLeads(dt: DataTable, bulkArr: ArrayBuffer[BulkCompatibleRequest])(implicit esClient: ElasticClient): Unit = {
    if (bulkArr.size > bulkSize) {
      executeBulkSync(bulkArr)
      bulkArr.clear()
    }
    val nv = dt.getNewValues()
    val Array(leadsId, batchName, allocTime) = parseFields(dt, Seq("leads_id", "batch_name", "tmk_alloc_time"))
    //业务方某些batchName会有空格
    val batchName1 = batchName.trim
    if (!batchName1.isEmpty) {
      val allocDate = parseDate(allocTime)
      val allocHour = parseHour(allocTime)
      //    无论是I/U都需拉去码表，因为U不代表在es中不存在
      val scid = getFieldsById(Seq("second_channel_id"), true, batchName1, batchIndex, true)
        .getOrElse(Map.empty[String, Any]).getOrElse("second_channel_id", "none")

      val nv1 = nv ++ Map("alloc_date" -> allocDate, "alloc_hour" -> allocHour, "second_channel_id" -> scid)
      bulkArr.append(insertDocWithIdReq(leadsId, nv1, leadsIndex))
      /*更新依赖此记录的tmk_call索引*/
      updateCallRecord(Map("batch_name" -> batchName1, "second_channel_id" -> scid, "tmk_alloc_time" -> allocTime), leadsId)
    }
  }

  /*仅更新alloc_time*/
  def updateCallRecord(fields: Map[String, Any], leadsId: String)(implicit esClient: ElasticClient): Unit = {
    updateFieldsByScript("update_tmk_call", fields, Map("leads_id" -> leadsId), callRecordIndex)
  }

  def dealTmkLeads(dt: DataTable, bulkArr: ArrayBuffer[BulkCompatibleRequest])(implicit esClient: ElasticClient): Unit = {
    if (bulkArr.size > bulkSize) {
      executeBulkSync(bulkArr)
      bulkArr.clear()
    }
    val nv = dt.getNewValues()
    val Array(leadsId, tmkUid, batchName, yuekeTime, subscribeTime, attendTime, transTime, weixinTime) = parseFields(dt, Seq("leads_id", "tmk_uid", "batch_name",
      "yueke_time", "subscribe_time", "attend_time", "trans_time", "weixin_time"))
    val batchName1 = batchName.trim
    if (!batchName1.isEmpty) {
      val docId = leadsId + ":" + tmkUid
      val scid = getFieldsById(Seq("second_channel_id"), true, batchName1, batchIndex, true)
        .getOrElse(Map.empty[String, Any]).getOrElse("second_channel_id", "none")

      val yuekeDate = parseDate(yuekeTime)
      val subscribeDate = parseDate(subscribeTime)
      val attendDate = parseDate(attendTime)
      val transDate = parseDate(transTime)
      val weixinDate = parseDate(weixinTime)

      val nv1 = nv ++ Map("second_channel_id" -> scid,
        "yueke_date" -> yuekeDate, "yueke_hour" -> parseHour(yuekeTime),
        "subscribe_date" -> subscribeDate, "subscribe_hour" -> parseHour(subscribeTime),
        "attend_date" -> attendDate, "attend_hour" -> parseHour(attendTime),
        "trans_date" -> transDate, "trans_hour" -> parseHour(transTime),
        "weixin_date" -> weixinDate, "weixin_hour" -> parseHour(weixinTime)
      ) ++ Map("min_expire_time" -> calTmkMinExpireTime(dt, true))

      val minExpireTime = calTmkMinExpireTime(dt, true)
      bulkArr.append(insertDocWithIdReq(docId, nv1, leadsTmkIndex))
      /*更新依赖此记录的tmk*/
      updateTmkLesson(Map("min_expire_time" -> minExpireTime, "batch_name" -> batchName1, "second_channel_id" -> scid), leadsId, tmkUid)
    }
  }

  def parseDate(unixtime: String): String = {
    val ld = LocalDateTime.ofEpochSecond(unixtime.toLong, 0, ZoneOffset.ofHours(8))
    _formatter.format(ld)
  }

  def parseHour(unixtime: String): String = {
    val ld = LocalDateTime.ofEpochSecond(unixtime.toLong, 0, ZoneOffset.ofHours(8))
    ld.getHour.toString
  }

  /*仅更新min_expire_time,batch_name,second_channel_id*/
  def updateTmkLesson(fields: Map[String, Any], leadsId: String, tmkUid: String)(implicit esClient: ElasticClient): Unit = {
    updateFieldsByScript("update_tmk_leads_lesson", fields, Map("tmk_uid" -> tmkUid, "leads_id" -> leadsId), leadsLessonIndex)
  }

  def calTmkMinExpireTime(dt: DataTable, history: Boolean = false): String = {
    val Array(attendTime, transTime, expireTime) = parseFields(dt, Seq("attend_time", "trans_time", "expire_time"))
    val a = if (attendTime.toInt == 0) Integer.MAX_VALUE else attendTime.toInt
    val b = if (transTime.toInt == 0) Integer.MAX_VALUE else transTime.toInt
    val tmp = Math.min(a, Math.min(b, expireTime.toInt)).toString

    if (history) {
      val ts = DateTimeUtil.yyyyMMddHHmmss2millis("20190830000000").get / 1000
      if (a < ts || b < ts) Math.min(a, b).toString else tmp
    } else tmp
  }

  /*如果业务表结构发生更改,该函数可以在抛异常时将数据也打出来*/
  def parseFields(dt: DataTable, keys: Seq[String]): Array[String] = {
    val nv = dt.getNewValues()
    Try {
      for (key <- keys) yield nv(key).toString
    } match {
      case Success(values) =>
        values.toArray
      case Failure(exception) =>
        throw new RuntimeException(exception.getMessage + ":" + dt.toString)
    }
  }

  /*插入时拉取缓存的leads_tmk信息,缓存更新时会回填该表*/
  def dealLeadsLesson(dt: DataTable, bulkArr: ArrayBuffer[BulkCompatibleRequest])(implicit esClient: ElasticClient): Unit = {
    if (bulkArr.size > bulkSize) {
      executeBulkSync(bulkArr)
      bulkArr.clear()
    }
    val nv = dt.getNewValues()
    val Array(leadsId, tmkUid, courseId, lessonId, startTime, batchName) = parseFields(dt, Seq("leads_id", "tmk_uid", "course_id", "lesson_id", "start_time", "batch_name"))
    val batchName1 = batchName.trim
    if (!batchName1.isEmpty) {
      val tmkDocId = s"$leadsId:$tmkUid"
      val ex = getFieldsById(Seq("batch_name", "second_channel_id", "min_expire_time"), include = true, tmkDocId, leadsTmkIndex, true).getOrElse(Map.empty[String, Any])
      val docId = s"$leadsId:$tmkUid:$courseId:$lessonId"
      val nv1 = nv ++ Map("start_date" -> parseDate(startTime), "start_hour" -> parseHour(startTime)) ++ ex
      bulkArr.append(insertDocWithIdReq(docId, nv1, leadsLessonIndex))
    }
  }

  /*数据先于其他表,需要缓存*/
  /*缓存更新时回填依赖该缓存的表,leadsIndex和leadsTmkIndex*/
  def dealLeadsBatch(dt: DataTable, bulkArr: ArrayBuffer[BulkCompatibleRequest])(implicit esClient: ElasticClient): Unit = {
    val nv = dt.getNewValues()
    val Array(batchName, scid) = parseFields(dt, Seq("batch_name", "second_channel_id"))
    bulkArr.append(upsertFieldsByIdReq(nv, batchName.trim, batchIndex))

    /*更新时回填*/
    if (dt.TYPE == "U") {
      updateSCID(dt)
    }
  }

  /*需要更新tmk_leads,tmk_leads_tmk,tmk_leads_lesson,tmk_call*/
  def updateSCID(dt: DataTable)(implicit c: ElasticClient) = {
    val ov = dt.getOldValues()
    val nv = dt.getNewValues()
    if (!nv.get("second_channel_id").equals(ov.get("second_channel_id"))) {
      val scid = nv("second_channel_id")
      val batchName = nv("batch_name").toString.trim

      updateFieldsByScript("update_scid", Map("value" -> scid), Map("batch_name" -> batchName), leadsIndex)
      updateFieldsByScript("update_scid", Map("value" -> scid), Map("batch_name" -> batchName), leadsTmkIndex)
      updateFieldsByScript("update_scid", Map("value" -> scid), Map("batch_name" -> batchName), leadsLessonIndex)
      updateFieldsByScript("update_scid", Map("value" -> scid), Map("batch_name" -> batchName), callRecordIndex)
    }
  }

  /*依赖tmk_leads的"batch_name", "second_channel_id", "tmk_alloc_time"*/
  def dealCallRecord(dt: DataTable, bulkArr: ArrayBuffer[BulkCompatibleRequest])(implicit client: ElasticClient): Unit = {
    if (bulkArr.size > bulkSize) {
      executeBulkSync(bulkArr)
      bulkArr.clear()
    }
    val nv = dt.getNewValues()
    val Array(start_time, create_time, role, tmk_uid, call_type, leads_id, call_id) = parseFields(dt, Seq("start_time", "create_time", "role", "sale_uid", "call_type", "leads_id", "call_id"))
    if (role == "1") {
      val callStartTime = if (call_type == "2") create_time else start_time
      val callStartDate = parseDate(callStartTime)
      val callStartHour = parseHour(callStartTime)

      val extraFields = getFieldsById(Seq("batch_name", "second_channel_id", "tmk_alloc_time"), true, leads_id, leadsIndex, true)
      /*由于是业务方是inner join,如果拉取不到就舍弃*/
      if (extraFields.isDefined && extraFields.get.nonEmpty) {
        val nv1 = Map("start_date" -> callStartDate, "start_hour" -> callStartHour, "tmk_uid" -> tmk_uid) ++ nv - "sale_uid" ++ extraFields.get
        val docId = call_id
        bulkArr.append(insertDocWithIdReq(docId, nv1, callRecordIndex))
      }
    }
  }
}
