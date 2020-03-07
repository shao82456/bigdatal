//package sakura.test
//
//import com.homework.da.util.{CodisEndPoint, CodisUtil, JsonUtil}
////import kafka.utils.Logging
//import org.apache.spark.streaming.dstream.DStream
//import org.apache.spark.streaming.{CongestionMonitorListener, StreamingContext}
//import org.fire.spark.streaming.core.{FireStreaming, Logging}
//import org.fire.spark.streaming.core.plugins.kafka.KafkaDirectSource
//import redis.clients.jedis.Jedis
//
//import scala.util.Try
//
///**
//  * Created by zhouyisong on 2019/11/27
//  * Describe:
//  */
//object LpcComputeNewStreaming extends FireStreaming with Logging {
//  override def handle(ssc: StreamingContext): Unit = {
//    val sparkConf = ssc.sparkContext.getConf
//    //source源和sink源
//    val source = new KafkaDirectSource[String, String](ssc)
//    val sink = new EsSink[(String, List[Any])](ssc.sparkContext)
//    val codisEndPoint = new CodisEndPoint(sparkConf)
//    val partitionNum = ssc.sparkContext.getConf.getInt("spark.compute.partition.num", 30)
//    //批次监控
//    ssc.addStreamingListener(new CongestionMonitorListener(ssc))
//    val lines: DStream[(String, Map[String, Any])] = source.getDStream(_.value()).flatMap(x => Try {
//      if (x.startsWith("{")) {
//        val kafka_tuple = JsonUtil.toAny[(String, Map[String, Any])](x)
//        if (KAFKAKEY_LAST.contains(kafka_tuple._1.split("_").last)) {
//          Some(kafka_tuple)
//        } else None
//      } else if (x.startsWith("[")) {
//        val l = JsonUtil.toAny[List[Any]](x)
//        Some((l.head.asInstanceOf[String], l.last.asInstanceOf[Map[String, Any]]))
//      } else None
//    }.getOrElse(None)).flatMap(revertFunc(_))
//    lines.foreachRDD((rdd, time) => {
//      val resRDD = rdd.mapPartitions(exec(_, codisEndPoint))
//      val a=resRDD.repartition(partitionNum)
//      sink.output(a, time)
//      source.updateOffsets(time.milliseconds)
//    })
//  }
//
//  //###########################转换function######################################################
//  def revertFunc(value: (String, Map[String, Any])): Option[(String, Map[String, Any])] = {
//    val key = value._1
//    val map = value._2
//    val key_array = key.split("_")
//    //Array("activeAndRightcount","PreviewInfo","tcount","answerinfo")
//    key_array.last match {
//      case "activeAndRightcount" => activeAndRightcount(key_array, map)
//      case "PreviewInfo" => previewInfo(key_array, map)
//      case "tcount" => tcount(key_array, map)
//      case "answerinfo" => answerinfo(key_array, map)
//      case "HuDongactiveAndRightcount" => huDongactiveAndRightcount(key_array, map)
//      case _ =>
//        key match {
//          case KAFKA_STUDENT_ATTENDANCE | KAFKA_STUDENT_PLAYBACK => attendFun(key, map)
//          case _ => None
//        }
//    }
//  }
//
//  //###################################执行function#######################################################
//  def exec(iter: Iterator[(String, Map[String, Any])], codisEndPoint: CodisEndPoint): Iterator[(String, List[Any])]
//  = {
//    CodisUtil.safeClose { redis =>
//      iter.flatMap(compute(_, redis))
//    }(CodisUtil.connect(codisEndPoint))
//  }
//
//  //######################################计算function###############################################
//  def compute(tuple: (String, Map[String, Any]), redis: Jedis): List[(String, List[Any])] = {
//    val kafkakey = tuple._1
//    val map = tuple._2
//    kafkakey match {
//      case "activeAndRightcount" => activeAndRightcountCompute(redis, map)
//      case "HuDongactiveAndRightcount" => huDongactiveAndRightcountCompute(redis, map)
//      case "PreviewInfo" => previewInfoCompute(redis, map)
//      case "tcount" => tcountCompute(redis, map)
//      case "answerinfo" => answerinfoCompute(redis, map)
//      case KAFKA_STUDENT_ATTENDANCE | KAFKA_STUDENT_PLAYBACK => studentLesson(tuple, redis)
//      case _ => List.empty
//    }
//  }
//
//  //##################################转换分function###############################
//
//  /**
//    * LU维度到课，完课转换方法
//    *
//    * @param key 上游kafka List的第一个元素表示到课或者回放标志信息
//    * @param map 上游kakfa map 学生到课完课，章节，课程相关信息
//    *            上游数据结构：
//    *            回放：
//    *            PlaybackData(uid: String,
//    *            lessonId: String,
//    *            courseId: String,
//    *            startTime: Long,
//    *            stopTime: Long,
//    *            weekPlaybackTime: Long,
//    *            playbackTotalTime: Long,
//    *            isPlaybackLong:Boolean,
//    *            isPlaybackFinish:Boolean
//    *            )
//    *            到课：
//    *            SendData(uid: String,
//    *            lessonId: String,
//    *            courseId: String,
//    *            lessonName: String,
//    *            startTime: Long,
//    *            stopTime: Long,
//    *            teacherId: String,
//    *            attendLong: Boolean,
//    *            attend: Boolean,
//    *            attendLen: Long,
//    *            isFinish:Boolean,
//    *            process: String)
//    * @return
//    */
//  def attendFun(key: String, map: Map[String, Any]): Option[(String, Map[String, Any])] = key match {
//    case KAFKA_STUDENT_ATTENDANCE =>
//      val uid = map("uid").toString
//      val lessonId = map("lessonId").toString
//      val courseId = map("courseId").toString
//      //val startTime = map("startTime").toString.toLong//章节预计开始时间
//      //val attend = map("attend").asInstanceOf[Boolean]//是否到课5分钟
//      val attendLen = map("attendLen").toString.toLong
//      //是否完课
//      val resMap = Map(
//        "student_uid" -> uid.toLong,
//        "lesson_id" -> lessonId.toInt,
//        "course_id" -> courseId.toInt,
//        "attend_duration" -> attendLen,
//        "block_finish_attend_update_time" -> System.currentTimeMillis() / 1000
//      )
//      Some((key, resMap))
//    case KAFKA_STUDENT_PLAYBACK =>
//      val uid = map("uid").toString
//      val lessonId = (if (map("lessonId") != null) map("lessonId") else "0").toString
//      val courseId = (if (map("courseId") != null) map("courseId") else "0").toString
//      val playbackTotalTime = map("playbackTotalTime").toString.toLong
//      val resMap = Map(
//        "student_uid" -> uid.toLong,
//        "lesson_id" -> lessonId.toInt,
//        "course_id" -> courseId.toInt,
//        "play_duration" -> playbackTotalTime,
//        "block_finish_attend_update_time" -> System.currentTimeMillis() / 1000
//      )
//      Some((key, resMap))
//
//
//    case _ => None
//
//  }
//
//  /**
//    * 小初预习参与数正确数转换方法
//    *
//    * @param key_array kakfakey array 包括学生，章节信息
//    * @param map       kakfamap 包括LU维度的学生参与数，正确数信息
//    * @return
//    * kafka数据结构:
//    * studentUID_bindType_bindID_examType_relationType_examid_templateid_activeAndRightcount->json
//    * 例子:
//    * {"2381872915_0_252096_7_7_1250128_39955_activeAndRightcount":{"participate_num":1,"right_num":1,"relation_type":"7"}}
//    */
//  def activeAndRightcount(key_array: Array[String], map: Map[String, Any]): Option[(String, Map[String, Any])] = {
//    val examtype = key_array(3)
//    val bindtype = key_array(1)
//    if (bindtype == "0") examtype match {
//      case "5" | "13" =>
//        val student_uid = key_array(0)
//        val lesson_id = key_array(2)
//        val resObjMap = Map(
//          "student_uid" -> student_uid,
//          "lesson_id" -> lesson_id,
//          "participate_num" -> map("participate_num").toString.toInt,
//          "right_num" -> map("right_num").toString.toInt,
//          "examType" -> examtype,
//          (if (examtype == "5") "block_exam5_update_time" else "block_exam13_update_time") -> System.currentTimeMillis() / 1000
//        )
//        Some(("activeAndRightcount", resObjMap))
//      case _ => None
//    }
//    else None
//  }
//
//  /**
//    * 初高预习预习信息转换方法
//    *
//    * @param key_array kakfakey array 包括学生，章节信息
//    * @param map       kakfamap 包括LU维度的学生预习状态，渠道信息
//    * @return
//    * kafka数据结构:
//    * studentUID_lessonId_PreviewInfo->jsonString
//    * 例子：
//    * {"2112898912_186196_PreviewInfo":{"status":"1","source":"0","create_time":"1571125325"}}
//    * source:预习渠道，status:预习状态，create_time:预习时间
//    */
//  def previewInfo(key_array: Array[String], map: Map[String, Any]): Option[(String, Map[String, Any])] = {
//    val student_uid = key_array(0)
//    val lesson_id = key_array(1)
//    val resObjMap = Map(
//      "student_uid" -> student_uid,
//      "lesson_id" -> lesson_id,
//      "source" -> map("source").toString.toInt,
//      "status" -> map("status").toString.toInt,
//      "block_exam13_update_time" -> System.currentTimeMillis() / 1000
//    )
//    Some(("PreviewInfo", resObjMap))
//  }
//
//  /**
//    * 章节id对应的题目的数量(互动题总题数也包括在里面)
//    *
//    * @param key_array kakfakey array 包括章节信息,章节类型信息
//    * @param map       kakfamap 包括L维度的总题数信息
//    * @return
//    * kakfa结构：
//    * kafkakey:bindType_bindID_examType_examID_tcount
//    * kafkamap:{"tcount":"12","keyInfoischange":0,"isHasRelation":1,"relation_type":3}
//    * 例子：
//    * {"0_186458_1_0_tcount":{"tcount":7,"keyInfoischange":1,"isHasRelation":1,"relation_type":1}}
//    * {"0_186418_10_70999_tcount":{"tcount":2,"keyInfoischange":0,"isHasRelation":1,"relation_type":"10"}}
//    */
//  def tcount(key_array: Array[String], map: Map[String, Any]): Option[(String, Map[String, Any])] = {
//    val examType = key_array(2)
//    val bindType = key_array(0)
//    if (bindType == "0") examType match {
//      case "5" | "13" | "1" =>
//        val resObjMap = Map(
//          "examType" -> examType,
//          "lesson_id" -> key_array(1),
//          "total_num" -> map("tcount").toString.toInt
//        ) ++ (if (examType == "1") Map("block_exam1_update_time" -> System.currentTimeMillis() / 1000) else Map.empty)
//        Some(("tcount", resObjMap))
//      case _ => None
//    }
//    else None
//  }
//
//  /**
//    * 答案的详情信息转换方法
//    *
//    * @param key_array kakfakey array 包括学生，章节信息
//    * @param map       kakfamap 包括LU维度提交时间以及批改信息
//    * @return
//    * kakfa结构：
//    * studentUID_bindType_bindID_examType_relationType_answerinfo→Json
//    * 用户ID_绑定类型_绑定ID_测试类型&answerinfo
//    * 例子：
//    * {"2365959252_0_223133_10_10_answerinfo":{"hw_is_jungle":1,"createtime":"1569915545","score":"500","hw_jungle_time":"1569915545","relation_type":"10","hw_jungle_level":0}}
//    */
//  def answerinfo(key_array: Array[String], map: Map[String, Any]): Option[(String, Map[String, Any])] = {
//    val examType = key_array(3)
//    val bindType = key_array(1)
//    if (bindType == "0") examType match {
//      case "5" | "13" =>
//        val resObjMap = Map(
//          "student_uid" -> key_array(0),
//          "lesson_id" -> key_array(2),
//          "submit_time" -> map("createtime").toString.toLong,
//          "examType" -> examType,
//          (if (examType == "5") "block_exam5_update_time" else "block_exam13_update_time") -> System.currentTimeMillis() / 1000
//        )
//        Some(("answerinfo", resObjMap))
//      case _ => None
//    }
//    else None
//  }
//
//  /**
//    * 互动题参与数正确数转换方法
//    *
//    * @param key_array kakfakey array 包括学生，章节信息
//    * @param map       kakfamap 包括LU维度提交时间以及批改信息
//    * @return
//    * kakfa 结构：
//    * 用户ID_绑定类型_绑定ID_测试类型_参与&activecount->:json
//    * 例子：
//    * {"2224984294_0_216202_1_HuDongactiveAndRightcount":{"participate_num":3,"right_num":3}}
//    */
//  def huDongactiveAndRightcount(key_array: Array[String], map: Map[String, Any]): Option[(String, Map[String, Any])] = {
//    val bindType = key_array(1)//
//    if (bindType == "0") Some(("HuDongactiveAndRightcount", Map(
//      "student_uid" -> key_array(0),
//      "lesson_id" -> key_array(2),
//      "participate_num" -> map("participate_num").toString.toInt,
//      "right_num" -> map("right_num").toString.toInt,
//      "block_exam1_update_time" -> System.currentTimeMillis() / 1000
//    ))) else None
//  }
//
//  //####################################计算分function######################################################
//
//  /**
//    * 到课回放推到ES和kakfa的数据处理
//    *
//    * @param value
//    * @param redis
//    * @return
//    * 发到kafka数据结构：                                                     回放：["LU_PLAYBACK", {
//    * 到课：["lu_attend", {                                                        "student_uid": "2385011744",
//    * "student_uid": "2385011744",                                                 "lesson_id": "245217",
//    * "lesson_id": "245217",                                                       "course_id": "260381",
//    * "course_id": "260381",                                                       "play_duration": 1569897900,
//    * "attend_duration": 1569897900                                               "_finish_attend_update_time": 1567353426
//    * "block_finish_attend_update_time": 1567353426                              }]
//    * }]
//    */
//  def studentLesson(value: (String, Map[String, Any]), redis: Jedis): List[(String, List[Any])] = {
//    val map = value._2
//    val key = value._1
//    key match {
//      case KAFKA_STUDENT_ATTENDANCE =>
//        val course_id = map("course_id").toString
//        val student_uid = map("student_uid").toString
//        val lesson_id = map("lesson_id").toString
//        val course_info = if (redis.hexists(TBL_COURSE_INFO, course_id)) redis.hget(TBL_COURSE_INFO, course_id) else ""
//        val course_map = Try(JsonUtil.toAny[Map[String, Any]](course_info)).getOrElse(Map.empty)
//        val courseType = course_map.get("courseType").getOrElse("-1").toString
//        val shortTraining = course_map.get("shortTraining").getOrElse("2").toString
//        val temMap = Map("course_type" -> courseType.toInt, "is_short_train" -> shortTraining.toInt)
//        val LuKey = s"${lesson_id}_${student_uid}"
//        List((LuKey, List(ATTEND_KAFKA_KEY, map - ("course_id") ++ temMap)))
//      case KAFKA_STUDENT_PLAYBACK =>
//        val course_id = map("course_id").toString
//        val student_uid = map("student_uid").toString
//        val lesson_id = map("lesson_id").toString
//        val course_info = if (redis.hexists(TBL_COURSE_INFO, course_id)) redis.hget(TBL_COURSE_INFO, course_id) else ""
//        val course_map = Try(JsonUtil.toAny[Map[String, Any]](course_info)).getOrElse(Map.empty)
//        val courseType = course_map.get("courseType").getOrElse("-1").toString
//        val shortTraining = course_map.get("shortTraining").getOrElse("2").toString
//        val temMap = Map("course_type" -> courseType.toInt, "is_short_train" -> shortTraining.toInt)
//        val LuKey = s"${lesson_id}_${student_uid}"
//        List((LuKey, List(PLAYBACK_KAFKA_KEY, map - ("course_id") ++ temMap)))
//      case _ => List.empty
//    }
//  }
//
//  /**
//    * 小学初高预习参与数正确数总题数推到下游ES以及kafka数据结构
//    *
//    * @param redis
//    * @param map
//    * @return
//    * kafka数据结构：
//    * 初高：["lu_exam13_activeAndRightTcount", {
//    * "student_uid": "2385011744",
//    * "lesson_id": "245217",
//    * "exam13":{
//    * "participate_num": 5,
//    * "right_num":4,
//    * "total_num":8,
//    * "block_exam13_update_time": 1567353426
//    * }
//    * }]
//    * 小学的kafka数据结构exam13换成exam5
//    */
//  def activeAndRightcountCompute(redis: Jedis, map: Map[String, Any]): List[(String, List[Any])] = {
//    val student_uid = map("student_uid").toString
//    val lesson_id = map("lesson_id").toString
//    val examType = map("examType").toString
//    val course_id = if (redis.exists(s"${LC}_${lesson_id}")) redis.get(s"${LC}_${lesson_id}").split("_")(0) else "0"
//    val course_info = if (redis.hexists(TBL_COURSE_INFO, course_id)) redis.hget(TBL_COURSE_INFO, course_id) else ""
//    val course_map = Try(JsonUtil.toAny[Map[String, Any]](course_info)).getOrElse(Map.empty)
//    val courseType = course_map.get("courseType").getOrElse("-1").toString
//    val shortTraining = course_map.get("shortTraining").getOrElse("2").toString
//    val Lukey = s"${lesson_id}_${student_uid}"
//    if (redis.exists(s"${LESSON_TCOUNT}_${lesson_id}_${examType}")) {
//      val total_num = redis.get(s"${LESSON_TCOUNT}_${lesson_id}_${examType}").toInt
//      val resObjMap = map - ("student_uid", "lesson_id", "examType") ++ Map("total_num" -> total_num)
//      val resMap = Map(
//        "student_uid" -> student_uid.toLong,
//        "lesson_id" -> lesson_id.toInt,
//        "course_type" -> courseType.toInt,
//        "is_short_train" -> shortTraining.toInt,
//        (if (examType == "5") "exam5" else "exam13") -> resObjMap
//      )
//      List((Lukey, List((if (examType == "5") EXAM5_ACTIVEANDRIGHTCOUNT else EXAM13_ACTIVEANDRIGHTCOUNT), resMap)))
//    } else {
//      //表示没有关联到总题数仍然将数据发往Es
//      val resObjMap = map - ("student_uid", "lesson_id", "examType")
//      val resMap = Map(
//        "student_uid" -> student_uid.toLong,
//        "lesson_id" -> lesson_id.toInt,
//        "course_type" -> courseType.toInt,
//        "is_short_train" -> shortTraining.toInt,
//        (if (examType == "5") "exam5" else "exam13") -> resObjMap
//      )
//      List((Lukey, List((if (examType == "5") EXAM5_ACTIVEANDRIGHTCOUNT else EXAM13_ACTIVEANDRIGHTCOUNT), resMap)))
//    }
//  }
//
//  /**
//    * 初高预习推向下游ES以及kafka的数据处理
//    * 数据结构：
//    * ["lu_exam13_status_source", {
//    * "student_uid": "2385011744",
//    * "lesson_id": "245217",
//    * "exam13":{
//    * "source": 1,
//    * "status":1,
//    * "block_exam13_update_time": 1567353426
//    * }
//    * }]
//    *
//    * @param redis
//    * @param map
//    * @return
//    */
//  def previewInfoCompute(redis: Jedis, map: Map[String, Any]): List[(String, List[Any])] = {
//    val student_uid = map("student_uid").toString
//    val lesson_id = map("lesson_id").toString
//    val Lukey = s"${lesson_id}_${student_uid}"
//    val course_id = if (redis.exists(s"${LC}_${lesson_id}")) redis.get(s"${LC}_${lesson_id}").split("_")(0) else "0"
//    val course_info = if (redis.hexists(TBL_COURSE_INFO, course_id)) redis.hget(TBL_COURSE_INFO, course_id) else ""
//    val course_map = Try(JsonUtil.toAny[Map[String, Any]](course_info)).getOrElse(Map.empty)
//    val courseType = course_map.get("courseType").getOrElse("-1").toString
//    val shortTraining = course_map.get("shortTraining").getOrElse("2").toString
//    val resObjMap = map - ("student_uid", "lesson_id")
//    val resMap = Map(
//      "student_uid" -> student_uid.toLong,
//      "lesson_id" -> lesson_id.toInt,
//      "course_type" -> courseType.toInt,
//      "is_short_train" -> shortTraining.toInt,
//      "exam13" -> resObjMap)
//    List((Lukey, List(EXAM13_STATUS_SOURSE, resMap)))
//  }
//
//  /**
//    * 小初高预习总题数存入redis码表，以及互动题推向下游ES和kafka的数据处理
//    * 下游数据结构：
//    * ["l_exam1_tcount",{
//    * "lesson_id":"245217",
//    * "exam1":{
//    * "total_num":4,
//    * "block_exam1_update_time":1567353426
//    * }
//    * }]
//    *
//    * @param redis
//    * @param map
//    * @return
//    */
//  def tcountCompute(redis: Jedis, map: Map[String, Any]): List[(String, List[Any])] = {
//    val lesson_id = map("lesson_id").toString
//    val total_num = map("total_num").toString
//    val examType = map("examType").toString
//    val course_id = if (redis.exists(s"${LC}_${lesson_id}")) redis.get(s"${LC}_${lesson_id}").split("_")(0) else "0"
//    val course_info = if (redis.hexists(TBL_COURSE_INFO, course_id)) redis.hget(TBL_COURSE_INFO, course_id) else ""
//    val course_map = Try(JsonUtil.toAny[Map[String, Any]](course_info)).getOrElse(Map.empty)
//    val courseType = course_map.get("courseType").getOrElse("-1").toString
//    val shortTraining = course_map.get("shortTraining").getOrElse("2").toString
//    redis.set(s"${LESSON_TCOUNT}_${lesson_id}_${examType}", total_num)
//    examType match {
//      case "1" =>
//        redis.hgetAll(s"${HUDONG_REDIS_PRE}_${lesson_id}").entrySet.toArray.map(x => {
//          val student_partic_correct = x.toString.split("=")
//          val student = student_partic_correct(0)
//          val partic_correct = student_partic_correct(1)
//          val Lukey = s"${lesson_id}_${student}"
//          val objMap = map - ("lesson_id", "examType") ++ Map("participate_num" -> partic_correct.split("_")(0).toInt,
//            "right_num" -> partic_correct.split("_")(1).toInt)
//          (Lukey, List(EXAM1_ACTIVEANDRIGHTCOUNT, Map(
//            "lesson_id" -> lesson_id.toInt,
//            "student_uid" -> student.toLong,
//            "course_type" -> courseType.toInt,
//            "is_short_train" -> shortTraining.toInt,
//            "exam1" -> objMap
//          )))
//        }).toList
//      case _ => List.empty
//    }
//
//  }
//
//  /**
//    * 小初高预习的提交时间数据处理
//    * 数据结构：
//    * ["lu_exam13_submit_time", {
//    * "student_uid": "2385011744",
//    * "lesson_id": "245217",
//    * "exam13":{
//    * "submit_time": 1569897900,
//    * "block_exam13_update_time": 1567353426
//    * }
//    * }]
//    * 小学预习为exam5
//    *
//    * @param redis
//    * @param map
//    * @return
//    */
//  def answerinfoCompute(redis: Jedis, map: Map[String, Any]): List[(String, List[Any])] = {
//    //redis.set(s"${LcRediskey}_${lesson_id}", s"${course_id}_${lesson_type}")
//    val student_uid = map("student_uid").toString
//    val lesson_id = map("lesson_id").toString
//    val examType = map("examType").toString
//    val Lukey = s"${lesson_id}_${student_uid}"
//    val course_id = if (redis.exists(s"${LC}_${lesson_id}")) redis.get(s"${LC}_${lesson_id}").split("_")(0) else "0"
//    val course_info = if (redis.hexists(TBL_COURSE_INFO, course_id)) redis.hget(TBL_COURSE_INFO, course_id) else ""
//    val course_map = Try(JsonUtil.toAny[Map[String, Any]](course_info)).getOrElse(Map.empty)
//    val courseType = course_map.get("courseType").getOrElse("-1").toString
//    val shortTraining = course_map.get("shortTraining").getOrElse("2").toString
//    val resObjMap = map - ("student_uid", "lesson_id", "examType")
//    val resMap = Map(
//      "student_uid" -> student_uid.toLong,
//      "lesson_id" -> lesson_id.toInt,
//      "course_type" -> courseType.toInt,
//      "is_short_train" -> shortTraining.toInt,
//      (if (examType == "5") "exam5" else "exam13") -> resObjMap)
//    List((Lukey, List((if (examType == "5") EXAM5_SUBMIT_TIME else EXAM13_SUBMIT_TIME), resMap)))
//  }
//
//
//  /**
//    * 互动题参与数正确数推向下游ES和kafka数据处理
//    * 数据结构：
//    * ["lu_exam1_activeAndRight", {
//    * "student_uid": "2385011744",
//    * "lesson_id": "245217",
//    * "exam1":{
//    * "participate_num": 5,
//    * "right_num":4,
//    * "block_exam1_update_time": 1567353426
//    * }
//    * }]
//    *
//    * @param redis
//    * @param map
//    * @return
//    */
//  def huDongactiveAndRightcountCompute(redis: Jedis, map: Map[String, Any]): List[(String, List[Any])] = {
//    val student_uid = map("student_uid").toString
//    val lesson_id = map("lesson_id").toString
//    val course_id = if (redis.exists(s"${LC}_${lesson_id}")) redis.get(s"${LC}_${lesson_id}").split("_")(0) else "0"
//    val course_info = if (redis.hexists(TBL_COURSE_INFO, course_id)) redis.hget(TBL_COURSE_INFO, course_id) else ""
//    val course_map = Try(JsonUtil.toAny[Map[String, Any]](course_info)).getOrElse(Map.empty)
//    val courseType = course_map.get("courseType").getOrElse("-1").toString
//    val shortTraining = course_map.get("shortTraining").getOrElse("2").toString
//    val participate_num = map("participate_num").toString
//    val right_num = map("right_num").toString
//    val Lukey = s"${lesson_id}_${student_uid}"
//    redis.hset(s"${HUDONG_REDIS_PRE}_${lesson_id}", student_uid, s"${participate_num}_${right_num}")
//    redis.expire(s"${HUDONG_REDIS_PRE}_${lesson_id}", REDIS_EXPIRE_TIME)
//    if (redis.exists(s"${LESSON_TCOUNT}_${lesson_id}_1")) {
//      val total_num = redis.get(s"${LESSON_TCOUNT}_${lesson_id}_1")
//      val resObjMap = map - ("student_uid", "lesson_id") ++ Map("total_num" -> total_num.toInt)
//      val resMap = Map(
//        "student_uid" -> student_uid.toLong,
//        "lesson_id" -> lesson_id.toInt,
//        "course_type" -> courseType.toInt,
//        "is_short_train" -> shortTraining.toInt,
//        "exam1" -> resObjMap)
//      List((Lukey, List(EXAM1_ACTIVEANDRIGHTCOUNT, resMap)))
//    } else {
//      //表示如果关联不到redis，仍然将参与数正确数插入至ES
//      val resObjMap = map - ("student_uid", "lesson_id")
//      val resMap = Map(
//        "student_uid" -> student_uid.toLong,
//        "lesson_id" -> lesson_id.toInt,
//        "course_type" -> courseType.toInt,
//        "is_short_train" -> shortTraining.toInt,
//        "exam1" -> resObjMap)
//      List((Lukey, List(EXAM1_ACTIVEANDRIGHTCOUNT, resMap)))
//    }
//  }
//}
