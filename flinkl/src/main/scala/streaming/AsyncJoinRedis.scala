//package streaming
//
//
//import java.util.{Collections, Properties}
//import java.util.concurrent.TimeUnit
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.datastream.AsyncDataStream
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
//import scala.util.parsing.json.JSONObject
//
//
///*
//    关于异步IO原理的讲解可以参考浪尖的知乎～：
//    https://zhuanlan.zhihu.com/p/48686938
// */
//object AsyncJoinRedis {
//
//  def main(args: Array[String]): Unit = { // set up the streaming execution environment
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    // 选择设置事件事件和处理事件
//    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
//    val properties = new Properties
//    properties.setProperty("bootstrap.servers", "localhost:9093")
//    properties.setProperty("group.id", "AsyncIOSideTableJoinRedis")
//    val kafkaConsumer010 = new FlinkKafkaConsumer010[_]("jsontest", new Nothing, properties)
//    val source = env.addSource(kafkaConsumer010)
//    val asyncFunction = new AsyncIOSideTableJoinRedis.SampleAsyncFunction
//    // add async operator to streaming job
//    var result = null
//    if (true) result = AsyncDataStream.orderedWait(source, asyncFunction, 1000000L, TimeUnit.MILLISECONDS, 20).setParallelism(1)
//    else result = AsyncDataStream.unorderedWait(source, asyncFunction, 10000, TimeUnit.MILLISECONDS, 20).setParallelism(1)
//    result.print
//    env.execute(classOf[AsyncIOSideTableJoinRedis].getCanonicalName)
//  }
//
//  private class SampleAsyncFunction extends RichAsyncFunction[JSONObject, JSONObject] {
//    private var redisClient = null
//
//    @throws[Exception]
//    override def open(parameters: Configuration): Unit = {
//      super.open(parameters)
//      val config = new Nothing
//      config.setHost("127.0.0.1")
//      config.setPort(6379)
//      val vo = new Nothing
//      vo.setEventLoopPoolSize(10)
//      vo.setWorkerPoolSize(20)
//      val vertx = Vertx.vertx(vo)
//      redisClient = RedisClient.create(vertx, config)
//    }
//
//    @throws[Exception]
//    override def close(): Unit = {
//      super.close()
//      if (redisClient != null) redisClient.close(null)
//    }
//
//    override def asyncInvoke(input: JSONObject, resultFuture: ResultFuture[JSONObject]): Unit = {
//      val fruit = input.getString("fruit")
//      // 获取hash-key值
//      //            redisClient.hget(fruit,"hash-key",getRes->{
//      //            });
//      // 直接通过key获取值，可以类比
//      redisClient.get(fruit, (getRes) => {
//        def foo(getRes) = if (getRes.succeeded) {
//          val result = getRes.result
//          if (result == null) {
//            resultFuture.complete(null)
//          }
//          else {
//            input.put("docs", result)
//            resultFuture.complete(Collections.singleton(input))
//          }
//        }
//        else if (getRes.failed) {
//          resultFuture.complete(null)
//          return
//        }
//
//        foo(getRes)
//      })
//    }
//  }
//
//}