//package jedisl.connect
//
//import redis.clients.jedis.{Jedis, JedisMonitor}
//
//import scala.collection.JavaConversions._
//
///**
// * Author: shaoff
// * Date: 2020/3/18 16:19
// * Package: jedisl.connect
// * Description:
// *
// */
//class MyMonitor extends JedisMonitor {
//  override def onCommand(command: String): Unit = {
//
//  }
//}
//
//object MonitorDemo {
//
//  def main(args: Array[String]): Unit = {
//    new Thread(new Runnable() {
//      override def run(): Unit = {
//        try // sleep 100ms to make sure that monitor thread runs first
//          Thread.sleep(100)
//        catch {
//          case e: InterruptedException =>
//
//        }
//        val j = new Jedis("localhost")
//        for (i <- 0 until 10) {
//          j.incr("test2")
//        }
//        j.disconnect
//      }
//    }).start()
//
//    val jedis = new Jedis("localhost")
//
//    jedis.monitor(new JedisMonitor() {
//      private var count = 0
//
//      def onCommand(command: String): Unit = {
//        if (command.contains("INCR")) count += 1
//        if (count == 5) client.disconnect
//        println(command)
//      }
//    })
//  }
//
//  def write(key: String, data: Map[String, String]): Unit = {
//    JedisUtil.safeClose(jedis => {
//      jedis.hset(key, data)
//    })(JedisUtil.connect("localhost:6379"))
//  }
//
//  def get(key: String): Map[String, String] = {
//    JedisUtil.safeClose(jedis => {
//      jedis.hgetAll(key).toMap
//    })(JedisUtil.connect("localhost:6379"))
//  }
//}
