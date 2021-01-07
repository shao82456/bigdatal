package agent

import jedisl.connect.JedisUtil
import redis.clients.jedis.Response

import scala.collection.JavaConversions._

/**
 * Author: shaoff
 * Date: 2020/3/18 15:23
 * Package: jedisl.connect
 * Description:
 * java -javaagent:/Users/sakura/stuff/bigdatal/sparkl/target/sparkl-1.0.jar
 */
object JedisDemo {
  def main(args: Array[String]): Unit = {
        write("test1",Map("shao"->"19","wang"->"17"))
        write("test3",Map("shao"->"xj","wang"->"db"))
        print(get("test1"))
//    pipeline
    JedisUtil.close()
  }

  def write(key: String, data: Map[String, String]): Unit = {
    JedisUtil.safeClose(jedis => {
      jedis.hset(key, data)
    })(JedisUtil.connect("localhost:6379"))
  }

  def get(key: String): Map[String, String] = {
    JedisUtil.safeClose(jedis => {
      jedis.hgetAll(key).toMap
    })(JedisUtil.connect("localhost:6379"))
  }

  def pipeline() = {
    val res: Array[String] = JedisUtil.safeClosePipe { pipe =>
      val v1 = pipe.hget("test1", "shao")
      val v2 = pipe.hget("test3", "lee")
      Array(v1, v2).flatMap(Option(_).map(_.toString))
    }(JedisUtil.connect("localhost:6379"))

    println(res.mkString(","))
  }

}
