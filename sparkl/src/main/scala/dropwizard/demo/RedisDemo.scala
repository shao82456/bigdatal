package dropwizard.demo

/**
 * Author: shaoff
 * Date: 2020/3/19 17:15
 * Package: dropwizard.demo
 * Description:
 *
 */

import java.lang.reflect.Proxy
import java.util.concurrent.TimeUnit

import com.codahale.metrics._
import jedisl.connect.JedisUtil
import redis.clients.jedis.Jedis
import scala.collection.JavaConversions._

object RedisDemo {

  def main(args: Array[String]): Unit = {
    startReport()
    write("test1", Map("shao" -> "19", "wang" -> "17"))
    println(get("test1"))
    wait5Seconds()
    JedisUtil.close()
  }

  def write(key: String, data: Map[String, String]): Unit = {
    JedisUtil.safeClose(jedis => {
      val myJedis2 =jedis2MyJedis(jedis)
      myJedis2.hset(key, data)
    })(JedisUtil.connect("localhost:6379"))
  }

  def get(key: String): Map[String, String] = {
    JedisUtil.safeClose(jedis => {
      val myJedis2 = jedis2MyJedis(jedis)
      myJedis2.hgetAll(key).toMap
    })(JedisUtil.connect("localhost:6379"))
  }

  private[demo] def startReport(): Unit = {
    val reporter = ConsoleReporter.forRegistry(MyJedis2.metrics).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build
    reporter.start(1, TimeUnit.SECONDS)
  }

  private[demo] def wait5Seconds(): Unit = {
    try Thread.sleep(5 * 1000)
    catch {
      case e: InterruptedException =>

    }
  }

  def jedis2MyJedis(jedis: Jedis): MyJedis2 = {
    val myJedis = Proxy.newProxyInstance(classOf[MyJedis2].getClassLoader, Array[Class[_]](classOf[MyJedis2]), new MyJedis2.SimpleInvocationHandler(jedis)).asInstanceOf[MyJedis2]
    myJedis
  }

}

