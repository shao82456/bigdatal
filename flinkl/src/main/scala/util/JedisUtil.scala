package util

/**
 * Author: shaoff
 * Date: 2020/3/18 15:36
 * Package: jedisl.connect
 * Description:
 *
 */

import java.util.concurrent.ConcurrentHashMap

import org.slf4j.LoggerFactory
import redis.clients.jedis.exceptions.JedisConnectionException
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig, Pipeline}

import scala.annotation.meta.getter
import scala.collection.JavaConversions._
import scala.util._


object JedisUtil {

  val logger = LoggerFactory.getLogger(getClass)

  @transient
  @getter
  private lazy val jedis_pools: ConcurrentHashMap[String, JedisPool] =
    new ConcurrentHashMap[String, JedisPool]()

  /**
   * 创建或者获取一个Redis 连接池
   *
   * @param url
   * @return
   */
  def connect(url: String): Jedis = {
    val pools = jedis_pools.getOrElseUpdate(url, createJedisPool(url))
    var sleepTime: Int = 4
    var conn: Jedis = null
    while (conn == null) {
      try {
        conn = pools.getResource
      } catch {
        case e: JedisConnectionException if e.getCause.toString.
          contains("ERR max number of clients reached") => {
          if (sleepTime < 500) sleepTime *= 2
          Thread.sleep(sleepTime)
        }
        case e: Exception => throw e
      }
    }
    conn
  }

  /**
   * 创建一个连接池
   *
   * @param url
   * @return
   */
  def createJedisPool(url: String): JedisPool = {

    logger.info(s"createJedisPool with ${url} ")
    val poolConfig: JedisPoolConfig = new JedisPoolConfig()
    /*最大连接数*/
    poolConfig.setMaxTotal(300)
    /*最大空闲连接数*/
    poolConfig.setMaxIdle(64)
    /*在获取连接的时候检查有效性, 默认false*/
    poolConfig.setTestOnBorrow(true)
    poolConfig.setTestOnReturn(false)
    /*在空闲时检查有效性, 默认false*/
    poolConfig.setTestWhileIdle(false)
    /*逐出连接的最小空闲时间 默认300000毫秒(5分钟)*/
    poolConfig.setMinEvictableIdleTimeMillis(300000)
    /*逐出扫描的时间间隔(毫秒) 如果为负数,则不运行逐出线程, 默认-1*/
    poolConfig.setTimeBetweenEvictionRunsMillis(30000)
    poolConfig.setNumTestsPerEvictionRun(-1)
    val Array(ip, port) = url.split(":")
    new JedisPool(poolConfig,
      ip,
      port.toInt)
  }


  def safeClose[R](f: Jedis => R)(implicit jedis: Jedis): R = {
    val result = f(jedis)
    Try {
      jedis.close()
    } match {
      case Success(_) => logger.debug("jedis.close successful.")
      case Failure(_) => logger.error("jedis.close failed.")
    }
    result
  }

  def safeClosePipe[R](f: Pipeline => R)(implicit jedis: Jedis): R = {
    val pipe = jedis.pipelined()
    val result = f(pipe)
    Try {
      pipe.sync()
      pipe.close()
      jedis.close()
    } match {
      case Success(_) => logger.debug("pipe.close successful.")
      case Failure(_) => logger.error("pipe.close failed.")
    }
    result
  }

  def close(): Unit = {
    jedis_pools.foreach { case (_, pool) => pool.close() }
    jedis_pools.clear()
  }
}


//case class CodisEndPoint(host:String,
//                         port:Int,
//                         password: String = null) extends Serializable {
//
//  def this(conf: SparkConf, bnsConfName: String, passwordConfName: String) {
//    this(conf.get(bnsConfName, "codis-ykbzrdd.na.bjdd"), conf.get(passwordConfName, null))
//  }
//
//  def this(conf: SparkConf) {
//    this(conf, "spark.codis.bns", "spark.codis.passwd")
//  }
//}


