package org.fire.spark.streaming.core.monitor

import org.apache.spark.SparkConf
import org.fire.spark.streaming.core.plugins.redis.{RedisConnectionPool, RedisEndpoint}

import scala.util.Try

/**
  * Created by cloud on 2019/04/08.
  */
class LogToRedis(conf: SparkConf) {
  val redisEndpoint = new RedisEndpoint(conf)

  def logCount(key: String): Boolean = this.synchronized[Boolean] {
    RedisConnectionPool.safeClose(redis => Try {
      val cnt = Option(redis.get(key)).getOrElse("0").toLong
      redis.set(key, s"${cnt + 1}")
      true
    }.getOrElse(false))(RedisConnectionPool.connect(redisEndpoint))
  }

  def logMessage(key: String, msg: String): Boolean = {
    RedisConnectionPool.safeClose(redis => Try {
      redis.lpush(key, msg) > 0
    }.getOrElse(false))(RedisConnectionPool.connect(redisEndpoint))
  }

}
