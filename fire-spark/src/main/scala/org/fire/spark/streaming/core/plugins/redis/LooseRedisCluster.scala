package org.fire.spark.streaming.core.plugins.redis

import java.util.concurrent.ConcurrentHashMap

import redis.clients.jedis.{Jedis, JedisPool}

/**
  * Created by cloud on 2019/03/27.
  */
class LooseRedisCluster(endpoints: List[RedisEndpoint]) {
  private val pools: ConcurrentHashMap[Int, JedisPool] = new ConcurrentHashMap[Int, JedisPool]

  def connect(key: String): Jedis = {
    pools.get(key.hashCode).getResource
  }
}
