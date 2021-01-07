package shao

import io.prometheus.client.dropwizard.DropwizardExports

import scala.util.matching.Regex

object TmpReg {

  def main(args: Array[String]): Unit = {
    val reg = new Regex("(.*driver_|.*_\\d+_)(.+)")

//    val s1="application_1600933308957_0350.1.KafkaConsumer.test_shao-p1.response_rate"
//    val s2=DropwizardExports.sanitizeMetricName(s1)
//    println(s2)
//    val res=reg.replaceAllIn(s2,"$2")
//    println(res)

    val instrumentedMethods = Class.forName("redis.clients.jedis.JedisCommands").getDeclaredMethods.map(_.getName).toSet
    println(instrumentedMethods.mkString(","))
    val instrumentedMethods2 = "zrevrangeByLex,zrangeByScore,del,hgetAll,setex,zrangeByLex,getbit,strlen,hstrlen,move,ltrim,zrangeByScoreWithScores,psetex,lindex,sadd,getSet,geodist,zremrangeByLex,georadiusReadonly,touch,srem,setnx,hmset,scard,sscan,georadiusByMemberReadonly,restore,zcount,pttl,zrevrangeByScore,bitfield,linsert,set,hsetnx,pfadd,lrange,zrevrank,geohash,zrevrangeWithScores,llen,incrBy,zrem,lrem,zrange,setrange,hkeys,georadius,incrByFloat,dump,geoadd,pexpireAt,zrevrangeByScoreWithScores,hincrByFloat,hget,geopos,decrBy,pexpire,lpush,expireAt,smembers,rpush,hlen,zrank,unlink,setbit,zlexcount,persist,srandmember,spop,pfcount,zadd,rpop,hvals,getrange,hset,zcard,georadiusByMember,substr,zscore,exists,rpushx,echo,hdel,zremrangeByScore,lpushx,zrevrange,expire,get,hincrBy,sort,ttl,sismember,type,zincrby,incr,brpop,lset,zscan,bitcount,hexists,decr,blpop,bitpos,lpop,hscan,zrangeWithScores,hmget,append,zremrangeByRank".split(",").toSet

    println(instrumentedMethods2)
  }



}
