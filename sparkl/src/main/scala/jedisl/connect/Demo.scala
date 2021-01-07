package jedisl.connect
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
 * Author: shaoff
 * Date: 2020/3/18 15:23
 * Package: jedisl.connect
 * Description:
 *
 */
object Demo {
  def main(args: Array[String]): Unit = {
//    write("test1",Map("shao"->"19","wang"->"17"))
//    write("test3",Map("shao"->"xj","wang"->"db"))
//    print(get("test1"))
    pipeline
    JedisUtil.close()
  }

  def write(key:String,data:Map[String,String]):Unit={
    JedisUtil.safeClose(jedis=>{
      jedis.hset(key,data)
    })(JedisUtil.connect("localhost:6379"))
  }

  def get(key:String):Map[String,String]={
    JedisUtil.safeClose(jedis=>{
      jedis.hgetAll(key).toMap
    })(JedisUtil.connect("localhost:6379"))
  }

  def pipeline()={
    JedisUtil.safeClosePipe{pipe=>
      pipe.hget("test1","shao")
      pipe.hget("test3","lee")
//      println(pipe.get)
      val res=pipe.syncAndReturnAll().flatMap(Option(_).map(_.toString))
      println(res.mkString(","))
    }(JedisUtil.connect("localhost:6379"))
  }

}
