package agent
import java.util.concurrent.ConcurrentHashMap

import com.codahale.metrics.{Gauge, Metric, MetricFilter, MetricRegistry}


/**
 * Author: shaoff
 * Date: 2020/9/28 19:43
 * Package: agent
 * Description:
 *
 */
class JedisInstrument {

}

object JedisInstrument{
  val registry=new MetricRegistry() {}
  val h1 = registry.histogram("a")
  val duration = new ConcurrentHashMap[String, Long]()

  def methodStart(name:String): Unit ={
    println(s"start $name")
  }

  def methodEnd(name:String,startTime:Long): Unit ={
    val duration=System.currentTimeMillis()-startTime
    println(s"end $name:$duration")
  }

  def pipeLineEnd(name:String,startTime:Long,size:Int): Unit ={
    //pipeline.sync在close时也会调用
    val duration = System.currentTimeMillis() - startTime
    if(duration>1){
      println(s"end $name:$duration")
    }
  }
}
