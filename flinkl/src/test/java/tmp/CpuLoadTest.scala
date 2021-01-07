package tmp

import java.lang.management.ManagementFactory
import java.util.concurrent.{Executors, TimeUnit}

import com.sun.management.OperatingSystemMXBean

/**
 * Author: shaoff
 * Date: 2020/8/21 16:21
 * Package: tmp
 * Description:
 *
 */
object CpuLoadTest {
  val ex = Executors.newCachedThreadPool()
  val buzyTask = new Runnable {
    override def run(): Unit = {
      while (true) {
        val x = 1 + 1
        Thread.sleep(10)
      }
    }
  }

  val reportTask = new Runnable {
    override def run(): Unit = {
      val mxBean = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean]
      while(true){
        println("load:" + mxBean.getProcessCpuLoad*100)
        Thread.sleep(1000*10)
      }
    }
  }


  def main(args: Array[String]): Unit = {
    ex.execute(reportTask)
      val coreNum=Runtime.getRuntime.availableProcessors()
      for(i<- 0 until coreNum ){
        //每30s增加一个task
        ex.execute(buzyTask)
      }
    ex.awaitTermination(1,TimeUnit.HOURS)
  }

}
