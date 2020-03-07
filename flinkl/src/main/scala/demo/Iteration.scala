package demo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
/**
 * Author: shaoff
 * Date: 2020/2/20 11:57
 * Package: demo
 * Description:
 *
 */
object Iteration {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironment()

    val lines:DataStream[Long] = env.generateSequence(1,1000)
    val iteratedStream=lines.iterate(it=>{
      val minusOne = it.map( v => v - 1)
      val stillGreaterThanZero = minusOne.filter (_ > 0)
      val lessThanZero = minusOne.filter(_ <= 0)
      (stillGreaterThanZero,lessThanZero)
    })

    iteratedStream.print()
    // build your program
    env.execute("iteration")
  }
}
