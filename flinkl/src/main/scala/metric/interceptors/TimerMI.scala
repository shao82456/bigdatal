package metric.interceptors

import java.io.Serializable
import java.lang.reflect.Method

import net.sf.cglib.proxy.{MethodInterceptor, MethodProxy};

abstract class TimerMI extends MethodInterceptor with Serializable {

  override def intercept(obj: AnyRef, method: Method, args: Array[AnyRef], methodProxy: MethodProxy): AnyRef = {
    if(whetherMetric(method)){
      methodProxy.invokeSuper(obj, args)
    }else{
      val start = System.currentTimeMillis()
      val res = methodProxy.invokeSuper(obj, args)
      val elapsed = System.currentTimeMillis() - start
      update(elapsed)
      res
    }
  }

  def update(duration: Long): Unit

  //是否统计
  def whetherMetric(method: Method):Boolean
}
