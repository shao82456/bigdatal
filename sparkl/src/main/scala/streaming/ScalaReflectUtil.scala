package streaming

import streaming.KafkaStreaming.getClass

/**
 * Author: shaoff
 * Date: 2020/9/27 12:04
 * Package: streaming
 * Description:
 *
 */
object ScalaReflectUtil {
  import scala.reflect.runtime.universe
  val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)

  def invokeMethodOfObject(objectName:String,methodName:String, args:Array[Any]):Any={
//    val module = runtimeMirror.staticModule("org.apache.spark.streaming.kafka010.CachedKafkaConsumer")
    val module = runtimeMirror.staticModule(objectName)
    val methods = runtimeMirror.reflectModule(module)
    val objMirror = runtimeMirror.reflect(methods.instance)
    val method = methods.symbol.typeSignature.member(universe.TermName(methodName)).asMethod
    objMirror.reflectMethod(method)(args)
  }

  def invokeMethod(instance:Any,clazz:Class[_],methodName:String,args:Array[Object]):Any={
    val m=clazz.getDeclaredMethod(methodName)
    m.setAccessible(true)
    m.invoke(instance, args:_*)
  }
}
