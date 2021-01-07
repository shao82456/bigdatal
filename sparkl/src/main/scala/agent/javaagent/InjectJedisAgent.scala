package agent.javaagent

/**
 * Author: shaoff
 * Date: 2020/9/28 19:05
 * Package: agent.javaagent
 * Description:
 *
 */

import java.lang.instrument.{ClassFileTransformer, IllegalClassFormatException}
import java.security.ProtectionDomain

import javassist.{ClassPool, CtClass}

class InjectJedisAgent extends ClassFileTransformer {
  val injectedClassName = "redis.clients.jedis.Jedis"
  val instrumentedMethods = Class.forName("redis.clients.jedis.JedisCommands").getDeclaredMethods.map(_.getName).toSet
  println("instrumented:" + instrumentedMethods)

  @throws[IllegalClassFormatException]
  def transform(loader: ClassLoader, className: String, classBeingRedefined: Class[_], protectionDomain: ProtectionDomain, classfileBuffer: Array[Byte]): Array[Byte] = {
    val clazzName = className.replace('/', '.')
    if (clazzName.equals(injectedClassName)) {
      try {
        val ctclass = ClassPool.getDefault.get(clazzName) // 使用全称,用于取得字节码类<使用javassist>
        val ctmethods = ctclass.getMethods
        for (ctMethod <- ctmethods) {


          if (instrumentedMethods.contains(ctMethod.getName)) { //
            ctMethod.addLocalVariable("startTime",CtClass.longType)
            ctMethod.insertBefore("startTime=System.currentTimeMillis();")
//            ctMethod.insertBefore()
            ctMethod.insertAfter("agent.JedisInstrument.methodEnd(\" " +ctMethod.getName() +"\",startTime);")
//            ctMethod.insertBefore("System.out.println(\"hello Im agent : " + ctMethod.getName + "\");")
          }
        }
        return ctclass.toBytecode
      } catch {
        case t: Throwable =>
          System.out.println(t.getMessage)
          t.printStackTrace()
      }
    }
    null
  }
}