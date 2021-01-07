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

class InjectJedisPipeLineAgent extends ClassFileTransformer {
  val injectedClassName = "redis.clients.jedis.Pipeline"

  @throws[IllegalClassFormatException]
  def transform(loader: ClassLoader, className: String, classBeingRedefined: Class[_], protectionDomain: ProtectionDomain, classfileBuffer: Array[Byte]): Array[Byte] = {
    val clazzName = className.replace('/', '.')
    if (clazzName.equals(injectedClassName)) {
      try {
        val ctclass = ClassPool.getDefault.get(clazzName) // 使用全称,用于取得字节码类<使用javassist>
        val ctmethods = ctclass.getMethods
        for (ctMethod <- ctmethods) {
          if (ctMethod.getName.startsWith("sync")) { //
            println(ctMethod.getName)
            ctMethod.addLocalVariable("startTime", CtClass.longType)
            ctMethod.insertBefore("startTime=System.currentTimeMillis();")
            ctMethod.insertAfter("agent.JedisInstrument.pipeLineEnd(\" " + ctMethod.getName() + "\",startTime);")
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