package util

import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
 * Author: shaoff
 * Date: 2020/7/2 15:12
 * Package: util
 * Description:
 *
 */
object JsonUtil {
  implicit val formats = org.json4s.DefaultFormats

  def toAny[T: Manifest](s: String): T = parse(s).extract[T]

  def parseToJValue(s: String): JValue = parse(s)


  def toJson(o: Any): String = compact(toJValue(o))

  def toJValue(o: Any): JValue = Extraction.decompose(o)

  def main(args: Array[String]): Unit = {
    val source = """{ "some": "JSON source" }"""
    val jsonAst = toAny[Map[String, Any]](source)
    val newSource = Map("timestamp" -> System.currentTimeMillis()) ++ jsonAst
    val source2 = toJson(newSource)

    println(source2)
    case class C1(some: String, timestamp: Long)
    val obj=toAny[C1](source2)
    println(obj)

    println(JsonUtil.toJson(C1(null,123)))
  }

}