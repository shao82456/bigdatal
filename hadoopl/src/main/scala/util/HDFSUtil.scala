package util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Author: shaoff
 * Date: 2020/7/17 16:22
 * Package: util
 * Description:
 *
 */
object HDFSUtil {
  val conf = new Configuration()
  val fs = FileSystem.get(conf)

  def delete(pathStr: String, recursive: Boolean = true): Unit = {
    val path = new Path(pathStr)
    fs.delete(path, recursive)
  }

  def delete_if_exist(pathStr: String): Unit = {
    val path = new Path(pathStr)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
  }

  /*def listDir(pathStr:String):List[String]={
    val path = new Path(pathStr)


  */

  def main(args: Array[String]): Unit = {
      delete_if_exist("test/output")
  }
}
