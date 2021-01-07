package yarn

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import yarn.ClientDemo.yarnClient

/**
 * Author: shaoff
 * Date: 2020/4/24 11:48
 * Package: yarn
 * Description:
 *
 */
object ClientDemo {
  val yarnClient = YarnClient.createYarnClient

  def main(args: Array[String]): Unit = {
    System.setProperty("spark.yarn.jars", "a.jar")
    val spConf = new SparkConf(true)
//    println(spConf.getAll.toMap)

    val hadoopConf = SparkHadoopUtil.get.newConfiguration(spConf)
    println(hadoopConf.get("fs.defaultFS"))
    val yarnConf = new YarnConfiguration(hadoopConf)
    yarnClient.init(yarnConf)
    yarnClient.start()
    val app=yarnClient.createApplication()
    println(app.getApplicationSubmissionContext)
    println(app.getNewApplicationResponse)
    println(yarnClient.getAllQueues)
  }

}
