package yarn.demo.app.shell

import java.io.{DataInputStream, File, FileInputStream}
import java.util

import org.apache.commons.cli.{GnuParser, HelpFormatter, Options}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records.{ApplicationAttemptId, FinalApplicationStatus}
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.apache.hadoop.yarn.util.ConverterUtils
import org.slf4j.LoggerFactory

/**
 * Author: shaoff
 * Date: 2020/6/5 19:24
 * Package: yarn.demoapp.shell
 * Description:
 *
 */
class AppMaster1 {

  import AppMaster1._
  private var conf:Configuration=new Configuration()
  private var appAttemptID: ApplicationAttemptId = _

  private val shellEnv: util.Map[String, String] = new util.HashMap[String, String]
  private var shellCommand = "ls"
  private var shellArgs = "-l"

  private var amRMClient:AMRMClient[ContainerRequest] = _

  def init(args: Array[String]): Boolean = {
    val opts = new Options
    opts.addOption("app_attempt_id", true, "App Attempt ID. Not to be used unless for testing purposes")
    opts.addOption("shell_env", true, "Environment for shell script. Specified as env_key=env_val pairs")
    opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run the shell command")
    opts.addOption("container_vcores", true, "Amount of virtual cores to be requested to run the shell command")
    opts.addOption("num_containers", true, "No. of containers on which the shell command needs to be executed")
    opts.addOption("priority", true, "Application Priority. Default 0")
    opts.addOption("debug", false, "Dump out debug information")
    opts.addOption("help", false, "Print usage")


    val cliParser = new GnuParser().parse(opts, args)
    if (args.length == 0) {
      new HelpFormatter().printHelp("ApplicationMaster", opts)
      throw new IllegalArgumentException("No args specified for application master to initialize")
    }

    if (cliParser.hasOption("debug")) {
      //        dumpOutDebugInfo()
    }

    val envs = System.getenv()
    if (envs.containsKey(Environment.CONTAINER_ID.name())) {
      val containerId = ConverterUtils.toContainerId(envs.get(Environment.CONTAINER_ID.name))
      appAttemptID = containerId.getApplicationAttemptId
    } else {
      throw new IllegalArgumentException("Container_id not set in environment")
    }

    if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV + " not set in the environment")
    if (!envs.containsKey(Environment.NM_HOST.name)) throw new RuntimeException(Environment.NM_HOST.name + " not set in the environment")
    if (!envs.containsKey(Environment.NM_HTTP_PORT.name)) throw new RuntimeException(Environment.NM_HTTP_PORT + " not set in the environment")
    if (!envs.containsKey(Environment.NM_PORT.name)) throw new RuntimeException(Environment.NM_PORT.name + " not set in the environment")

    log.info("Application master for app" + ", appId=" + appAttemptID.getApplicationId + ", clustertimestamp=" + appAttemptID.getApplicationId.getClusterTimestamp + ", attemptId=" + appAttemptID.getAttemptId)

    if (fileExist(shellCommandPath)) shellCommand = readContent(shellCommandPath)
    if (fileExist(shellArgsPath)) shellArgs = readContent(shellArgsPath)
    if (cliParser.hasOption("shell_env")) {
      val shellEnvs = cliParser.getOptionValues("shell_env").map(_.trim)
      for (env <- shellEnvs) {
        val index = env.indexOf('=')
        if (index == -1) {
          shellEnv.put(env, "")
        } else {
          val key = env.substring(0, index)
          val value = if (index < (env.length - 1)) "" else env.substring(index + 1)
          shellEnv.put(key, value)
        }
      }
    }

    //    containerMemory = cliParser.getOptionValue("container_memory", "10").toInt
    //    containerVirtualCores = cliParser.getOptionValue("container_vcores", "1").toInt
    //    numTotalContainers = cliParser.getOptionValue("num_containers", "1").toInt
    //    if (numTotalContainers == 0) throw new IllegalArgumentException("Cannot run distributed shell with no containers")
    //    requestPriority = cliParser.getOptionValue("priority", "0").toInt

    amRMClient = AMRMClient.createAMRMClient[ContainerRequest]
    amRMClient.init(conf)
    amRMClient.start()
    true
  }

  def run():Boolean={
    import sys.process._

    val cmd =shellCommand + " " + shellArgs
    val exitCode = cmd.!
    val res = exitCode == 0
    val  appStatus: FinalApplicationStatus =if(res) FinalApplicationStatus.SUCCEEDED else FinalApplicationStatus.FAILED
    amRMClient.unregisterApplicationMaster(appStatus, s"exit_code:$exitCode", null)
    res
  }
  private def fileExist(filePath: String): Boolean = new File(filePath).exists

  private def readContent(filePath: String): String = {
    var ds: DataInputStream = null
    try {
      ds = new DataInputStream(new FileInputStream(filePath))
      ds.readUTF
    } finally org.apache.commons.io.IOUtils.closeQuietly(ds)
  }
}

object AppMaster1 {
  private val log = LoggerFactory.getLogger(this.getClass)
  private val shellCommandPath = "shellCommands"
  private val shellArgsPath = "shellArgs"
  private val appMasterJarPath = "AppMaster.jar"
  // Hardcoded path to custom log_properties
  private val log4jPath = "log4j.properties"
  val SCRIPT_PATH = "ExecScript"

  def main(args: Array[String]): Unit = {
    var res = false
    try {
      val am = new AppMaster1()
      log.info("initializing application master")
      if (!am.init(args)) {
        System.exit(0)
      }
      res=am.run()
    } catch {
      case t: Throwable =>
        log.error(s"failed running application master,$t")
        System.exit(1)
    }
    if (res) {
      log.info("application master completed successful")
      System.exit(0)
    } else {
      log.info("application master failed. exiting")
      System.exit(2)
    }
  }
}