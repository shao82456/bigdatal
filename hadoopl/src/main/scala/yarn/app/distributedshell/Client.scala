package yarn.app.distributedshell

import java.util

import org.apache.commons.cli.{GnuParser, HelpFormatter, Option, Options}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils
import org.slf4j.LoggerFactory
import yarn.demo.app.shell.DSConstants

/**
 * Author: shaoff
 * Date: 2020/6/5 19:24
 * Package: yarn.demoapp.shell
 * Description:
 *
 * args:
 * -jar /Users/sakura/stuff/bigdatal/hadoopl/target/hadoopl-1.0.jar
 * -shell_command ls
 * -shell_args -l
 *
 */
class Client(appMasterMainClass: String, conf: Configuration) {

  import Client._

  var opts: Options = _
  var debugFlag: Boolean = false
  var keepContainers = false
  var log4jPropFile: String = _

  // Application master specific info to register a new Application with RM/ASM
  private var appName = ""
  // App master priority
  private var amPriority = 0
  // Queue for App master
  private var amQueue = ""
  // Amt. of memory resource to request for to run the App Master
  private var amMemory = 10
  // Amt. of virtual core resource to request for to run the App Master
  private var amVCores = 1

  private var appMasterJar = ""
  private var shellCommand = ""
  // Location of shell script
  private var shellScriptPath = ""
  private var shellArgs: Array[String] = Array.empty[String]
  private val shellEnv: util.Map[String, String] = new util.HashMap[String, String]
  private var shellCmdPriority = 0

  // Amt of memory to request for container in which shell script will be executed
  private var containerMemory = 10
  // Amt. of virtual cores to request for container in which shell script will be executed
  private var containerVirtualCores = 1
  // No. of containers in which the shell script needs to be executed
  private var numContainers = 1
  private var nodeLabelExpression: String = _

  // Start time for client
  private val clientStartTime = System.currentTimeMillis
  // Timeout threshold for client. Kill app after time interval expires.
  private var clientTimeout = 600000
  private var attemptFailuresValidityInterval: Long = -1

  private var yarnClient: YarnClient = _

  //construct
  yarnClient = YarnClient.createYarnClient()
  yarnClient.init(conf)
  opts = new Options
  opts.addOption("appname", true, "Application Name. Default value - DistributedShell")
  opts.addOption("priority", true, "Application Priority. Default 0")
  opts.addOption("queue", true, "RM Queue in which this application is to be submitted")
  opts.addOption("timeout", true, "Application timeout in milliseconds")
  opts.addOption("master_memory", true, "Amount of memory in MB to be requested to run the application master")
  opts.addOption("master_vcores", true, "Amount of virtual cores to be requested to run the application master")
  opts.addOption("jar", true, "Jar file containing the application master")
  opts.addOption("shell_command", true, "Shell command to be executed by " + "the Application Master. Can only specify either --shell_command " + "or --shell_script")
  opts.addOption("shell_script", true, "Location of the shell script to be " + "executed. Can only specify either --shell_command or --shell_script")
  opts.addOption("shell_args", true, "Command line args for the shell script." + "Multiple args can be separated by empty space.")
  opts.getOption("shell_args").setArgs(Option.UNLIMITED_VALUES)
  opts.addOption("shell_env", true, "Environment for shell script. Specified as env_key=env_val pairs")
  opts.addOption("shell_cmd_priority", true, "Priority for the shell command containers")
  opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run the shell command")
  opts.addOption("container_vcores", true, "Amount of virtual cores to be requested to run the shell command")
  opts.addOption("num_containers", true, "No. of containers on which the shell command needs to be executed")
  opts.addOption("keep_containers_across_application_attempts", false, "Flag to indicate whether to keep containers across application attempts." + " If the flag is true, running containers will not be killed when" + " application attempt fails and these containers will be retrieved by" + " the new application attempt ")
  opts.addOption("attempt_failures_validity_interval", true, "when attempt_failures_validity_interval in milliseconds is set to > 0," + "the failure number will not take failures which happen out of " + "the validityInterval into failure count. " + "If failure count reaches to maxAppAttempts, " + "the application will be failed.")
  opts.addOption("debug", false, "Dump out debug information")
  opts.addOption("help", false, "Print usage")
  opts.addOption("node_label_expression", true, "Node label expression to determine the nodes" + " where all the containers of this application" + " will be allocated, \"\" means containers" + " can be allocated anywhere, if you don't specify the option," + " default node_label_expression of queue will be used.")
  opts.addOption("log_properties", true, "log4j.properties file")


  def this() {
    this("yarn.app.distributedshell.AppMaster", new YarnConfiguration())
  }

  def init(args: Array[String]): Boolean = {
    val cliParser = new GnuParser().parse(opts, args)

    //    if (args.length == 0) throw new IllegalArgumentException("No args specified for client to initialize")
    if (cliParser.hasOption("help")) {
      printUsage()
      return true
    }

    if (cliParser.hasOption("debug")) {
      debugFlag = true
    }
    if (cliParser.hasOption("keep_containers_across_application_attempts")) {
      log.info("keep_containers_across_application_attempts")
      keepContainers = true
    }

    log4jPropFile = cliParser.getOptionValue("log_properties", "")

    appName = cliParser.getOptionValue("appname", "DistributedShell")
    amPriority = cliParser.getOptionValue("priority", "0").toInt
    amQueue = cliParser.getOptionValue("queue", "default")
    amMemory = cliParser.getOptionValue("master_memory", "10").toInt
    amVCores = cliParser.getOptionValue("master_vcores", "1").toInt

    if (amMemory < 0) throw new IllegalArgumentException("Invalid memory specified for application master, exiting." + " Specified memory=" + amMemory)
    if (amVCores < 0) throw new IllegalArgumentException("Invalid virtual cores specified for application master, exiting." + " Specified virtual cores=" + amVCores)

    if (!cliParser.hasOption("jar")) throw new IllegalArgumentException("No jar file specified for application master")

    appMasterJar = cliParser.getOptionValue("jar")

    if (!cliParser.hasOption("shell_command") && !cliParser.hasOption("shell_script")) {
      throw new IllegalArgumentException("No shell command or shell script specified to be executed by application master")
    } else if (cliParser.hasOption("shell_command") && cliParser.hasOption("shell_script")) {
      throw new IllegalArgumentException("Can not specify shell_command option " + "and shell_script option at the same time")
    } else if (cliParser.hasOption("shell_command")) shellCommand = {
      cliParser.getOptionValue("shell_command")
    } else {
      shellScriptPath = cliParser.getOptionValue("shell_script")
    }
    if (cliParser.hasOption("shell_args")) {
      shellArgs = cliParser.getOptionValues("shell_args")
    }
    if (cliParser.hasOption("shell_env")) {
      val envs = cliParser.getOptionValues("shell_env").map(_.trim)
      for (env <- envs) {
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

    shellCmdPriority = cliParser.getOptionValue("shell_cmd_priority", "0").toInt
    containerMemory = cliParser.getOptionValue("container_memory", "10").toInt
    containerVirtualCores = cliParser.getOptionValue("container_vcores", "1").toInt
    numContainers = cliParser.getOptionValue("num_containers", "1").toInt

    if (containerMemory < 0 || containerVirtualCores < 0 || numContainers < 1) {
      throw new IllegalArgumentException("Invalid no. of containers or container memory/vcores specified," + " exiting." + " Specified containerMemory=" + containerMemory + ", containerVirtualCores=" + containerVirtualCores + ", numContainer=" + numContainers)
    }

    nodeLabelExpression = cliParser.getOptionValue("node_label_expression", null)
    clientTimeout = cliParser.getOptionValue("timeout", "600000").toInt

    attemptFailuresValidityInterval = cliParser.getOptionValue("attempt_failures_validity_interval", "-1").toLong
    true
  }

  def run(): Boolean = {
    log.info("Running Client")
    yarnClient.start()

    val clusterMetrics = yarnClient.getYarnClusterMetrics
    log.info("Got Cluster metric info from ASM" + ", numNodeManagers=" + clusterMetrics.getNumNodeManagers)

    val clusterNodeReports = yarnClient.getNodeReports(NodeState.RUNNING)
    log.info("Got Cluster node info from ASM")
    import scala.collection.JavaConversions._
    for (node <- clusterNodeReports) {
      log.info("Got node report from ASM for" + ", nodeId=" + node.getNodeId + ", nodeAddress" + node.getHttpAddress + ", nodeRackName" + node.getRackName + ", nodeNumContainers" + node.getNumContainers)
    }

    val queueInfo = yarnClient.getQueueInfo(this.amQueue)
    log.info("Queue info" + ", queueName=" + queueInfo.getQueueName + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity + ", queueApplicationCount=" + queueInfo.getApplications.size + ", queueChildQueueCount=" + queueInfo.getChildQueues.size)

    val listAclInfo = yarnClient.getQueueAclsInfo
    import scala.collection.JavaConversions._
    for (aclInfo <- listAclInfo) {
      import scala.collection.JavaConversions._
      for (userAcl <- aclInfo.getUserAcls) {
        log.info("User ACL Info for Queue" + ", queueName=" + aclInfo.getQueueName + ", userAcl=" + userAcl.name)
      }
    }

    // Get a new application id
    val app = yarnClient.createApplication
    val appResponse = app.getNewApplicationResponse
    val maxMem = appResponse.getMaximumResourceCapability.getMemory
    log.info("Max mem capabililty of resources in this cluster " + maxMem)

    // A resource ask cannot exceed the max. 
    if (amMemory > maxMem) {
      log.info("AM memory specified above max threshold of cluster. Using max value." + ", specified=" + amMemory + ", max=" + maxMem)
      amMemory = maxMem
    }

    val maxVCores = appResponse.getMaximumResourceCapability.getVirtualCores
    log.info("Max virtual cores capabililty of resources in this cluster " + maxVCores)

    if (amVCores > maxVCores) {
      log.info("AM virtual cores specified above max threshold of cluster. " + "Using max value." + ", specified=" + amVCores + ", max=" + maxVCores)
      amVCores = maxVCores
    }

    // set the application name
    val appContext = app.getApplicationSubmissionContext
    val appId = appContext.getApplicationId
    appContext.setKeepContainersAcrossApplicationAttempts(keepContainers)
    appContext.setApplicationName(appName)
    appContext.setApplicationType("DistributedShell")

    if (attemptFailuresValidityInterval >= 0) appContext.setAttemptFailuresValidityInterval(attemptFailuresValidityInterval)

    // set local resources for the application master
    // local files or archives as needed
    // In this scenario, the jar file for the application master is part of the local resources			
    val localResources = new util.HashMap[String, LocalResource]

    log.info("Copy App Master jar from local filesystem and add to local environment")
    // Copy the application master jar to the filesystem 
    // Create a local resource to point to the destination jar path 
    val fs = FileSystem.get(conf)
    addToLocalResources(fs, appMasterJar, appMasterJarPath, appId.toString, localResources, null)
    // Set the log4j properties if needed
    if (!log4jPropFile.isEmpty) addToLocalResources(fs, log4jPropFile, log4jPath, appId.toString, localResources, null)

    // The shell script has to be made available on the final container(s)
    // where it will be executed. 
    // To do this, we need to first copy into the filesystem that is visible 
    // to the yarn framework. 
    // We do not need to set this as a local resource for the application 
    // master as the application master does not need it. 		
    var hdfsShellScriptLocation = ""
    var hdfsShellScriptLen = 0L
    var hdfsShellScriptTimestamp = 0L
    if (!shellScriptPath.isEmpty) {
      val shellSrc = new Path(shellScriptPath)
      val shellPathSuffix = appName + "/" + appId.toString + "/" + SCRIPT_PATH
      val shellDst = new Path(fs.getHomeDirectory, shellPathSuffix)
      fs.copyFromLocalFile(false, true, shellSrc, shellDst)
      hdfsShellScriptLocation = shellDst.toUri.toString
      val shellFileStatus = fs.getFileStatus(shellDst)
      hdfsShellScriptLen = shellFileStatus.getLen
      hdfsShellScriptTimestamp = shellFileStatus.getModificationTime
    }

    if (!shellCommand.isEmpty) {
      addToLocalResources(fs, null, shellCommandPath, appId.toString, localResources, shellCommand)
    }

    if (shellArgs.length > 0) {
      addToLocalResources(fs, null, shellArgsPath, appId.toString, localResources, shellArgs.mkString(" "))
    }

    // Set the env variables to be setup in the env where the application master will be run
    log.info("Set the environment for the application master")
    val env = new util.HashMap[String, String]

    // put location of shell script into env
    // using the env info, the application master will create the correct local resource for the 
    // eventual containers that will be launched to execute the shell scripts
    env.put(DSConstants.DS_SCRIPT_LOCATION, hdfsShellScriptLocation)
    env.put(DSConstants.DS_SCRIPT_TIMESTAMP, hdfsShellScriptTimestamp.toString)
    env.put(DSConstants.DS_SCRIPT_LEN, hdfsShellScriptLen.toString)

    // Add AppMaster.jar location to classpath 		
    // At some point we should not be required to add 
    // the hadoop specific classpaths to the env. 
    // It should be provided out of the box. 
    // For now setting all required classpaths including
    // the classpath to "." for the application jar
    val classPathEnv = new StringBuilder(Environment.CLASSPATH.$$).append(ApplicationConstants.CLASS_PATH_SEPARATOR).append(".")
    for (s <- YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH) {
      classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR)
      classPathEnv.append(s.trim)
    }
    classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append(appMasterJarPath)
    println(classPathEnv.toString)
    env.put("CLASSPATH", classPathEnv.toString)

    // Set the necessary command to execute the application master 
    val vargs = new util.Vector[CharSequence](30)

    // Set java executable command 
    log.info("Setting up app master command")
    vargs.add(Environment.JAVA_HOME.$$ + "/bin/java")
    // Set Xmx based on am memory size
    vargs.add("-Xmx" + amMemory + "m")
    // Set class name 
    vargs.add(appMasterMainClass)
    // Set params for Application Master
    vargs.add("--container_memory " + String.valueOf(containerMemory))
    vargs.add("--container_vcores " + String.valueOf(containerVirtualCores))
    vargs.add("--num_containers " + String.valueOf(numContainers))
    if (null != nodeLabelExpression) appContext.setNodeLabelExpression(nodeLabelExpression)
    vargs.add("--priority " + String.valueOf(shellCmdPriority))

    import scala.collection.JavaConversions._
    for (entry <- shellEnv.entrySet) {
      vargs.add("--shell_env " + entry.getKey + "=" + entry.getValue)
    }
    if (debugFlag) vargs.add("--debug")

    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout")
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr")

    // Get final commmand
    val command = vargs.mkString(" ")
    log.info("Completed setting up app master command " + command)
    val commands = new util.ArrayList[String]
    commands.add(command)

    // Set up the container launch context for the application master
    val amContainer = ContainerLaunchContext.newInstance(localResources, env, commands, null, null, null)

    // Set up resource type requirements
    // For now, both memory and vcores are supported, so we set memory and 
    // vcores requirements
    appContext.setResource(Resource.newInstance(amMemory, amVCores))
    appContext.setAMContainerSpec(amContainer)

    // Set the priority for the application master
    // TODO - what is the range for priority? how to decide? 
    appContext.setPriority(Priority.newInstance(amPriority))
    // Set the queue to which this application is to be submitted in the RM
    appContext.setQueue(amQueue)

    // Submit the application to the applications manager
    // SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);
    // Ignore the response as either a valid response object is returned on success 
    // or an exception thrown to denote some form of a failure
    log.info("Submitting application to ASM")
    yarnClient.submitApplication(appContext)
    // TODO
    // Try submitting the same request again
    // app submission failure?

    // Monitor the application
    monitorApplication(appId)
  }

  def printUsage(): Unit = {
    new HelpFormatter().printHelp("Client", opts)
  }

  private def monitorApplication(appId: ApplicationId): Boolean = {
    while (true) {
      // Check app status every 1 second.
      try {
        Thread.sleep(1000)
      }
      catch {
        case e: InterruptedException =>
          log.debug("Thread sleep in monitoring loop interrupted")
      }
      // Get application report for the appId we are interested in
      val report = yarnClient.getApplicationReport(appId)
      log.info("Got application report from ASM for" + ", appId=" + appId.toString + ", clientToAMToken=" + report.getClientToAMToken + ", appDiagnostics=" + report.getDiagnostics + ", appMasterHost=" + report.getHost + ", appQueue=" + report.getQueue + ", appMasterRpcPort=" + report.getRpcPort + ", appStartTime=" + report.getStartTime + ", yarnAppState=" + report.getYarnApplicationState.toString + ", distributedFinalState=" + report.getFinalApplicationStatus.toString + ", appTrackingUrl=" + report.getTrackingUrl + ", appUser=" + report.getUser)
      val state = report.getYarnApplicationState
      val dsStatus = report.getFinalApplicationStatus
      if (YarnApplicationState.FINISHED eq state) {
        if (FinalApplicationStatus.SUCCEEDED eq dsStatus) {
          log.info("Application has completed successfully. Breaking monitoring loop")
          return true
        }
        else {
          log.info("Application did finished unsuccessfully." + " YarnState=" + state.toString + ", DSFinalStatus=" + dsStatus.toString + ". Breaking monitoring loop")
          return false
        }
      }
      else if ((YarnApplicationState.KILLED eq state) || (YarnApplicationState.FAILED eq state)) {
        log.info("Application did not finish." + " YarnState=" + state.toString + ", DSFinalStatus=" + dsStatus.toString + ". Breaking monitoring loop")
        return false
      } else if (System.currentTimeMillis > (clientStartTime + clientTimeout)) {
        log.info("Reached client specified timeout for application. Killing application")
        forceKillApplication(appId)
        return false
      }
    }
    false
  }

  private def addToLocalResources(fs: FileSystem, fileSrcPath: String, fileDstPath: String, appId: String, localResources: util.Map[String, LocalResource], resources: String): Unit = {
    val suffix = appName + "/" + appId + "/" + fileDstPath
    val dst = new Path(fs.getHomeDirectory, suffix)
    if (fileSrcPath == null) {
      var ostream: FSDataOutputStream = null
      try {
        ostream = FileSystem.create(fs, dst, new FsPermission(710.toShort))
        ostream.writeUTF(resources)
      } finally {
        if (ostream != null) {
          ostream.close()
        }
      }
    } else {
      fs.copyFromLocalFile(new Path(fileSrcPath), dst)
    }
    val scFileStatus = fs.getFileStatus(dst)
    val scRsrc = LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(dst.toUri), LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, scFileStatus.getLen, scFileStatus.getModificationTime)
    localResources.put(fileDstPath, scRsrc)
  }


  private def forceKillApplication(appId: ApplicationId): Unit = {
    yarnClient.killApplication(appId)
  }
}

object Client {
  private val log = LoggerFactory.getLogger(this.getClass)
  private val shellCommandPath = "shellCommands"
  private val shellArgsPath = "shellArgs"
  private val appMasterJarPath = "AppMaster.jar"
  // Hardcoded path to custom log_properties
  private val log4jPath = "log4j.properties"
  val SCRIPT_PATH = "ExecScript"

  def main(args: Array[String]): Unit = {
    var result = false
    try {
      val client = new Client()
      log.info("Initializing Client")
      try {
        val doRun = client.init(args)
        if (!doRun) System.exit(0)
      } catch {
        case e: IllegalArgumentException =>
          System.err.println(e.getLocalizedMessage)
          client.printUsage()
          System.exit(-1)
      }
      result = client.run()
    } catch {
      case t: Throwable =>
        log.error("Error running Client", t)
        System.exit(1)
    }
    if (result) {
      log.info("Application completed successfully")
      System.exit(0)
    }
    log.error("Application failed to complete successfully")
    System.exit(2)
  }
}