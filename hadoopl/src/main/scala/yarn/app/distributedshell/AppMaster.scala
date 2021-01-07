package yarn.app.distributedshell

import java.io.{DataInputStream, File, FileInputStream, IOException}
import java.net.{URI, URISyntaxException}
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import org.apache.commons.cli.{GnuParser, HelpFormatter, Options}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.util.Shell
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl
import org.apache.hadoop.yarn.client.api.async.{AMRMClientAsync, NMClientAsync}
import org.apache.hadoop.yarn.exceptions.YarnException
import org.apache.hadoop.yarn.util.ConverterUtils
import org.slf4j.LoggerFactory
import yarn.demo.app.shell.DSConstants

import scala.collection.mutable.ListBuffer

/**
 * Author: shaoff
 * Date: 2020/6/5 19:24
 * Package: yarn.demoapp.shell
 * Description:
 *
 */
class AppMaster {
  var numFailedContainers: AtomicInteger = new AtomicInteger(0)


  import AppMaster._

  private var conf: Configuration = new Configuration()
  private var appAttemptID: ApplicationAttemptId = _

  private val shellEnv: util.Map[String, String] = new util.HashMap[String, String]
  private var shellCommand = ""
  private var shellArgs = ""
  private var amRMClient: AMRMClientAsync[ContainerRequest] = _
  var launchThreads: ListBuffer[Thread] = ListBuffer.empty[Thread]
  private var done = false
  private val numCompletedContainers = new AtomicInteger(0)
  private var numTotalContainers = 0

  private var scriptPath = ""

  var containerMemory = 0
  var containerVirtualCores = 0
  var requestPriority = 0

  private var nmClientAsync: NMClientAsync = _
  private var containerListener: NMCallBackHandler = _

  private var shellScriptPathTimestamp = 0L
  private var shellScriptPathLen = 0L

  private val appMasterHostname = NetUtils.getHostname
  private val appMasterRpcPort = -1
  // Tracking url to which app master publishes info for clients to monitor
  private val appMasterTrackingUrl = ""

  private val numAllocatedContainers = new AtomicInteger()
  private val numRequestedContainers = new AtomicInteger()

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

    val envs: util.Map[String, String] = System.getenv()
    log.debug("envs: " + envs.toString)

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

    if (!fileExist(shellCommandPath) && envs.get(DSConstants.DS_SCRIPT_LOCATION).isEmpty)
      throw new IllegalArgumentException("No shell command or shell script specified to be executed by application master")

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

    if (envs.containsKey(DSConstants.DS_SCRIPT_LOCATION)) {
      scriptPath = envs.get(DSConstants.DS_SCRIPT_LOCATION)
      if (envs.containsKey(DSConstants.DS_SCRIPT_TIMESTAMP)) shellScriptPathTimestamp = envs.get(DSConstants.DS_SCRIPT_TIMESTAMP).toLong
      if (envs.containsKey(DSConstants.DS_SCRIPT_LEN)) shellScriptPathLen = envs.get(DSConstants.DS_SCRIPT_LEN).toLong
      if (!scriptPath.isEmpty && (shellScriptPathTimestamp <= 0 || shellScriptPathLen <= 0)) {
        log.error("Illegal values in env for shell script path" + ", path=" + scriptPath + ", len=" + shellScriptPathLen + ", timestamp=" + shellScriptPathTimestamp)
        throw new IllegalArgumentException("Illegal values in env for shell script path")
      }
    }

    containerMemory = cliParser.getOptionValue("container_memory", "10").toInt
    containerVirtualCores = cliParser.getOptionValue("container_vcores", "1").toInt
    numTotalContainers = cliParser.getOptionValue("num_containers", "1").toInt
    if (numTotalContainers == 0) throw new IllegalArgumentException("Cannot run distributed shell with no containers")
    requestPriority = cliParser.getOptionValue("priority", "0").toInt

    true
  }

  def run(): Unit = {
    val allocListener = new RMCallbackHandler()
    amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener)
    amRMClient.init(conf)
    amRMClient.start()

    containerListener = createNMCallbackHandler()
    nmClientAsync = new NMClientAsyncImpl(containerListener)
    nmClientAsync.init(conf)
    nmClientAsync.start()


    val response = amRMClient.registerApplicationMaster(appMasterHostname, appMasterRpcPort, appMasterTrackingUrl)
    // Dump out information about cluster capability as seen by the
    // resource manager
    val maxMem = response.getMaximumResourceCapability.getMemory
    log.info("Max mem capabililty of resources in this cluster " + maxMem)

    val maxVCores = response.getMaximumResourceCapability.getVirtualCores
    log.info("Max vcores capabililty of resources in this cluster " + maxVCores)

    // A resource ask cannot exceed the max.
    if (containerMemory > maxMem) {
      log.info("Container memory specified above max threshold of cluster." + " Using max value." + ", specified=" + containerMemory + ", max=" + maxMem)
      containerMemory = maxMem
    }

    if (containerVirtualCores > maxVCores) {
      log.info("Container virtual cores specified above max threshold of cluster." + " Using max value." + ", specified=" + containerVirtualCores + ", max=" + maxVCores)
      containerVirtualCores = maxVCores
    }

    val previousAMRunningContainers = response.getContainersFromPreviousAttempts
    log.info(appAttemptID + " received " + previousAMRunningContainers.size + " previous attempts' running containers on AM registration.")
    numAllocatedContainers.addAndGet(previousAMRunningContainers.size)

    val numTotalContainersToRequest = numTotalContainers - previousAMRunningContainers.size
    // Setup ask for containers from RM
    // Send request for containers to RM
    // Until we get our fully allocated quota, we keep on polling RM for
    // containers
    // Keep looping until all the containers are launched and shell script
    // executed on them ( regardless of success/failure).
    for (i <- 0 until numTotalContainersToRequest) {
      val containerAsk = setupContainerAskForRM()
      amRMClient.addContainerRequest(containerAsk)
    }
    numRequestedContainers.set(numTotalContainers)
    /* import sys.process._

     val cmd = shellCommand + " " + shellArgs
     val exitCode = cmd.!
     val res = exitCode == 0
     val appStatus: FinalApplicationStatus = if (res) FinalApplicationStatus.SUCCEEDED else FinalApplicationStatus.FAILED
     amRMClient.unregisterApplicationMaster(appStatus, s"exit_code:$exitCode", null)
     res*/
  }

  def finish(): Boolean = { // wait for completion.
    while (!done && (numCompletedContainers.get != numTotalContainers)) {
      try {
        Thread.sleep(200)
      }
      catch {
        case ex: InterruptedException =>
      }
    }
    // Join all launched threads
    // needed for when we time out
    // and we need to release containers

    for (launchThread <- launchThreads) {
      try {
        launchThread.join(10000)
      }
      catch {
        case e: InterruptedException =>
          log.info("Exception thrown in thread join: " + e.getMessage)
          e.printStackTrace()
      }
    }
    // When the application completes, it should stop all running containers
    log.info("Application completed. Stopping running containers")
    nmClientAsync.stop()
    // When the application completes, it should send a finish application
    // signal to the RM
    log.info("Application completed. Signalling finish to RM")
    var appStatus: FinalApplicationStatus = null
    var appMessage: String = null
    var success = true
    if (numFailedContainers.get == 0 && numCompletedContainers.get == numTotalContainers) {
      appStatus = FinalApplicationStatus.SUCCEEDED
    }
    else {
      appStatus = FinalApplicationStatus.FAILED
      appMessage = "Diagnostics." + ", total=" + numTotalContainers + ", completed=" + numCompletedContainers.get + ", allocated=" + numAllocatedContainers.get + ", failed=" + numFailedContainers.get
      log.info(appMessage)
      success = false
    }
    try amRMClient.unregisterApplicationMaster(appStatus, appMessage, null)
    catch {
      case ex: YarnException =>
        log.error("Failed to unregister application", ex)
      case e: IOException =>
        log.error("Failed to unregister application", e)
    }
    amRMClient.stop()
    // Stop Timeline Client
    success
  }

  def setupContainerAskForRM(): ContainerRequest = { // setup requirements for hosts
    // using * as any host will do for the distributed shell app
    // set the priority for the request
    // TODO - what is the range for priority? how to decide?
    val pri = Priority.newInstance(requestPriority)
    // Set up resource type requirements
    // For now, memory and CPU are supported so we set memory and cpu requirements
    val capability = Resource.newInstance(containerMemory, containerVirtualCores)
    val request = new AMRMClient.ContainerRequest(capability, null, null, pri)
    log.info("Requested container ask: " + request.toString)
    request
  }

  def createNMCallbackHandler(): NMCallBackHandler = {
    new NMCallBackHandler(this)
  }

  private def fileExist(filePath: String): Boolean = {
    new File(filePath).exists
  }

  private def readContent(filePath: String): String = {
    var ds: DataInputStream = null
    try {
      ds = new DataInputStream(new FileInputStream(filePath))
      ds.readUTF
    } finally org.apache.commons.io.IOUtils.closeQuietly(ds)
  }

  private def renameScriptFile(renamedScriptPath: Path): Unit = {
    val fs = renamedScriptPath.getFileSystem(conf)
    fs.rename(new Path(scriptPath), renamedScriptPath)
    log.info("User " + " added suffix(.sh/.bat) to script file as " + renamedScriptPath)
  }

  private class RMCallbackHandler extends AMRMClientAsync.CallbackHandler {

    override def onContainersCompleted(completedContainers: util.List[ContainerStatus]): Unit = {
      log.info("Got response from RM for container ask, completedCnt=" + completedContainers.size)
      import scala.collection.JavaConversions._
      for (containerStatus <- completedContainers) {
        log.info(appAttemptID + " got container status for containerID=" + containerStatus.getContainerId + ", state=" + containerStatus.getState + ", exitStatus=" + containerStatus.getExitStatus + ", diagnostics=" + containerStatus.getDiagnostics)
        // increment counters for completed/failed containers
        val exitStatus = containerStatus.getExitStatus
        if (0 != exitStatus) { // container failed
          if (ContainerExitStatus.ABORTED != exitStatus) { // shell script failed
            // counts as completed
            numCompletedContainers.incrementAndGet
            numFailedContainers.incrementAndGet
          }
          else { // container was killed by framework, possibly preempted
            // we should re-try as the container was lost for some reason
            numAllocatedContainers.decrementAndGet
            numRequestedContainers.decrementAndGet
            // we do not need to release the container as it would be done
            // by the RM
          }
        }
        else { // nothing to do
          // container completed successfully
          numCompletedContainers.incrementAndGet
          log.info("Container completed successfully." + ", containerId=" + containerStatus.getContainerId)
        }
      }
      // ask for more containers if any failed
      val askCount = numTotalContainers - numRequestedContainers.get
      numRequestedContainers.addAndGet(askCount)
      if (askCount > 0) {
        for (i <- 0 until askCount) {
          val containerAsk = setupContainerAskForRM()
          amRMClient.addContainerRequest(containerAsk)
        }
      }
      if (numCompletedContainers.get == numTotalContainers) {
        done = true
      }
    }

    override def onContainersAllocated(allocatedContainers: util.List[Container]): Unit = {
      log.info("Got response from RM for container ask, allocatedCnt=" + allocatedContainers.size)
      numAllocatedContainers.addAndGet(allocatedContainers.size)
      import scala.collection.JavaConversions._
      for (allocatedContainer <- allocatedContainers) {
        log.info("Launching shell command on a new container." + ", containerId=" + allocatedContainer.getId + ", containerNode=" + allocatedContainer.getNodeId.getHost + ":" + allocatedContainer.getNodeId.getPort + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress + ", containerResourceMemory" + allocatedContainer.getResource.getMemory + ", containerResourceVirtualCores" + allocatedContainer.getResource.getVirtualCores)
        // + ", containerToken"
        // +allocatedContainer.getContainerToken().getIdentifier().toString());
        val runnableLaunchContainer = new LaunchContainerRunnable(allocatedContainer, containerListener)
        val launchThread = new Thread(runnableLaunchContainer)
        // launch and start the container on a separate thread to keep
        // the main thread unblocked
        // as all containers may not be allocated at one go.
        launchThreads.add(launchThread)
        launchThread.start()
      }
    }

    override def onShutdownRequest(): Unit = {
      done = true
    }

    override def onNodesUpdated(updatedNodes: util.List[NodeReport]): Unit = {
    }

    override def getProgress: Float = { // set progress to deliver to RM on next heartbeat
      val progress = numCompletedContainers.get.toFloat / numTotalContainers
      progress
    }

    override def onError(e: Throwable): Unit = {
      done = true
      amRMClient.stop()
    }
  }

  private class LaunchContainerRunnable(container: Container, containerListener: AppMaster.NMCallBackHandler) extends Runnable {

    override def run(): Unit = {
      log.info("Setting up container launch container for containerid=" + container.getId)
      // Set the local resources
      val localResources = new util.HashMap[String, LocalResource]
      // The container for the eventual shell commands needs its own local
      // resources too.
      // In this scenario, if a shell script is specified, we need to have it
      // copied and made available to the container.
      if (!scriptPath.isEmpty) {
        var renamedScriptPath: Path = null
        if (Shell.WINDOWS) renamedScriptPath = new Path(scriptPath + ".bat")
        else renamedScriptPath = new Path(scriptPath + ".sh")
        try // rename the script file based on the underlying OS syntax.
          renameScriptFile(renamedScriptPath)
        catch {
          case e: Exception =>
            log.error("Not able to add suffix (.bat/.sh) to the shell script filename", e)
            // We know we cannot continue launching the container
            // so we should release it.
            numCompletedContainers.incrementAndGet
            numFailedContainers.incrementAndGet
            return
        }
        var yarnUrl: URL = null
        try {
          yarnUrl = ConverterUtils.getYarnUrlFromURI(new URI(renamedScriptPath.toString))
        }
        catch {
          case e: URISyntaxException =>
            log.error("Error when trying to use shell script path specified" + " in env, path=" + renamedScriptPath, e)
            // A failure scenario on bad input such as invalid shell script path
            // TODO
            numCompletedContainers.incrementAndGet
            numFailedContainers.incrementAndGet
            return
        }
        val shellRsrc = LocalResource.newInstance(yarnUrl, LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, shellScriptPathLen, shellScriptPathTimestamp)
        localResources.put(if (Shell.WINDOWS) ExecBatScripStringtPath else ExecShellStringPath, shellRsrc)
        shellCommand = if (Shell.WINDOWS) windows_command else linux_bash_command
      }

      // Set the necessary command to execute on the allocated container
      val vargs = new util.Vector[CharSequence](5)
      // Set executable command
      vargs.add(shellCommand)
      // Set shell script path
      if (!scriptPath.isEmpty) {
        vargs.add(if (Shell.WINDOWS) ExecBatScripStringtPath else ExecShellStringPath)
      }
      // Set args for the shell command if any
      vargs.add(shellArgs)
      // Add log redirect params
      vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout")
      vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")
      // Get final commmand
      val command = new StringBuilder
      import scala.collection.JavaConversions._
      for (str <- vargs) {
        command.append(str).append(" ")
      }
      val commands = new util.ArrayList[String]
      commands.add(command.toString)
      // Set up ContainerLaunchContext, setting local resource, environment,
      // command and token for constructor.
      // Note for tokens: Set up tokens for the container too. Today, for normal
      // shell commands, the container in distribute-shell doesn't need any
      // tokens. We are populating them mainly for NodeManagers to be able to
      // download anyfiles in the distributed file-system. The tokens are
      // otherwise also useful in cases, for e.g., when one is running a
      // "hadoop dfs" command inside the distributed shell.
      val ctx = ContainerLaunchContext.newInstance(localResources, shellEnv, commands, null, null, null)
      containerListener.addContainer(container.getId, container)
      nmClientAsync.startContainerAsync(container, ctx)
    }
  }
}


object AppMaster {
  private val log = LoggerFactory.getLogger(this.getClass)
  private val shellCommandPath = "shellCommands"
  private val shellArgsPath = "shellArgs"
  private val appMasterJarPath = "AppMaster.jar"
  // Hardcoded path to custom log_properties
  private val log4jPath = "log4j.properties"
  val SCRIPT_PATH = "ExecScript"

  val ExecShellStringPath = Client.SCRIPT_PATH + ".sh"
  val ExecBatScripStringtPath = Client.SCRIPT_PATH + ".bat"
  val linux_bash_command = "bash"
  val windows_command = "cmd /c"

  def main(args: Array[String]): Unit = {
    var res = false
    try {
      val am = new AppMaster()
      log.info("initializing application master")
      if (!am.init(args)) {
        System.exit(0)
      }
      am.run()
      res = am.finish()
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

  class NMCallBackHandler extends NMClientAsync.CallbackHandler {

    private val containers: ConcurrentMap[ContainerId, Container] = new ConcurrentHashMap[ContainerId, Container]
    private var applicationMaster: AppMaster = _

    def this(applicationMaster: AppMaster) {
      this()
      this.applicationMaster = applicationMaster
    }

    def addContainer(containerId: ContainerId, container: Container): Unit = {
      containers.putIfAbsent(containerId, container)
    }

    override def onContainerStopped(containerId: ContainerId): Unit = {
      log.debug("Succeeded to stop Container " + containerId)
      containers.remove(containerId)
    }

    override def onContainerStatusReceived(containerId: ContainerId, containerStatus: ContainerStatus): Unit = {
      log.debug("Container Status: id=" + containerId + ", status=" + containerStatus)
    }

    override def onContainerStarted(containerId: ContainerId, allServiceResponse: util.Map[String, ByteBuffer]): Unit = {
      log.debug("Succeeded to start Container " + containerId)
      val container = containers.get(containerId)
      if (container != null) {
        applicationMaster.nmClientAsync.getContainerStatusAsync(containerId, container.getNodeId)
      }
    }

    override def onStartContainerError(containerId: ContainerId, t: Throwable): Unit = {
      log.error("Failed to start Container " + containerId)
      containers.remove(containerId)
      applicationMaster.numCompletedContainers.incrementAndGet
      applicationMaster.numFailedContainers.incrementAndGet
    }

    override def onGetContainerStatusError(containerId: ContainerId, t: Throwable): Unit = {
      log.error("Failed to query the status of Container " + containerId)
    }

    override def onStopContainerError(containerId: ContainerId, t: Throwable): Unit = {
      log.error("Failed to stop Container " + containerId)
      containers.remove(containerId)
    }
  }

}