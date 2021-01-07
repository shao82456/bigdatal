package yarn.simpleApp;

import com.sun.org.apache.xalan.internal.xsltc.dom.SAXImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Author: shaoff
 * Date: 2020/5/27 15:29
 * Package: PACKAGE_NAME
 * Description:
 */
public class SimpleClient {
    static Logger log= LoggerFactory.getLogger(SimpleClient.class);
    static String appName="test_yarn_shaoff";
    static String queue="default";
    static boolean keepContainers=true;
    static String type="YARN_TEST";

    static String srcResource="";
    static String dstResource="data.txt";
    static Map<String,String> myEnv=new HashMap<>();
    static {
        myEnv.put("HELLO","WORLD");
    }

    public static void main(String[] args) throws Exception {
        /*process submit args*/
        srcResource=args[0];
        log.info("============submit args process done===========");
        Configuration conf=new Configuration();
        FileSystem fs=FileSystem.get(conf);

        /*initialize yarn client */
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
        log.info("============init yarn client done===============");
        /*request app_id*/
        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        log.info("============get newAppResponse done===============");
        /*set submission context*/
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();
        appContext.setApplicationName(appName);
        appContext.setQueue(queue);
        appContext.setKeepContainersAcrossApplicationAttempts(keepContainers);
        appContext.setApplicationType(type);
        log.info("============set submission context done===============");
        /*prepare am container */

        /*prepare local resources*/
        Map<String, LocalResource> localResources = new HashMap<>();
        String suffix = appName + "/" + appId + "/" + dstResource;
        Path dst=new Path(fs.getHomeDirectory(),suffix);
        fs.copyFromLocalFile(new Path(srcResource),dst);
        FileStatus scFileStatus = fs.getFileStatus(dst);
        localResources.put(dstResource,
                LocalResource.newInstance(ConverterUtils.getYarnUrlFromPath(dst),
                        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                        scFileStatus.getLen(),scFileStatus.getModificationTime()));

        /*prepare env from am*/
        Map<String, String> env = myEnv;

        /*prepare executable cmd*/
    }

}
