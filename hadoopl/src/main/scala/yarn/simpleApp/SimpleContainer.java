//package yarn.simpleApp;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.yarn.api.records.LocalResource;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.Map;
//
///**
// * Author: shaoff
// * Date: 2020/5/27 15:53
// * Package: yarn.simpleApp
// * Description:
// */
//public class SimpleContainer {
//    private static Logger log= LoggerFactory.getLogger(SimpleContainer.class);
//
//
//        public static Map<String, LocalResource> prepareLocalResources() throws Exception {
//        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
//
//        log.info("Copy App Master jar from local filesystem and add to local environment");
//        // Copy the application master jar to the filesystem
//        // Create a local resource to point to the destination jar path
//        FileSystem fs = FileSystem.get(new Configuration());
////        addToLocalResources(fs, appMasterJar, appMasterJarPath, appId.toString(),
//                localResources, null);
//
//        // Set the log4j properties if needed
//        /*if (!log4jPropFile.isEmpty()) {
//            addToLocalResources(fs, log4jPropFile, log4jPath, appId.toString(),
//                    localResources, null);
//        }*/
//    }
//
//}
