package shao.jvm;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.exporter.PushGateway;
import io.prometheus.client.hotspot.DefaultExports;
import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Enumeration;

/**
 * Author: shaoff
 * Date: 2020/4/29 12:25
 * Package: shao.jvm
 * Description:
 */
public class Demo {
    public static void main(String[] args) throws Exception {
//        SparkExecutorExports.initialize();
//        Field f = Unsafe.class.getDeclaredField("theUnsafe");
//        f.setAccessible(true);
//        Unsafe us = (Unsafe) f.get(null);
//        us.allocateMemory(1024*100);
//        startReport();

        Gauge  g= Gauge.build().name("state").help("test label").labelNames("tag").create();
        Gauge b= Gauge.build().name("current_time").help("Current unixtime.").labelNames("area") .create()
                .setChild(g.labels("t1"),"t1_v2");

        g.register();
        b.register();


        int i=0;
        startReport();
        while (true){
            g.labels("t1").set(i++);
            Thread.sleep(1000);
            if(i%3==0){
                System.out.println("once");
//                startReport();
                Enumeration<Collector.MetricFamilySamples> it = CollectorRegistry.defaultRegistry.metricFamilySamples();
                while(it.hasMoreElements()){
                    Collector.MetricFamilySamples mfs = it.nextElement();
                    System.out.println(mfs.name);
                    System.out.println(mfs.samples);
                }
                 CollectorRegistry.defaultRegistry.metricFamilySamples();
            }

        }
//        Thread.sleep(1000*60*3);
    }
    static void startReport() throws Exception {
        /*try {
            HTTPServer server = new HTTPServer(10001);
        } catch (IOException e) {
            e.printStackTrace();
        }*/
        PushGateway pg=new PushGateway("localhost:9091");
        pg.pushAdd(CollectorRegistry.defaultRegistry,"abc");
        /*ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(1, TimeUnit.SECONDS);*/
    }
}
