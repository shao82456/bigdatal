package shao.dropwizard;

/**
 * Author: shaoff
 * Date: 2020/3/19 16:06
 * Package: dropwizard.demo
 * Description:
 */

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import io.prometheus.client.Collector;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.exporter.PushGateway;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class GetStarted {
    static final MetricRegistry metrics = new MetricRegistry();
    static Collector collector = new DropwizardExports(metrics);

    static {
        collector.register();
    }

    static AtomicInteger id = new AtomicInteger();

    public static void main(String args[]) {
//        startReport();
        Gauge requests = metrics.register("currentId", new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return id.incrementAndGet();
            }
        });
        while (true){
            startPushGateWay();
            wait5Seconds();
        }


//        metrics.meter("requests").mark();
    }

    static void startReport() {
        try {
            HTTPServer server = new HTTPServer(10001);
        } catch (IOException e) {
            e.printStackTrace();
        }
        /*ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(1, TimeUnit.SECONDS);*/
    }

    static void startPushGateWay() {
        PushGateway pg = new PushGateway("localhost:9091");
        try {
            pg.pushAdd(collector,"testPG");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void wait5Seconds() {
        try {
            Thread.sleep(5 * 1000);
        } catch (InterruptedException e) {
        }
    }
}
