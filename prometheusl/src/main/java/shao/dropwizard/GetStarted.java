package shao.dropwizard;

/**
 * Author: shaoff
 * Date: 2020/3/19 16:06
 * Package: dropwizard.demo
 * Description:
 */

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import io.prometheus.client.Collector;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.exporter.PushGateway;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class GetStarted {
    static final MetricRegistry metrics = new MetricRegistry();
    static Collector collector = new DropwizardExports(metrics);
    static PushGateway pg = new PushGateway("localhost:9091");

    static {
        collector.register();
    }

    static AtomicInteger id = new AtomicInteger();

    public static void main(String[] args) throws InterruptedException {
        Histogram h = metrics.histogram("abc");
        ExecutorService es = Executors.newCachedThreadPool();
        es.execute(() -> {
            startReport();
            while (true) {
//                push();
                h.update(System.currentTimeMillis()%100);
                wait5Seconds();
            }
        });

        Gauge<Integer> requests = metrics.register("currentId", new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return id.incrementAndGet();
            }
        });


        es.awaitTermination(1, TimeUnit.MINUTES);
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

    static void push() {
        try {
            pg.pushAdd(collector, "testPG");
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
