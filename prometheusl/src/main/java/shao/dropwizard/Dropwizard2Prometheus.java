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
import io.prometheus.client.dropwizard.samplebuilder.CustomMappingSampleBuilder;
import io.prometheus.client.dropwizard.samplebuilder.MapperConfig;
import io.prometheus.client.dropwizard.samplebuilder.SampleBuilder;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.exporter.PushGateway;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Dropwizard2Prometheus {
    static final MetricRegistry metrics = new MetricRegistry();
    static Collector collector;

    static {
        /*处理dropwizard metricName变为prometheus labels*/
        MapperConfig config = new MapperConfig();
// The match field in MapperConfig is a simplified glob expression that only allows * wildcard.
        config.setMatch("*.*.jedis.*");
        config.setName("streaming-jedis");
        Map<String, String> labels = new HashMap<String, String>();
// ... more configs
// Labels to be extracted from the metric. Key=label name. Value=label template
        labels.put("app", "${0}");
        labels.put("executor", "${1}");
        labels.put("method", "${2}");
        config.setLabels(labels);

        SampleBuilder sampleBuilder = new CustomMappingSampleBuilder(Arrays.asList(config));
        collector = new DropwizardExports(metrics, sampleBuilder);
        collector.register();
    }

    static AtomicInteger id = new AtomicInteger();

    public static void main(String args[]) {
//        startReport();
        Gauge requests = metrics.register("JedisTZ.driver.jedis.hget", new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return id.incrementAndGet();
            }
        });
        while (true) {
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
            pg.pushAdd(collector, "Drop2Prometheus");
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
