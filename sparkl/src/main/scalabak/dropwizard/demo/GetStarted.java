package dropwizard.demo;

/**
 * Author: shaoff
 * Date: 2020/3/19 16:06
 * Package: dropwizard.demo
 * Description:
 */
import com.codahale.metrics.*;
import java.util.concurrent.TimeUnit;

public class GetStarted {
    static final MetricRegistry metrics = new MetricRegistry();
    static{
        metrics.register(MetricRegistry.name(GetStarted.class, "cache-evictions"), new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return 3;
            }
        });
    }
    public static void main(String args[]) {
        startReport();
        Timer requests = metrics.timer("request");
//       metrics.meter("requests").mark();
        wait5Seconds();
    }

    static void startReport() {
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(1, TimeUnit.SECONDS);
    }

    static void wait5Seconds() {
        try {
            Thread.sleep(5*1000);
        }
        catch(InterruptedException e) {}
    }
}
