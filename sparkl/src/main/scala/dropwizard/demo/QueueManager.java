package dropwizard.demo;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

/**
 * Author: shaoff
 * Date: 2020/3/19 16:12
 * Package: dropwizard.demo
 * Description:
 */
public class QueueManager {
    static final MetricRegistry metrics = new MetricRegistry();

    final Queue queue;

    public QueueManager(String name) {
        this.queue =new LinkedList();
        metrics.register(MetricRegistry.name(QueueManager.class, name, "size"),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return queue.size();
                    }
                });
    }

    static void startReport() {
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(1, TimeUnit.SECONDS);
    }

    static void waitSeconds(int sec) {
        try {
            Thread.sleep(sec*1000);
        }
        catch(InterruptedException e) {}
    }

    public static void main(String[] args) {
        startReport();
        QueueManager qm=new QueueManager("queue1");
        qm.queue.add("abc");
        waitSeconds(3);
        qm.queue.add("def");
        waitSeconds(3);
    }
}
