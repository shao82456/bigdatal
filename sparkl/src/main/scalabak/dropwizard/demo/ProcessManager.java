package dropwizard.demo;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

/**
 * Author: shaoff
 * Date: 2020/3/19 16:12
 * Package: dropwizard.demo
 * Description:
 */

class Request{

}
class RequestHandler{
    private Timer timer;

    public RequestHandler(Timer timer) {
        this.timer = timer;
    }

    public void handle(Request req){
        final Timer.Context context = timer.time();
        try {
            ProcessManager.waitSeconds(1);
        } finally {
            context.stop();
        }
    }
}
public class ProcessManager {
    static final MetricRegistry metrics = new MetricRegistry();
    final Timer timer;
    final RequestHandler handler;
    public ProcessManager(String name) {
       timer=metrics.timer(MetricRegistry.name(RequestHandler.class, name));
       this.handler=new RequestHandler(timer);
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
        ProcessManager qm=new ProcessManager("duration");
        qm.handler.handle(new Request());
        qm.handler.handle(new Request());
        waitSeconds(3);
        qm.handler.handle(new Request());
        qm.handler.handle(new Request());
        waitSeconds(3);
    }
}
