package shao;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.Summary;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 演示Prometheus-java_client的用法
 * 四种数据类型
 * Counter 只能递增或重置的整数值，可以用来记录pv等指标
 * Gauge 可以保存一个浮点值，能够自由设置，还提供了其他便捷API
 * Histogram（直方图，记录总数，总和，落到桶内的观测制范围，默认有生成桶，还提供了API生成指数分布桶，线性分布的桶
 * Summary 和Histogram类似，不过记录的是各个分位点的观测值
 * <p>
 * 问题：
 * 1.这些对象是否线程安全
 */
public class DemoServer {
    /*初始化metrics*/
    /*累计请求监控*/
    static final Counter total_requests = Counter.build()
            .name("requests_total").help("Total requests.").register();
    /*处理中请求监控*/
    static final Gauge inprogressRequests = Gauge.build()
            .name("inprogress_requests").help("Inprogress requests.").register();
    /*请求的处理时长分布监控*/
    static final Histogram process_duration = Histogram.build()
            .name("process_duration").exponentialBuckets(0.5, 2, 10).help("Request process duration in seconds.").register();
    /*请求处理延迟的分位数监控*/
    static final Summary process_delay = Summary.build()
            .name("process_delay").
                    quantile(0.1, 0.01).
                    quantile(0.3, 0.01).
                    quantile(0.5, 0.01).
                    quantile(0.7, 0.01).
                    quantile(0.9, 0.01).
                    quantile(0.95, 0.01).help("Request delay in seconds.").register();

    //模拟请求队列
    LinkedBlockingDeque<Request> requests = new LinkedBlockingDeque<>();
    //worker
    ExecutorService es;

    /**
     * 模拟请求接口
     */
    void request(Request q) {
        for (CallBack cb : q.listeners) {
            cb.onReceive();
        }
        requests.add(q);
    }

    /*模拟请求处理*/
    Runnable task = new Runnable() {
        @Override
        public void run() {
            while (!Thread.interrupted()) {
                Request req = null;
                try {
                    req = requests.take();
                    //请求开始处理
                    System.out.println("server begin process request " + req.toSleepSeconds);
                    for (CallBack cp : req.listeners) {
                        cp.onBegin();
                    }
                    Thread.sleep((long) (1000 * req.toSleepSeconds));
                    //请求处理结束
                    for (CallBack cp : req.listeners) {
                        cp.onFinish();
                    }
                    System.out.println("server finish process request " + total_requests.get());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    };

    //让prometheus通过pull拉数据
    void monitorStart(){
        DefaultExports.initialize();
        try {
            HTTPServer server = new HTTPServer(10001);
//            InetSocketAddress address = new InetSocketAddress("192.168.1.222", 9201);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    void start() {
        this.es = Executors.newFixedThreadPool(2);
        monitorStart();
        es.execute(task);
        es.execute(task);
    }

    void close() {
        es.shutdown();
    }

    public static void main(String[] args) {
        /*启动服务器*/
        DemoServer server = new DemoServer();
        server.start();
        /*模拟两个客户端*/
        Random random = new Random();
        Runnable task = () -> {
            while (!Thread.interrupted()) {
                double requestSec = random.nextDouble() * 5;
                System.out.println(Thread.currentThread().getName() + " sent request " + requestSec);
                Request q=new Request(requestSec);
                //增加监控回调
                q.addCallBack(new MonitorCallBack(total_requests,inprogressRequests,process_duration,process_delay));
                server.request(q);
                int sleepSec = random.nextInt(5);
                try {
                    Thread.sleep(1000 * sleepSec);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

        };
        new Thread(task, "client1").start();
        new Thread(task, "client2").start();
    }
}

interface CallBack {
    void onBegin();

    void onFinish();

    void onReceive();
}

class MonitorCallBack implements CallBack {
    Counter total;
    Gauge inProcess;
    Histogram duration;
    Summary delay;

    Summary.Timer delayTimer;
    Histogram.Timer durationTimer;

    public MonitorCallBack(Counter total, Gauge inProcess, Histogram duration, Summary delay) {
        this.total = total;
        this.inProcess = inProcess;
        this.duration = duration;
        this.delay = delay;
    }

    @Override
    public void onReceive() {
        /*增加累计请求数，初始化处理延迟计时器*/
        total.inc();
        this.delayTimer = delay.startTimer();
    }

    @Override
    public void onBegin() {
        /*增加处理中请求数，记录处理延迟，初始化处理时长计时器，*/
        inProcess.inc();
        delayTimer.observeDuration();
        durationTimer = duration.startTimer();
    }

    @Override
    public void onFinish() {
        /*记录处理时长，减少处理中请求数*/
        durationTimer.observeDuration();
        inProcess.dec();
    }
}

class Request {
    static AtomicInteger counter = new AtomicInteger();
    //请求需要处理的时间
    double toSleepSeconds;
    List<CallBack> listeners = new ArrayList<>();

    public Request(double toSleepSeconds) {
        System.out.println("total sent " + counter.incrementAndGet());
        this.toSleepSeconds = toSleepSeconds;
    }

    public void addCallBack(CallBack cb) {
        listeners.add(cb);
    }
}