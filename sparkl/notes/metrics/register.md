## spark的metric体系使用记录

### 工作原理
spark的metric体系面向worker,master,driver,executor,application等实例
这里以driver,executor为主，spark程序中，driver/executor进程中都会构建一个SparkEnv对象，其中含有一个MetricSystem对象，这个对象有个根MetricRegistry对象，当driver/executor中的sparkEnv的这个对象创建后，会开始从配置中加载driver/exuector instance下的所有source和sink,其中会把每个source自定义的MetricRegistry注册到根MetricRegistry中，MetricRegistry实际就是个Set<Metric>,这一步类似addAll操作，然后把每个sink注册到根MetricRegistry中。整体过程就是这样，这里需要注意的是，自定义的Source中的指标，需要在被注册到根MetricRegistry中先注册到自己的MetricRegistry中,以下就是我犯的错误
```java
object JedisMetrics extends Source {
  override val sourceName: String = "jedis"
  override val metricRegistry: MetricRegistry = new MetricRegistry()
}

public class JedisInvocationHandler implements InvocationHandler {
    private Object realObj;

    public JedisInvocationHandler(Object realObj) {
        this.realObj = realObj;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        long st = System.currentTimeMillis();
        Object result = method.invoke(realObj, args);
        String methodName = method.getName();
        Histogram histogram = JedisMetrics.metricRegistry().histogram(MetricRegistry.name(methodName));
        histogram.update(System.currentTimeMillis() - st);
        return result;
    }
}
```
上面这段代码就是为了监控Jedis操作的时长，其中histogram的创建是在对应方法被调用后产生的，一定会晚于向根MetricRegistry注册,因此最后指标无法被sink采集到。

可以用以下代码验证
```java
public class GetStarted {
    static final MetricRegistry metrics = new MetricRegistry();
    static final MetricRegistry rootMetrics = new MetricRegistry();
    static {
        rootMetrics.register("name2",metrics);
    }
    public static void main(String args[]) {
        startReport();
        Meter requests = metrics.meter(MetricRegistry.name("request"));

//       metrics.meter("requests").mark();
        wait5Seconds();
    }

    static void startReport() {
        ConsoleReporter reporter = ConsoleReporter.forRegistry(rootMetrics)
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
```

### 问题记录
1.spark自定义的source需要先注册
