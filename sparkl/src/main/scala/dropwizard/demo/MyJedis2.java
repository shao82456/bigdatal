package dropwizard.demo;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import jedisl.monitor.MyJedis;
import redis.clients.jedis.*;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Author: shaoff
 * Date: 2020/3/18 17:39
 * Package: jedisl.monitor
 * Description:
 */
public interface MyJedis2 extends JedisCommands, MultiKeyCommands,
        AdvancedJedisCommands, ScriptingCommands, BasicCommands, ClusterCommands, SentinelCommands {

    MetricRegistry metrics = new MetricRegistry();
    ConcurrentHashMap<String, Histogram> histograms = new ConcurrentHashMap<>();

    class SimpleInvocationHandler implements InvocationHandler {
        private Object realObj;

        public SimpleInvocationHandler(Object realObj) {
            this.realObj = realObj;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            long st = System.currentTimeMillis();
            Object result = method.invoke(realObj, args);
            String methodName = method.getName();
            Histogram histogram = histograms.getOrDefault(methodName, metrics.histogram(MetricRegistry.name("jedis",methodName )));
            histogram.update(System.currentTimeMillis() - st);
            histograms.put(methodName, histogram);
            return result;
        }
    }

}

