package jedisl.monitor;

import io.prometheus.client.Summary;
import redis.clients.jedis.*;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;

/**
 * Author: shaoff
 * Date: 2020/3/18 17:39
 * Package: jedisl.monitor
 * Description:
 */
public interface MyJedis extends JedisCommands, MultiKeyCommands,
        AdvancedJedisCommands, ScriptingCommands, BasicCommands, ClusterCommands, SentinelCommands {

    /*请求处理延迟的分位数监控*/
    Summary process_delay = Summary.build().labelNames("app","method")
            .name("op_duration").
                    quantile(0.1, 0.01).
                    quantile(0.3, 0.01).
                    quantile(0.5, 0.01).
                    quantile(0.9, 0.01).
                    quantile(0.95, 0.01).help("Request duration in seconds.").register();

    class SimpleInvocationHandler implements InvocationHandler {
        private Object realObj;
        private String appName;

        public SimpleInvocationHandler(Object realObj,String appName) {
            this.realObj = realObj;
            this.appName=appName;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            Summary.Timer st = process_delay.labels(appName,method.getName()).startTimer();
//            System.out.println("entering " + method.getName());
            Object result = method.invoke(realObj, args);
            st.observeDuration();
//            System.out.println("leaving " + method.getName());
            return result;
        }
    }

    static MyJedis fromJedis(Jedis jedis,Map<String,String> params) {
        MyJedis myJedis = (MyJedis) Proxy.newProxyInstance(MyJedis.class.getClassLoader(), new Class<?>[]{MyJedis.class},
                new SimpleInvocationHandler(jedis,params.get("spark.app.name")));
        return myJedis;
    }
}

