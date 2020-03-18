package jedisl.monitor;

import redis.clients.jedis.*;

/**
 * Author: shaoff
 * Date: 2020/3/18 17:39
 * Package: jedisl.monitor
 * Description:
 */
public interface MyJedis extends JedisCommands, MultiKeyCommands,
        AdvancedJedisCommands, ScriptingCommands, BasicCommands, ClusterCommands, SentinelCommands  {

//    final static
}
