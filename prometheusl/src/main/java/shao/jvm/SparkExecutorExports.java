package shao.jvm;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.hotspot.*;

/**
 * Author: shaoff
 * Date: 2020/4/29 12:53
 * Package: shao.jvm
 * Description:
 */

/**
 * Registers the default Hotspot collectors.
 * <p>
 * This is intended to avoid users having to add in new
 * registrations every time a new exporter is added.
 * <p>
 * Example usage:
 * <pre>
 * {@code
 *   DefaultExports.initialize();
 * }
 * </pre>
 */
public class SparkExecutorExports {
    private static boolean initialized = false;

    /**
     * Register the default Hotspot collectors with the default
     * registry. It is safe to call this method multiple times, as
     * this will only register the collectors once.
     */
    public static synchronized void initialize() {
        if (!initialized) {
            register(CollectorRegistry.defaultRegistry);
            initialized = true;
        }
    }

    /**
     * Register the default Hotspot collectors with the given registry.
     */
    public static void register(CollectorRegistry registry) {
        new StandardExports().register(registry);
        new MemoryPoolsExports().register(registry);
        new MemoryAllocationExports().register(registry);
        new BufferPoolsExports().register(registry);
//        new GarbageCollectorExports().register(registry);
        new ThreadExports().register(registry);
        new ClassLoadingExports().register(registry);
//        new VersionInfoExports().register(registry);
    }

}
