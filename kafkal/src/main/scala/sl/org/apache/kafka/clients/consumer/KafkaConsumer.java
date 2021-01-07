package sl.org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import sl.org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.*;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Author: shaoff
 * Date: 2020/7/2 11:43
 * Package: sl.org.apache.kafka.clients.consumer
 * Description:
 * 模拟Consumer
 */
public class KafkaConsumer<K, V> implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(org.apache.kafka.clients.consumer.KafkaConsumer.class);
    private static final long NO_Thread_ID = -1;
    private final AtomicLong currentThreadId = new AtomicLong(NO_Thread_ID);
    private final AtomicInteger refCount = new AtomicInteger(0);
    private final SubscriptionState subscriptions;
    private final ConsumerCoordinator coordinator;
    private final ConsumerNetworkClient client;
    private final int requestTimeoutMs;
    private final Time time;
    private final Metrics metrics;
    private final Metadata metadata;
    private final long retryBackoffMs;
    private final ConsumerInterceptors<K, V> interceptors;
    private final Fetcher<K, V> fetcher;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;


    private static final String JMX_PREFIX = "kafka.consumer";

    public KafkaConsumer(Properties properties) {
        this(properties, null, null);
    }

    public KafkaConsumer(Properties properties,
                         Deserializer<K> keyDeserializer,
                         Deserializer<V> valueDeserializer) {

        this(new ConsumerConfig(ConsumerConfig.addDeserializerToConfig(properties, keyDeserializer, valueDeserializer)),
                keyDeserializer,
                valueDeserializer);
    }

    private KafkaConsumer(ConsumerConfig config,
                         Deserializer<K> keyDeserializer,
                         Deserializer<V> valueDeserializer) {
        try {
            log.debug("Starting the Kafka consumer");
            this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
            int sessionTimeOutMs = config.getInt(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG);
            int fetchMaxWaitMs = config.getInt(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG);
            if (this.requestTimeoutMs <= sessionTimeOutMs || this.requestTimeoutMs <= fetchMaxWaitMs)
                throw new ConfigException(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG + " should be greater than " + ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG + " and " + ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG);
            this.time = new SystemTime();

            //metrics
            String clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);
            Map<String, String> metricsTags = new LinkedHashMap<>();
            metricsTags.put("client-id", clientId);
            MetricConfig metricConfig = new MetricConfig().samples(config.getInt(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG))
                    .timeWindow(config.getLong(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                    .tags(metricsTags);
            List<MetricsReporter> reporters = config.getConfiguredInstances(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                    MetricsReporter.class);
            reporters.add(new JmxReporter(JMX_PREFIX));
            this.metrics = new Metrics(metricConfig, reporters, time);

            //meta
            this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
            this.metadata = new Metadata(retryBackoffMs, config.getLong(ConsumerConfig.METADATA_MAX_AGE_CONFIG));
            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
            this.metadata.update(Cluster.bootstrap(addresses), 0);

            //network
            String metricGrpPrefix = "consumer";
            ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config.values());
            NetworkClient netClient = new NetworkClient(
                    new Selector(config.getLong(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG), metrics, time, metricGrpPrefix, channelBuilder),
                    this.metadata,
                    clientId,
                    100, // a fixed large enough value will suffice
                    config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG),
                    config.getInt(ConsumerConfig.SEND_BUFFER_CONFIG),
                    config.getInt(ConsumerConfig.RECEIVE_BUFFER_CONFIG),
                    config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG), time);
            this.client = new ConsumerNetworkClient(netClient, metadata, time, retryBackoffMs,
                    config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG));

//            this.interceptors = interceptorList.isEmpty() ? null : new ConsumerInterceptors<>(interceptorList);
            List<PartitionAssignor> assignors = config.getConfiguredInstances(
                    ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                    PartitionAssignor.class);
            this.interceptors = null;

            //coordinator
            OffsetResetStrategy offsetResetStrategy = OffsetResetStrategy.LATEST;
//                valueOf(config.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).toUpperCase(Locale.ROOT));
            this.subscriptions = new SubscriptionState(offsetResetStrategy);
            this.coordinator = new ConsumerCoordinator(this.client,
                    config.getString(ConsumerConfig.GROUP_ID_CONFIG),
                    config.getInt(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG),
                    config.getInt(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG),
                    assignors,
                    this.metadata,
                    this.subscriptions,
                    metrics,
                    metricGrpPrefix,
                    this.time,
                    retryBackoffMs,
                    new ConsumerCoordinator.DefaultOffsetCommitCallback(),
                    config.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG),
                    config.getLong(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG),
                    this.interceptors,
                    config.getBoolean(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG));

            //fetcher
            if (keyDeserializer == null) {
                this.keyDeserializer = config.getConfiguredInstance(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        Deserializer.class);
                this.keyDeserializer.configure(config.originals(), true);
            } else {
                config.ignore(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
                this.keyDeserializer = keyDeserializer;
            }
            if (valueDeserializer == null) {
                this.valueDeserializer = config.getConfiguredInstance(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        Deserializer.class);
                this.valueDeserializer.configure(config.originals(), false);
            } else {
                config.ignore(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
                this.valueDeserializer = valueDeserializer;
            }
            this.fetcher = new Fetcher<>(this.client,
                    config.getInt(org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MIN_BYTES_CONFIG),
                    config.getInt(org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG),
                    config.getInt(org.apache.kafka.clients.consumer.ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG),
                    config.getInt(org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG),
                    config.getBoolean(org.apache.kafka.clients.consumer.ConsumerConfig.CHECK_CRCS_CONFIG),
                    this.keyDeserializer,
                    this.valueDeserializer,
                    this.metadata,
                    this.subscriptions,
                    metrics,
                    metricGrpPrefix,
                    this.time,
                    this.retryBackoffMs);

        } finally {

        }

    }

    @Override
    public void close() throws IOException {

    }

    public void acquire() {
        long tid = Thread.currentThread().getId();
        if (currentThreadId.get() != tid && !currentThreadId.compareAndSet(NO_Thread_ID, tid)) {
            throw new ConcurrentModificationException("not support");
        }
        refCount.incrementAndGet();
    }

    public void release() {
        //支持可重入
        long tid = Thread.currentThread().getId();
        assert tid == currentThreadId.get();
        if (refCount.decrementAndGet() == 0) {
            currentThreadId.set(NO_Thread_ID);
        }
    }

    public ConsumerRecords<K, V> poll(long timeout) throws Exception {
        acquire();
        try {
            assert timeout > 0;
            long remaining = timeout;
            do {
                long startMs = System.currentTimeMillis();
                //get once
                pollOnce(remaining);
                remaining -= System.currentTimeMillis() - startMs;
            } while (remaining > 0);
            return ConsumerRecords.empty();
        } finally {
            release();
        }
    }

    private Map<TopicPartition, List<ConsumerRecord<K, V>>> pollOnce(long timeout) {
        if (subscriptions.partitionsAutoAssigned()) {
            coordinator.ensureCoordinatorReady();
        }
        if (!subscriptions.hasAllFetchPositions()) {
            updateFetchPositions(this.subscriptions.missingFetchPositions());
        }
        long now = time.milliseconds();
        // execute delayed tasks (e.g. autocommits and heartbeats) prior to fetching records
        client.executeDelayedTasks(now);
        //先尝试返回已经fetched数据
        Map<TopicPartition, List<ConsumerRecord<K, V>>> records = fetcher.fetchedRecords();
        if (!records.isEmpty()) {
            return records;
        }
        fetcher.sendFetches();
        client.poll(timeout, now);
        return fetcher.fetchedRecords();
    }

    private void updateFetchPositions(Set<TopicPartition> partitions) {
        // refresh commits for all assigned partitions
        coordinator.refreshCommittedOffsetsIfNeeded();
        // then do any offset lookups in case some positions are not known
//        fetcher.updateFetchPositions(partitions);
    }
}
