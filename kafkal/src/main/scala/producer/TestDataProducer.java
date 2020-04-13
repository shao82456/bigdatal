package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;
import java.util.Scanner;
import java.util.UUID;


//Create java class named “SimpleProducer”
public class TestDataProducer {
    private static Logger log = LoggerFactory.getLogger(TestDataProducer.class);
    private static String topic = "test";
    private static String bootstrapServer = "localhost:9092";
    private static int batchSize = 10;
    private static int batchInterval = 3;

    public static void main(String[] args) throws Exception {
        if (args.length >= 2) {
            topic = args[0];
            bootstrapServer = args[1];
        }
        if (args.length >= 4) {
            batchSize = Integer.parseInt(args[2]);
            batchInterval = Integer.parseInt(args[3]);
        }
        InputStream data = Thread.currentThread().getContextClassLoader().getResourceAsStream("data");
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Scanner scanner = new Scanner(data);
        String line = scanner.nextLine();
        while (true) {
            for (int i = 0; i < batchSize; i++) {
                producer.send(new ProducerRecord<>(topic,
                        UUID.randomUUID().toString(), line), (metadata, exception) -> {
                    if (exception != null) {
                        log.warn(exception.getMessage());
                    }
                });
            }
            log.info("sent batch of {} records", batchSize);
            Thread.sleep(1000 * batchInterval);
        }
    }
}
