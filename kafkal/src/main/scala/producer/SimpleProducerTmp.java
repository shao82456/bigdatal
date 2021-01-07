package producer;//import util.properties packages

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

//import simple producer packages
//import KafkaProducer packages
//import ProducerRecord packages

//Create java class named “SimpleProducer”
public class SimpleProducerTmp {
    private static Logger log = LoggerFactory.getLogger(SimpleProducerTmp.class);
    private static String data = "/Users/sakura/stuff/bigdatal/kafkal/src/main/resources/data";
    private static String topic = "test";
    private static String bootstrapServer = "localhost:9092";

    public static void main(String[] args) throws Exception {
        if (args.length > 0) {
            data = args[0];
            topic = args[1];
            bootstrapServer = args[2];
        }
        //Assign topicName to string variable
        // create instance for properties to access producer configs
        Properties props = new Properties();
        //Assign localhost id
        props.put("bootstrap.servers", bootstrapServer);
        //Set acknowledgements for producer requests.
//        props.put("acks", "all");
        //If the request fails, the producer can automatically retry,
//        props.put("retries", 2);
        //Specify buffer size in config
//        props.put("batch.size", 16384);
        //Reduce the no of requests less than 0
//        props.put("linger.ms", 1);
        props.put("metadata.fetch.timeout.ms", 30000);
        props.put("max.request.size", 1048576);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
//        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer(props);
        Producer<String, String> producer2 = new KafkaProducer(props);

        ExecutorService executorService=Executors.newCachedThreadPool();
        executorService.execute(()->{
            try{
                FileInputStream fin = new FileInputStream(data);
                while (true) {
                    byte[] bytes = new byte[1024 * 1];
                    fin.read(bytes);
                    String content = new String(bytes, StandardCharsets.UTF_8);
                    for (int i = 0; i < 10; i++) {
                        producer.send(new ProducerRecord<>(topic,
                                UUID.randomUUID().toString(), content), (metadata, exception) -> {
                            if (exception != null) {
                                log.warn(exception.getMessage());
                            }
                        });
                    }
                    log.info("sent ten");
                    Thread.sleep(1000 * 3);
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        });

        executorService.execute(()->{
            try{
                FileInputStream fin = new FileInputStream(data);
                while (true) {
                    byte[] bytes = new byte[1024 * 1];
                    fin.read(bytes);
                    String content = new String(bytes, StandardCharsets.UTF_8);
                    for (int i = 0; i < 10; i++) {
                        producer2.send(new ProducerRecord<>(topic,
                                UUID.randomUUID().toString(), content), (metadata, exception) -> {
                            if (exception != null) {
                                log.warn(exception.getMessage());
                            }
                        });
                    }
                    log.info("sent ten");
                    Thread.sleep(1000 * 3);
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        });
        Thread.sleep(1000 * 40);
//        producer.close();
    }
}