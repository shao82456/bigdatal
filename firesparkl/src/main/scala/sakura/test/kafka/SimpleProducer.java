package sakura.test.kafka;//import util.properties packages

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.UUID;

//import simple producer packages
//import KafkaProducer packages
//import ProducerRecord packages

//Create java class named “SimpleProducer”
public class SimpleProducer {
    private static Logger log=LoggerFactory.getLogger(SimpleProducer.class);
    public static void main(String[] args) throws Exception {

        //Assign topicName to string variable
        String topicName = "test";
        // create instance for properties to access producer configs
        Properties props = new Properties();
        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9092");
        //Set acknowledgements for producer requests.
        props.put("acks", "all");
        //If the request fails, the producer can automatically retry,
        props.put("retries", 1);
        //Specify buffer size in config
        props.put("batch.size", 16384);
        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);
        props.put("max.request.size",10485760);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer(props);

        while (true) {
            String path = "/Users/sakura/project/firesparkl/src/main/scala/sakura/test/kafka/data";
            FileInputStream fin = new FileInputStream(path);
            byte[] bytes = new byte[1024 * 1024*9];
            fin.read(bytes);
            String content = new String(bytes, StandardCharsets.UTF_8);
            for(int i=0;i<50;i++){
                producer.send(new ProducerRecord<>(topicName,
                        UUID.randomUUID().toString(), content));
            }
            log.info("content length:{}",content.length());
            Thread.sleep(1000*3);
        }
//        producer.close();
    }
}