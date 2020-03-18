package producer;//import util.properties packages

import org.apache.kafka.clients.producer.*;
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
    private static String data="/Users/sakura/stuff/bigdatal/firesparkl/src/main/resources/data";
    private static String topic="test";
    private static String bootstrapServer="localhost:9093";

    public static void main(String[] args) throws Exception {
        if(args.length>0){
            data=args[0];
            topic=args[1];
            bootstrapServer=args[2];
        }
        //Assign topicName to string variable
        // create instance for properties to access producer configs
        Properties props = new Properties();
        //Assign localhost id
        props.put("bootstrap.servers", bootstrapServer);
        //Set acknowledgements for producer requests.
        props.put("acks", "all");
        //If the request fails, the producer can automatically retry,
        props.put("retries", 2);
        //Specify buffer size in config
        props.put("batch.size", 16384);
        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);
        props.put("metadata.fetch.timeout.ms",3000);
        props.put("max.request.size",1048576);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer(props);

        while (true) {
<<<<<<< Updated upstream:kafkal/src/main/scala/producer/SimpleProducer.java
            FileInputStream fin = new FileInputStream(data);
            byte[] bytes = new byte[1024 * 1024*9];
            fin.read(bytes);
            String content = new String(bytes, StandardCharsets.UTF_8);
            for(int i=0;i<1;i++){
                producer.send(new ProducerRecord<>(topic,
                        UUID.randomUUID().toString(), content), (metadata, exception) -> {
                            if(exception!=null){
                                log.warn(exception.getMessage());
                            }
                        });
                log.info("sent once");
=======
            String path = "/home/sakura/final/bigdatal/firesparkl/src/main/scala/sakura/test/kafka/data";
            FileInputStream fin = new FileInputStream(path);
            byte[] bytes = new byte[1024 * 1024*9];
            fin.read(bytes);
            String content = new String(bytes, StandardCharsets.UTF_8);
            for(int i=0;i<5;i++){
                producer.send(new ProducerRecord<>(topicName,
                        UUID.randomUUID().toString(), content));
>>>>>>> Stashed changes:firesparkl/src/main/scala/sakura/test/kafka/SimpleProducer.java
            }
            log.info("content length:{}",content.length());
            Thread.sleep(1000*3);
        }
//        producer.close();
    }
}