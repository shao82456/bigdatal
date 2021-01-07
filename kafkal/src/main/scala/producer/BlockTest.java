package producer;

import kafka.cluster.Partition;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * Author: shaoff
 * Date: 2020/6/2 14:28
 * Package: producer
 * Description:
 */
public class BlockTest {

    public static void blockForPartitions() throws Exception {
        Properties props = new Properties();
        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9092");
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
        KafkaProducer producer = new KafkaProducer(props);
        /*FileInputStream fin = new FileInputStream(data);
        int c=0;
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
//            log.info("content length:{}", content.length());
            Thread.sleep(1000 * 3);
            c++;
            if(c>10){
                break;
            }
        }*/
        try{
            List<PartitionInfo> p= producer.partitionsFor("_offset");
            System.out.println(p);
        }catch(Exception e)  {
            System.out.println(e.getMessage());
        }

//        Thread.sleep(1000 * 40);
//        producer.close();
    }

    public static void main(String[] args) throws Exception {
            blockForPartitions();
    }
}

