package com.atguigu.myproducer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author suyso
 * @create 2020-05-08 19:20
 */
public class Producer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("acks", "all");
        properties.setProperty("bootstrap.servers", "hadoop103:9092");
        properties.setProperty("batch.size", "10");
        properties.setProperty("linger.ms", "100");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 100; i++) {

            Future<RecordMetadata> result = producer.send(new ProducerRecord<String, String>(
                    "first",
                    "这是第" + i + "条消息"
            ), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (recordMetadata != null) {
                        String topic = recordMetadata.topic();
                        int partition = recordMetadata.partition();
                        long offset = recordMetadata.offset();
                        System.out.println(
                                topic + "话题"
                                        + partition + "分区"
                                        + offset + "条消息发送成功"
                        );
                    }

                }
            });
            RecordMetadata recordMetadata = result.get();
        }
        producer.close();
    }
}
