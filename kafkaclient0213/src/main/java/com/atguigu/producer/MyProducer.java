package com.atguigu.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author suyso
 * @create 2020-05-08 10:46
 */
public class MyProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //1.new对象，初始化一个KafkaProducer的抽象对象
        Properties properties = new Properties();
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("acks", "all");
        properties.setProperty("bootstrap.servers", "hadoop103:9092");
        properties.setProperty("batch.size", "10");
        properties.setProperty("linger.ms", "500");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //2.做事情
        for (int i = 0; i < 100; i++) {
            Future<RecordMetadata> result = producer.send(new ProducerRecord<String, String>(
                    "first",
                    "Message" + i,
                    "这是第" + i + "条信息"
            ), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (recordMetadata != null) {
                        String topic = recordMetadata.topic();
                        int partition = recordMetadata.partition();
                        long offset = recordMetadata.offset();
                        System.out.println(
                                topic + "话题"
                                        + partition + "分区第"
                                        + offset + "条数据发送成功"
                        );
                    }
                }
            });
            RecordMetadata recordMetadata = result.get();
            System.out.println("第" + i + "条消息发送结束");
        }
        //2.关闭资源
        producer.close();
    }
}
