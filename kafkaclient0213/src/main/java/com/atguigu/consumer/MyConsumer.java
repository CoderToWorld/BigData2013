package com.atguigu.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * @author suyso
 * @create 2020-05-08 14:18
 */
public class MyConsumer {
    public static void main(String[] args) {
        //1. new消费者
        Properties properties = new Properties();

        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("bootstrap.servers", "hadoop103:9092");
        properties.setProperty("group.id", "Idea07");
        properties.setProperty("auto.offset.reset", "earliest");

        //关闭自动提交
        properties.setProperty("enable.auto.commit", "false");

        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<String, String>(properties);
        //2. 消费数据
        //consumer要首先订阅要拉取数据的话题
        consumer.subscribe(Collections.singleton("first"));

        //拉取数据
        Duration duration = Duration.ofMillis(500);

        while(true) {
            ConsumerRecords<String, String> poll = consumer.poll(duration);
            for (ConsumerRecord<String, String> record : poll) {
                System.out.println(record);
            }

            //手动同步提交offset
//            consumer.commitSync();

            //手动异步提交
            consumer.commitAsync(
                    new OffsetCommitCallback() {
                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                            offsets.forEach(
                                    (t, o) -> {
                                        System.out.println("分区：" + t + "\nOffset：" + o);
                                    }
                            );
                        }
                    }
            );
        }
        //3. 关闭资源
//        consumer.close(Duration.ofSeconds(10));
    }
}