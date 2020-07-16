package com.atguigu.myconsumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * @author suyso
 * @create 2020-05-08 19:21
 */
public class Consumer {
    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("bootstrap.servers", "hadoop103:9092");
        properties.setProperty("group.id", "g1");
        properties.setProperty("auto.offset.reset", "earliest");

        properties.setProperty("enable.auto.commit", "false");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singleton("first"));

        Duration duration = Duration.ofMillis(500);
        while (true) {
            ConsumerRecords<String, String> poll = consumer.poll(duration);
            for (ConsumerRecord<String, String> record : poll) {
                System.out.println(record);
            }

            consumer.commitAsync(
                    new OffsetCommitCallback() {
                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                            map.forEach(
                                    (t, o) -> {
                                        System.out.println("分区：" + t + " \nOffset" + o);
                                    }
                            );
                        }
                    }
            );
        }

    }
}
