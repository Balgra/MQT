package Mqt;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class eventConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("group.id", "test-group");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        String topic = "sensor_events";

        consumer.subscribe(Collections.singletonList(topic));

        Map<String, Integer> colorCounts = new HashMap<>();
        int totalMessages = 0;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                String color = record.key();
                int count = colorCounts.getOrDefault(color, 0);
                colorCounts.put(color, count + 1);
                totalMessages++;
            }

            if (totalMessages > 0) {
                System.out.println("Color statistics:");
                for (Map.Entry<String, Integer> entry : colorCounts.entrySet()) {
                    String color = entry.getKey();
                    int count = entry.getValue();
                    double percentage = (count * 100.0) / totalMessages;
                    System.out.printf("Color: %s, Count: %d, Percentage: %.2f%%\n", color, count, percentage);
                }
            }
        }
    }
}