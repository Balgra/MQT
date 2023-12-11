package Mqt;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

public class eventConsumer {
    public static void main(String[] args) {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "localhost:9092");
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", StringDeserializer.class.getName());
        consumerProps.put("group.id", "test-group");

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        String consumerTopic = "sensor_events";
        String producerTopic = "color_statistics";

        consumer.subscribe(Collections.singletonList(consumerTopic));

        Map<String, Integer> colorCounts = new HashMap<>();
        int totalMessages = 0;

        while (!Thread.currentThread().isInterrupted()) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    String value = record.value();

                    SensorEvent event = parseSensorEvent(value);

                    String color = event.getColor();
                    String city = event.getCity();
                    String message = "Color: " + color + ", City: " + city;

                    // Print the color and city name
                    System.out.println(message);

                    // Update color counts
                    int count = colorCounts.getOrDefault(color, 0);
                    colorCounts.put(color, count + 1);
                    totalMessages++;

                    // Send color statistics to another Kafka topic
                    double percentage = (count + 1) * 100.0 / totalMessages;
                    ProducerRecord<String, String> statsRecord = new ProducerRecord<>(producerTopic, color, "Count: " + (count + 1) + ", Percentage: " + String.format("%.2f%%", percentage));
                    producer.send(statsRecord);
                }

            } catch (Exception e) {
                Thread.currentThread().interrupt(); // Preserve the interrupt status
                break; // Exit the loop or perform cleanup
            }
        }
    }

    private static SensorEvent parseSensorEvent(String json) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(json, SensorEvent.class);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}