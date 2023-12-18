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
        String producerTopicGrey = "color_grey";
        String producerTopicBlue = "color_blue";
        String producerTopicGreen = "color_green";
        String producerTopicPercentage = "color_percentage";

        consumer.subscribe(Collections.singletonList(consumerTopic));

        Map<String, Integer> colorCountsGrey = new HashMap<>();
        Map<String, Integer> colorCountsBlue = new HashMap<>();
        Map<String, Integer> colorCountsGreen = new HashMap<>();
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

                    // Update color counts for respective colors
                    Map<String, Integer> colorCounts;
                    switch (color) {
                        case "GREY":
                            colorCounts = colorCountsGrey;
                            break;
                        case "BLUE":
                            colorCounts = colorCountsBlue;
                            break;
                        case "GREEN":
                            colorCounts = colorCountsGreen;
                            break;
                        default:
                            colorCounts = new HashMap<>();
                            break;
                    }

                    int count = colorCounts.getOrDefault(color, 0);
                    colorCounts.put(color, count + 1);
                    totalMessages++;

                    // Send color statistics to respective Kafka topics
                    String topicToSend;
                    switch (color) {
                        case "GREY":
                            topicToSend = producerTopicGrey;
                            break;
                        case "BLUE":
                            topicToSend = producerTopicBlue;
                            break;
                        case "GREEN":
                            topicToSend = producerTopicGreen;
                            break;
                        default:
                            topicToSend = "";
                            break;
                    }
                    ProducerRecord<String, String> statsRecord = new ProducerRecord<>(topicToSend, color, "Count: " + (count + 1));
                    producer.send(statsRecord);
                }

                // Send color percentages to another Kafka topic
                if (totalMessages > 0) {
                    double percentageGrey = colorCountsGrey.getOrDefault("GREY", 0) * 100.0 / totalMessages;
                    double percentageBlue = colorCountsBlue.getOrDefault("BLUE", 0) * 100.0 / totalMessages;
                    double percentageGreen = colorCountsGreen.getOrDefault("GREEN", 0) * 100.0 / totalMessages;

                    ProducerRecord<String, String> percentageRecord = new ProducerRecord<>(producerTopicPercentage, "Color Percentages",
                            "Grey: " + String.format("%.2f%%", percentageGrey) +
                                    ", Blue: " + String.format("%.2f%%", percentageBlue) +
                                    ", Green: " + String.format("%.2f%%", percentageGreen));
                    producer.send(percentageRecord);
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