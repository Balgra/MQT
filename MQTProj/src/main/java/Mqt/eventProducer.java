package Mqt;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class eventProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String,String>(props);
        String topic = "sensor_events";

        String[] colors = {"GREEN", "BLUE", "GREY"};
        Random random = new Random();

        for (int i = 0; i < 10000; i++) {
            String color = colors[random.nextInt(colors.length)];
            String sensorEvent = generateSensorEvent(color);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, color, sensorEvent);
            producer.send(record);
        }

        producer.close();
    }

    private static String generateSensorEvent(String color) {
        return "Sensor event of color: " + color;
    }
}
