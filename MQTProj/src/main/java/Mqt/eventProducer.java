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

        Producer<String, String> producer = new KafkaProducer<>(props);
        String topic = "sensor_events";

        String[] names = {"Google", "AmaBoomer", "Porus"};
        String[] cities = {"Cimisoara", "Banat", "Crisana"};
        String[] colors = {"GREEN", "BLUE", "GREY"};
        Random random = new Random();

        for (int i = 0; i < 20; i++) {
            String name = names[random.nextInt(names.length)];
            String city = cities[random.nextInt(cities.length)];
            String color = colors[random.nextInt(colors.length)];

            SensorEvent event = new SensorEvent(name, city, color);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, color, event.toJSONString());
            producer.send(record);
        }

        producer.close();
    }
}