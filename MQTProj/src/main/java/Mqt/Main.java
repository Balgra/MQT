package Mqt;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

public class Main {
    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String topic = "sensor_events";
        int partitions = 3;
        short replicationFactor = 1;

        try (AdminClient adminClient = AdminClient.create(Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {

            boolean topicExists = adminClient.listTopics().names().get().contains(topic);

            if (!topicExists) {
                NewTopic newTopic = new NewTopic(topic, partitions, replicationFactor);
                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
                System.out.println("Topic created: " + topic);
            } else {
                System.out.println("Topic already exists: " + topic);
            }

            // Start Kafka Producer
            System.out.println("Starting Kafka Producer...");
            Thread producerThread = new Thread(() -> {
                eventProducer producer = new eventProducer();
                producer.main(new String[]{});
            });
            producerThread.start();

            // Start Kafka Consumer
            System.out.println("Starting Kafka Consumer with Statistics...");
            Thread consumerThread = new Thread(() -> {
                eventConsumer consumer = new eventConsumer();
                consumer.main(new String[]{});
            });
            consumerThread.start();

            // Wait for some time (e.g., 30 seconds) and then stop producer and consumer threads
            Thread.sleep(30000);

            producerThread.interrupt();
            consumerThread.interrupt();


        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
