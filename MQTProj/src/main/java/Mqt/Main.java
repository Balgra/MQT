package Mqt;

public class Main {
        public static void main(String[] args) {
            System.out.println("Starting Kafka Producer...");
            eventProducer producer = new eventProducer();
            producer.main(new String[]{});

            System.out.println("Starting Kafka Consumer with Statistics...");
            eventConsumer consumer = new eventConsumer();
            consumer.main(new String[]{});


        }
    }
