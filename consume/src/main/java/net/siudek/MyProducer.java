package net.siudek;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileReader;
import java.util.Properties;
import java.util.concurrent.Executors;

public class MyProducer {

    //Change constant to send messages to the desired topic, for this example we use 'test'
    private static final String TOPIC = "test";
    private static final int NUM_THREADS = 1;

    public static void main(String[] args) {
        final var producer = createProducer();
        final var executorService = Executors.newFixedThreadPool(NUM_THREADS);

        //Run NUM_THREADS TestDataReporters
        for (var i = 0; i < NUM_THREADS; i++)
            executorService.execute(new TestDataReporter(producer, TOPIC));    }

    private static Producer<Long, String> createProducer() {
        try{
            Properties properties = new Properties();
            properties.load(new FileReader("src/main/resources/producer.config"));
            properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            return new KafkaProducer<>(properties);
        } catch (Exception e){
            System.err.println("Failed to create producer with exception: " + e);
            System.exit(0);
            return null;
        }
    }
}
