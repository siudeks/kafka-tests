package net.siudek;

//Copyright (c) Microsoft Corporation. All rights reserved.
//Licensed under the MIT License.
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TestDataReporter implements Runnable {

    private static final int NUM_MESSAGES = 100;
    private final String TOPIC;

    private Producer<Long, String> producer;

    TestDataReporter(final Producer<Long, String> producer, String topic) {
        this.producer = producer;
        this.TOPIC = topic;
    }

    @Override
    public void run() {
        for(int i = 0; i < NUM_MESSAGES; i++) {
            long time = System.currentTimeMillis();
            System.err.println("Test Data #" + i + " from thread #" + Thread.currentThread().getId());

            final var record = new ProducerRecord<>(TOPIC, time, "Test Data #" + i);
            producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println(exception);
                        System.exit(1);
                    }
                });
        }
        System.out.println("Finished sending " + NUM_MESSAGES + " messages from thread #" + Thread.currentThread().getId() + "!");
    }
}