package io.fbaa.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.UnexpectedException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoWithShutdown {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer!");

        String bootstrapServer = "127.0.0.1:9092";
        String groupIp = "my-third-application";
        String topic = "demo_java";

        // creating consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupIp);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // creating the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get the reference to the currence thread
        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("Detect a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                //join the main thread to allow the execution of the code in the mai thread

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            //subscribe consumer to our topics
            consumer.subscribe(Collections.singletonList(topic)); //Arrays.asList(topic)

            //poll for new data
            while (true) {
//                log.info("Pooling.");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offsets: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("WakeupException!");
            // we ignore this one because this is an expected exception when closing a consumer
        } catch (Exception e) {
            log.info("Unespected Exception!");
            e.printStackTrace();

        } finally {
            consumer.close(); // this will also commit the offsets if need be
            log.info("The consumer os now gracefully closed!");
        }

    }
}