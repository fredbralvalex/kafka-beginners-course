package io.fbaa.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Producer Keys!");

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //sticky partitioner - too fast, so it is preferable(efficient) to batch in only one partition
        // using Thread make it sleep for 1s
        for (int i = 0; i < 10; i++) {

            String topic = "demo_java";
            String value = "Hello World " + i;
            String key = "id_" + i;

            // create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key,value);
            // send data - asynchronous operation
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    // executes every time the record is successfully sent or an exception is thrown
                    if (e == null) {
                        // the record was successfully sent
                        log.info("Received new metadata/ \n" +
                                "Topic: " + metadata.topic() + " \n" +
                                "Key: " + record.key() + " \n" +
                                "Partition: " + metadata.partition() + " \n" +
                                "Offset: " + metadata.offset() + " \n" +
                                "Timestamp: " + metadata.timestamp());
                    } else {
                        log.error("Error while producing.", e);
                    }
                }
            });
        }

        // flush data - synchronous
        producer.flush();

        // flush and close producer
        producer.close();
    }
}