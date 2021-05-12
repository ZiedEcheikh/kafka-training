package com.kafka.training.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithCallBack {
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class.getName());

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.80.129:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            String topic = "first-topic";
            String value = "hello Kafka" + i;
            String key = "key_" + i;
            //Create Producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            //Send Data
            producer.send(record, (recordMetadata, e) -> {
                if (e == null) {
                    logger.info("Received new metadata. \n" +
                            "Topic : " + recordMetadata.topic() + "\n" +
                            "Partition : " + recordMetadata.partition() + "\n" +
                            "Offset : " + recordMetadata.offset() + "\n" +
                            "Timestamp : " + recordMetadata.timestamp());
                } else {
                    logger.error("Error while producing", e);
                }
            }).get(); // block the send to make it synchronous
        }
        producer.flush();
        producer.close();
    }
}
