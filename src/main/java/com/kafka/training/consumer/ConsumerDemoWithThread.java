package com.kafka.training.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public ConsumerDemoWithThread() {

    }

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    public void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        String topic = "first-topic";
        String bootstrapServer = "192.168.80.129:9092";
        String groupID = "my-first-application";

        //latch for dealing with multiple thread
        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunable(latch, topic,
                bootstrapServer, groupID);
        logger.info("Creating the consumer thread");
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();
        //Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunable) myConsumerRunnable).shutDown();
        }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunable implements Runnable {


        private final CountDownLatch countDownLatch;
        private final KafkaConsumer<String, String> consumer;
        Logger logger = LoggerFactory.getLogger(ConsumerRunable.class.getName());

        public ConsumerRunable(CountDownLatch countDownLatch, String topic,
                               String bootstrapServer, String groupID) {
            //Create Consumer Properties
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//earliest, latest, none
            this.countDownLatch = countDownLatch;
            //Create Consumer
            this.consumer = new KafkaConsumer<>(properties);
            //subscribe consumer to our topic(s)
            this.consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            //poll for new data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("key :" + record.key() + " Value: " + record.value());
                        logger.info("Partition :" + record.partition() + " Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received Shutdown Signal!");
            } finally {
                consumer.close();
                countDownLatch.countDown();
            }
        }

        public void shutDown() {
            //the wakeup() method is a special method to interrupt consumer.poll()
            //it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }
}

