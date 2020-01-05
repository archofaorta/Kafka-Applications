package com.github.rajens.kafka.SimplePublishSubscribeDemo;

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

    private final String bootstapServers = "localhost:9092";
    private final String topic = "kafka_topic_a";
    private final String groupId = "my-fifth-kafka-application";

    public static void main(String[] args) {
     new ConsumerDemoWithThread().run();
    }

    private ConsumerDemoWithThread(){

    }

    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
        CountDownLatch latch = new CountDownLatch(1);
        Runnable myConsumerRunnable = new ConsumerRunnable(latch);

        //Start The Thread
        Thread myThread = new Thread(myConsumerRunnable) ;
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Caught Shutdown hook");
                ((ConsumerRunnable) myConsumerRunnable).shutdown();
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }finally {
                    logger.info("Application has exited");
                }
            }
        ,"Shutdown-thread"));

        try{
            latch.await();
        }catch(InterruptedException ex){
            logger.error("Application Interrupted Exception " + ex.getLocalizedMessage());
        }finally {
            logger.info("Application Is Closing ..");
        }
    }

    public class ConsumerRunnable implements Runnable{

        //Create Consumer Config
        private Properties  properties = new Properties();
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
        KafkaConsumer<String, String> consumer;
        CountDownLatch latch;

        public ConsumerRunnable(CountDownLatch latch){

            this.latch = latch;

            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                //Pull Messages
                while(true){
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord record : records) {

                        logger.info("Key: " + record.key() + " Value: " + record.value());
                        logger.info("Offset: " + record.offset() + " Partition: " + record.partition());
                    }
                }
            }catch (WakeupException wex) {
                logger.info("Received Shutdown Signal !!");
            }finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown(){
            //Special method to interrupt consumer
            consumer.wakeup();
        }
    }

}
