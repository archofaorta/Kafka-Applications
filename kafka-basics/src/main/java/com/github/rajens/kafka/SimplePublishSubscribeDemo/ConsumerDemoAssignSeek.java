package com.github.rajens.kafka.SimplePublishSubscribeDemo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class ConsumerDemoAssignSeek {

    private static Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());
    private static String bootstapServers = "localhost:9092";

    public static void main(String[] args) {

        String topic = "kafka_topic_a";
        //String groupId = "my-seventh-kafka-application";

        //Create Consumer Config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        //properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //assign and seek are used for replay data or fetch a specific message

        TopicPartition partition = new TopicPartition(topic,0);
        consumer.assign(Arrays.asList(partition));

        long offset = 15L;
        int recordsReadSoFar=0;
        boolean keepOnReading=true;

        //seek
        consumer.seek(partition,offset);

        //Poll for new messages
        while(keepOnReading){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record : records) {
                logger.info("Key: " + record.key() + " Value: " + record.value());
                logger.info("Offset: " + record.offset() + " Partition: " + record.partition());
                recordsReadSoFar+=1;

                if(recordsReadSoFar>=5){
                    keepOnReading=false; //exit condition
                    break; // break for loop
                }
            }
        }
        logger.info("Exiting Application");
    }
}
