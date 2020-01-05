package com.github.rajens.kafka.SimplePublishSubscribeDemo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.Integer;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithCallback {

    private static String bootstrapServers = "localhost:9092";
    private static Logger logger = LoggerFactory.getLogger("ProducerDemoWithCallback");

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>( properties);

        for(int i=0; i<10; i++) {

            //Same key goes to same partition
            String topic = "kafka_topic_a";
            String value = "Hello World " + Integer.toString(i);
            String key = "Id_" + Integer.toString(i);

            ProducerRecord<String, String> message
                    = new ProducerRecord(topic,key,value);

            logger.info("Key :" + key);
            producer.send(message, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Topic :" + recordMetadata.topic() + "\n" +
                                " Partition: " + recordMetadata.partition() + "\n" +
                                " Offset: " + recordMetadata.offset() + "\n" +
                                " Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.info("Error Producing Producer Message" + e.getLocalizedMessage());
                    }
                }
            }).get(); //async test
        }
        producer.flush();
        producer.close();

    }
}
