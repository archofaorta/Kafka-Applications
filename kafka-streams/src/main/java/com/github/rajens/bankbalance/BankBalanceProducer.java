package com.github.rajens.bankbalance;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.requests.ProduceRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class BankBalanceProducer {

    static Logger logger = LoggerFactory.getLogger(BankBalanceProducer.class.getName());

    public static void main(String[] args) {


        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"StringSerializer.class.getName()");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"StringSerializer.class.getName()");
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.RETRIES_CONFIG,"3");
        properties.put(ProducerConfig.LINGER_MS_CONFIG,"1");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");

        Producer<String,String> producer = new KafkaProducer<String, String>(properties);

        int i =0;
        while(true){
            logger.info("Producing Transaction Batch: " + i);
            try {
                producer.send(newTransaction("TRN-1"));
                Thread.sleep(1000);
                producer.send(newTransaction("TRN-2"));
                Thread.sleep(1000);
                producer.send(newTransaction("TRN-3"));
                Thread.sleep(1000);
                i+=1;
            } catch (InterruptedException e) {
                e.printStackTrace();
                producer.flush();
                producer.close();
                break;
            }
        }
    }

    public static ProducerRecord<String, String> newTransaction(String name){

        JsonNode trnNode = JsonNodeFactory.instance.objectNode();
        logger.info("Name: " + name);

        Integer amt = ThreadLocalRandom.current().nextInt(0,100);
        ((ObjectNode) trnNode).put("name",name);
        ((ObjectNode) trnNode).put("amount",amt);
        ((ObjectNode) trnNode).put("time", Instant.now().toString());

        return new ProducerRecord<String,String>("bank-transaction-topic",name,trnNode.toString());
    }
}
