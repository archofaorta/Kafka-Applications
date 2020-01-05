package com.github.rajens.bankbalance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BankTransactionProducerTest {

    @Test
     public void TransactionTest(){

        ProducerRecord<String,String> record = BankBalanceProducer.newTransaction("TRN-1");
        String key = record.key();
        String value = record.value();

        assertEquals(key,"TRN-1");
        System.out.print(value);

        ObjectMapper mapper = new ObjectMapper();

        try {
            JsonNode node = mapper.readTree(value);
            assertTrue(node.get("name").asText().equals("TRN-1"));
            assertTrue("Amount Should be between 0 and 100", node.get("amount").asInt()<100);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
