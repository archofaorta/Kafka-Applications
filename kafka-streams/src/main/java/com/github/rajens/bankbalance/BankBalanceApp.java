package com.github.rajens.bankbalance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import org.apache.kafka.common.utils.Bytes;
import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;

import static com.oracle.jrockit.jfr.ContentType.Bytes;

public class BankBalanceApp {

    static Logger logger = LoggerFactory.getLogger(BankBalanceApp.class.getName());

    public static void main(String[] args) {

        Properties properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-bank-balance");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0"); //not for prod

        // Exactly once processing!!
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.StringSerde.class.getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.serdeFrom(new JsonSerializer(),new JsonDeserializer()));

        // json Serializer, Deserializer
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, JsonNode> stream = streamsBuilder.stream("kafka-topic-bankbalance",
                Consumed.with(Serdes.String(),jsonSerde));

        logger.info("Printing Stream Before Transformation: ",
                stream.peek((key, value) -> logger.info("key= " + key + "Value= " + value.toString())));

        ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
        initialBalance.put("count",0);
        initialBalance.put("amount", 0);
        initialBalance.put("time", Instant.ofEpochMilli(0L).toString());

        KTable<String,JsonNode> groupedStream = stream.groupByKey(Serialized.with(Serdes.String(),jsonSerde))
                .aggregate(
                        () -> initialBalance, /* initializer */
                        (key,transaction,balance)-> getNewBalance(transaction,balance),
                        Materialized.<String, JsonNode, KeyValueStore<Bytes, byte[]>>as("aggregated-balance")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(jsonSerde)
                );

        //write to new topci
        groupedStream.toStream().to("new-balance-topic", Produced.with(Serdes.String(),jsonSerde));

        //create topology and start stream
        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.cleanUp();
        streams.start();

        // print the topology
        streams.localThreadsMetadata().forEach(data -> System.out.println(data));

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    private static JsonNode getNewBalance(JsonNode transaction, JsonNode balance) {
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put("count",balance.get("count").asInt()+1);
        newBalance.put("amount", balance.get("amount").asInt()
            + transaction.get("amount").intValue());
        newBalance.put("time", Instant.ofEpochMilli(Math.max(Instant.parse(balance.get("time").asText()).toEpochMilli(),
                Instant.parse(transaction.get("time").asText()).toEpochMilli())).toString());

        return  newBalance;
    }
}