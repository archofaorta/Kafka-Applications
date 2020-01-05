package com.github.rajens.wordcount;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(WordCountApp.class.getName());

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"kafka-streams-wordcount");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        //create input stream
        KStream<String, String> wcStream = builder.stream("word-count-input-topic");
        
        KTable <String,Long> wordCount = (KTable<String, Long>) wcStream.mapValues(value -> value.toLowerCase())
            .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
            .selectKey((key,value)-> value)
            .groupByKey()
            .count();

        wordCount.toStream().to("word-count-output-topic", Produced.with(Serdes.String(),Serdes.Long()));

        final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),properties);
        kafkaStreams.start();
        logger.info("Stream Topology: " + kafkaStreams.toString());

        //close application gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }

}
