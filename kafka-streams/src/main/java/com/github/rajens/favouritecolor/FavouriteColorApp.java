package com.github.rajens.favouritecolor;

import com.github.rajens.wordcount.WordCountApp;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class FavouriteColorApp {

    Logger logger = LoggerFactory.getLogger(WordCountApp.class.getName());

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(WordCountApp.class.getName());
        Properties properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"kafka-streams-favourite-color");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());



        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String,String> stream = streamsBuilder.stream("color-input-topic");


        logger.info("Printing Stream Before Transformation: ",
                stream.peek((key,value) -> logger.info("key= " + key + "Value= " + value)));

        KStream<String,String> textLine = stream.filter((key,value) -> (value.contains(",")))
                //Select 'User' as Key
                .selectKey((key,value)-> (value.split(","))[0].toLowerCase())
                //Convert value to lowercase
                .mapValues((value)-> (value.split(","))[1].toLowerCase())
                //filter on required color for instance - red, bule, green
                .filter((key,value)-> Arrays.asList("red","blue","green").contains(value));
        logger.info("Printing Stream After Transformation: "
                , stream.peek((key,value) -> logger.info("key= " + key + "Value= " + value)));
        //write to intermediate topic
        textLine.to("color-output-topic");
        //read from topic as table
        KTable<String,String> colorInTable = streamsBuilder.table("color-output-topic");
        //group by color and count
        KTable<String,Long> colorOutTable = colorInTable
                .groupBy((key,value)->new KeyValue<>(value,value))
                .count();
        //write back to topic
        colorOutTable.toStream().to("favourite-color-topic", Produced.with(Serdes.String(),Serdes.Long()));

        final KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(),properties);
        //kafkaStreams.cleanUp();
        kafkaStreams.start();
        logger.info("Stream Topology: " + kafkaStreams.toString());

        //close application gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));


    }


}
