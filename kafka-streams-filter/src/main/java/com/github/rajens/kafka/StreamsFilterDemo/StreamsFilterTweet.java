package com.github.rajens.kafka.StreamsFilterDemo;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweet {

    public static void main(String[] args) {

        //create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"twitter_tweets");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //input topic
        KStream<String,String> inputTopic = streamsBuilder.stream("twitter_tweets");
        KStream<String,String> filteredStream = inputTopic.filter(
                (k,jsonTweet) ->  getFollowersInTweets(jsonTweet) > 1000
        );
        filteredStream.to("filtered_tweets");
        //build a topology
        KafkaStreams kafkaStreams = new KafkaStreams(
                streamsBuilder.build(),
                properties);

        kafkaStreams.start();
    }

    private static Integer getFollowersInTweets(String jsonTweet){
        JsonParser jsonParser = new JsonParser();

        try {
         return   jsonParser.parse(jsonTweet)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        }

        catch(NullPointerException e){
            return 0;
        }

    }
}
