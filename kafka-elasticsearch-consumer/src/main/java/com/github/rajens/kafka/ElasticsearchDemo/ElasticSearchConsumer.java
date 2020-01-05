package com.github.rajens.kafka.ElasticsearchDemo;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import com.google.gson.JsonParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    public static void main(String[] args) throws  IOException {
        RestHighLevelClient client = createClient();
        KafkaConsumer<String,String> consumer = createConsumer();
        //Pull Messages
        while(true){
             ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
             for (ConsumerRecord<String, String> record : records) {

                 String id = extractId(record.value());

                 IndexRequest indexRequest = new IndexRequest(
                         "twitter",
                         "tweets",
                            id
                 ).source(record.value(), XContentType.JSON);

                 IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                 logger.info(indexResponse.getId());

                 try {
                   Thread.sleep(1000); //introduce delay
                 }
                 catch (InterruptedException e) {
                     e.printStackTrace();
                 }
             }
            //client.close();
        }
    }

    public ElasticSearchConsumer(){
    }

    public static KafkaConsumer<String,String> createConsumer(){

        String bootstapServers = "localhost:9092";
        String topic = "twitter_tweets";
        String groupId = "twitter_tweets";

        //Create Consumer Config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    public static RestHighLevelClient createClient (){

        String hostname = "kakfa-poc-6986890997.us-east-1.bonsaisearch.net";
        String username = "den2vqog3g";
        String password = "dppeee61i3";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username,password));

        RestClientBuilder builder = RestClient.builder( new HttpHost(hostname,443,"https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);

        return  client;

    }

    private static String extractId(String tweetJson){
        JsonParser jsonParser = new JsonParser();
        return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }


}
