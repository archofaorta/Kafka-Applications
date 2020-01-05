    package com.github.rajens.kafka.SimpleStreamDemo;

    import com.google.common.collect.Lists;
    import com.twitter.hbc.ClientBuilder;
    import com.twitter.hbc.core.Client;
    import com.twitter.hbc.core.Constants;
    import com.twitter.hbc.core.Hosts;
    import com.twitter.hbc.core.HttpHosts;
    import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
    import com.twitter.hbc.core.processor.StringDelimitedProcessor;
    import com.twitter.hbc.httpclient.auth.Authentication;
    import com.twitter.hbc.httpclient.auth.OAuth1;
    import org.apache.kafka.clients.producer.*;
    import org.apache.kafka.common.serialization.StringSerializer;
    import org.slf4j.Logger;
    import org.slf4j.LoggerFactory;

    import java.util.List;
    import java.util.Properties;
    import java.util.concurrent.BlockingQueue;
    import java.util.concurrent.LinkedBlockingQueue;
    import java.util.concurrent.TimeUnit;

    public class TwitterProducer {

        private static String bootstrapServers = "localhost:9092";

        // put your own credentials here for Twitter API
        private final String consumerKey = "";
        private final String consumerSecret = "";
        private final String  token = "";
        private final String tokenSecret = "";

        Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

        public static void main(String[] args) {
            new TwitterProducer().run();
        }

        public TwitterProducer(){
        }

        public void run(){

            logger.info("Setting up twitter client connection..");

            /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
            BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
            Client client = CreateTwitterClient(msgQueue);
            client.connect();

            //Create Kafka Producer
            KafkaProducer<String,String> producer = CreateKafkaProducer();

            Runtime.getRuntime().addShutdownHook(new Thread (() -> {
                logger.info("Stopping application..");
                logger.info("Closing twitter client connection");
                client.stop();
                logger.info("Shutting down producer");
                producer.close();
                logger.info("Application Closed");
            },"shutdown-thread"));

            // on a different thread, or multiple different threads....
            while (!client.isDone()) {
                String msg = null;
                try{
                    msg = msgQueue.poll(5, TimeUnit.MILLISECONDS);
                }catch (InterruptedException e){
                    e.printStackTrace();
                    client.stop();
                }
                if(msg != null){
                   logger.info(msg);
                    producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
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
                    });
                }
            }
            logger.info("End Application");
        }

        public Client CreateTwitterClient(BlockingQueue<String> msgQueue){

            /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
            Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
            StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
            List<String> terms = Lists.newArrayList("apache-kafka");
            hosebirdEndpoint.trackTerms(terms);

            // These secrets should be read from a config file,
            Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);
            ClientBuilder builder = new ClientBuilder()
                    .name("Client-01")  // optional: mainly for the logs
                    .hosts(hosebirdHosts)
                    .authentication(hosebirdAuth)
                    .endpoint(hosebirdEndpoint)
                    .processor(new StringDelimitedProcessor(msgQueue));

            Client hosebirdClient = builder.build();
            return hosebirdClient;
        }

        public KafkaProducer<String,String> CreateKafkaProducer(){
            Properties properties = new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            //safe producer
            properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
            properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
            properties.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,Integer.toString(Integer.MAX_VALUE));
            properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");

            //high throughput producer (at expense of small latency and CPU usage)
            properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
            properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
            properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));

            KafkaProducer<String,String> producer = new KafkaProducer<String, String>( properties);
            return producer;
        }
    }