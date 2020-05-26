package course.twitter;

import com.google.common.base.Strings;
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

    private static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    static final List<String> terms = Lists.newArrayList("kafka");

    public TwitterProducer() {

    }
    public static void main(String[] args)  {
        new TwitterProducer().run();
    }

    public void run(){
        logger.info("Start of the application");

        // create a twitter client
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
        Client client = createKafkaTwitterClient(msgQueue);
        // Attempts to establish a connection.
        client.connect();

        // create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
           logger.info("stopping application ...");
           logger.info("stopping client ...");
           client.stop();
           logger.info("stopping producer ...");
           producer.close();
            logger.info("done!");
        }));

        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            try {
                String msg = msgQueue.poll(5, TimeUnit.SECONDS);
                if (!Strings.isNullOrEmpty(msg)) {
                    logger.info(msg);
                    producer.send(new ProducerRecord<>("twitter_topic2", null, msg), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception != null) {
                                logger.error("Something goes wrong.", exception);
                            }
                        }
                    });
                }
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
                client.stop();
            }

        }

        // You can close a connection
        if (!client.isDone()){
            client.stop();
        }
        logger.info("End of the application");


        // loop to send tweets to kafka
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        final String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        return kafkaProducer;
    }

    private Client createKafkaTwitterClient(BlockingQueue<String> msgQueue) {


        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        // List<Long> followings = Lists.newArrayList(1234L, 566788L);
        // hosebirdEndpoint.followings(followings);


        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file

        Authentication hosebirdAuth = new OAuth1(ProducerConstants.API_KEY, ProducerConstants.API_SECRET_KEY, ProducerConstants.ACCESS_TOKEN, ProducerConstants.ACCESS_TOKEN_SECRET);

        // Creating a client
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        return hosebirdClient;
    }


}
