package twitter_lab_uriel;



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
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import sun.plugin2.os.windows.SECURITY_ATTRIBUTES;

import java.io.FileReader;
import java.io.IOException;
import java.security.Security;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class twitter_producer {
    //config
    public static final String protocol = "SSL";
    public static final String password = "password";
    public static final String location = "/home/priel/truststore.jks";
    public static final String consumer_certificate = "/home/priel/ca.pem";
    public static final String CONSUMER_KEYS= "kdbjYuubsptldw3ioSTFtMyCj";
    public static final String CONSUMER_SECRETS= "8SFtoUOUI62H3HSksOru0esLs0PEZRiPoT9sOPKBVkE5FQBNiL";
    public static final String SECRET = "xM4JXEu0DKZNyecc4UfW6HKR2JcYX6vOkHSsunQZAmSTh";
    public static final String TOKEN = "1236688745254002689-LkXcFyTEz7XXUp7DdP3yWCIUrY4go2";
    public static final String KEYSTORE_TYPE= "pkcs12";
    //outh2
    public static final String KEY_KEYSTORE_PASSWORD = "ES7ru7wDjq9L";


    Logger logger = LoggerFactory.getLogger(twitter_producer.class.getName());

    //producer constructor
    public twitter_producer(){}

    public static void main(String[] args) {
        new twitter_producer().run();
    }
    public void run () {
        logger.info("Setup");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        //twitter client
        Client client = twitterClient(msgQueue);
        // Attempts to establish a connection.
        client.connect();
        Properties properties = new Properties();
        try {
            properties.load(new FileReader("/home/priel/kafka_2.12-2.8.0/config/producer.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        String bootstrapserver = "kafka-bootstrap.lab.test:443";
        //truststore and keystore should be .jks files
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.setProperty(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, KEYSTORE_TYPE);
//        properties.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, consumer_certificate);
//        properties.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, KEY_KEYSTORE_PASSWORD);
//        properties.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, KEY_KEYSTORE_PASSWORD);
//        properties.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, location);
//        properties.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, password);
//        //WHY ???? properties.setProperty(SslConfigs.SSL_CLIENT_AUTH_CONFIG)
//        //properties.setProperty(SslConfigs.SSL_PROTOCOL_CONFIG, protocol);//MAYBE TLSv1.2?????
//        properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol);




        //kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        //prop
        //create_record();

        //sending twitts via looping
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                create_record(msg, producer);
                logger.info(msg);
            }
            logger.info("End of app");
        }
    }

    public void create_record(String msg, KafkaProducer<String, String> producer) {
        //create record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("twitter_tweets", msg);
        //send data
        producer.send(record);
        producer.flush();
    }

    public Client twitterClient(BlockingQueue<String> msgQueue){
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        //terms to stream
        //List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("twitter");
        //hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);
        //auth
        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEYS, CONSUMER_SECRETS, TOKEN, SECRET);
        //creating client
        ClientBuilder builder = new ClientBuilder()
                .name("Uriel-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }
}
