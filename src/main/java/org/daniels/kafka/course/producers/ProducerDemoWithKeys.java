package org.daniels.kafka.course.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {

    private static final Logger logger  = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.out.println("=== start producer === ");
        final String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        // create a producer record
        String msg = "This is a message sent from application. Current idx: %d";

        for (int i = 0; i < 10; i++) {
            String topicName = "first_topic";
            String key = "id_" + i;
            String message = String.format(msg, i);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topicName, key, message);
            // send data -
            // each key between execution goes to the same partition
            // key: id_0 partition: 1
            // key: id_1 partition: 0
            // key: id_2 partition: 2
            // key: id_3 partition: 0
            // key: id_4 partition: 2
            // key: id_5 partition: 2
            // key: id_6 partition: 0
            // key: id_7 partition: 2
            // key: id_8 partition: 1
            // key: id_9 partition: 2


            kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
                if (e == null) {
                    logger.info("Current key: " + key);
                    logger.info("Receive new metadata. \n"
                            + "Topic: " + recordMetadata.topic() + "\n"
                            + "Partition: " + recordMetadata.partition() + "\n"
                            + "Offset: " + recordMetadata.offset() + "\n"
                            + "Timestamp: " + recordMetadata.timestamp()
                    );
                } else {
                    logger.error("Error while producing", e);
                }
            }).get(); // make a code synchronous, don't do that in the production!!!
        }

        // two ways to sent data - flush or close
        kafkaProducer.flush();

        //kafkaProducer.close();
    }
}
