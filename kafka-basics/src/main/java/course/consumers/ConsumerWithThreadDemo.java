package course.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerWithThreadDemo {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerWithThreadDemo.class);

    public ConsumerWithThreadDemo() {

    }
    public static void main(String[] args) {
        new ConsumerWithThreadDemo().run();
    }

    public void run() {
        final String bootstrapServers = "127.0.0.1:9092";
        final String groupId = "my-sixth-application";
        final String firstTopic = "first_topic";

        // latch for dealing with multiple threads
        CountDownLatch countDownLatch = new CountDownLatch(1);

        // create the consumer runnable
        ConsumerThread consumerThread = new ConsumerThread(bootstrapServers, groupId, firstTopic, countDownLatch);
        Thread thread = new Thread(consumerThread);
        thread.start();


        // starts the thread
        Runtime.getRuntime().addShutdownHook(new Thread(
            () -> {
                logger.info("Caught shutdown hook");
                consumerThread.shutdown();

                try {
                    countDownLatch.await();
                    logger.info("Second await is called");
                } catch (InterruptedException e) {
                    logger.error("Error during count down await: {}" + e.getMessage(), e);
                }

                logger.info("Application has exited");
            }
        ));

        try {
            countDownLatch.await();
            logger.info("First await is called");
        } catch (InterruptedException e) {
            logger.error("Error during count down await: {}" + e.getMessage(), e);
        } finally {
            logger.info("Application is closing");
        }

    }


    public static class ConsumerThread implements Runnable {

        private CountDownLatch countDownLatch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerThread(
                              String bootstrapServers,
                              String groupId,
                              String topic,
                              CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
            // create consumer config
            Properties properties = createPropertiesConfig(bootstrapServers, groupId);

            // create consumer
            consumer = new KafkaConsumer<>(properties);

            // subscribe consumer to topic
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                logger.info("Thread name: " + Thread.currentThread().getName());
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: {}, Value: {}", record.key(), record.value());
                        logger.info("Partition: {}, Offset: {}", record.partition(), record.offset());
                    }
                }
            } catch (WakeupException we) {
                logger.info("Received a shutdown signal!");
            } finally {
                consumer.close();
                // tell our main code the we done with main consumer
                countDownLatch.countDown();
            }

        }

        public void shutdown() {
            // the wakeup is a special method to interrupt consumer.poll()
            // it will throw the exception WakeupExpectation
            consumer.wakeup();
        }

        private static Properties createPropertiesConfig(String bootstrapServers, String groupId) {
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            return properties;
        }
    }


}
