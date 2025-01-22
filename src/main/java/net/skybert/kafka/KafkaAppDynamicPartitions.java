package net.skybert.kafka;

import static java.lang.System.currentTimeMillis;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaAppDynamicPartitions {

  private static void produceSomeKafkaMessages(final List<String> pTopics)
      throws InterruptedException, ExecutionException {

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    try (Producer<String, String> producer =
        new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
      for (String topic : pTopics) {
        Future<RecordMetadata> futureRecord =
            producer.send(
                new ProducerRecord<String, String>(
                    topic, String.format("key-%s", generateRandomString(8), currentTimeMillis())));

        RecordMetadata recordMetadata = futureRecord.get();
        System.out.println(
            String.format(
                "Created Kafka message: topic: %s, partition: %s offset: %s",
                topic, recordMetadata.partition(), recordMetadata.offset()));
      }
    }
  }

  public static String generateRandomString(int length) {
    String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    StringBuilder sb = new StringBuilder(length);

    for (int i = 0; i < length; i++) {
      int randomIndex = ThreadLocalRandom.current().nextInt(characters.length());
      sb.append(characters.charAt(randomIndex)); // Append a random character
    }

    return sb.toString();
  }

  public static void main(String[] args) throws InterruptedException, ExecutionException {
    // Create properties for the consumers
    //
    // If both consumers use the same group.id, partitions will be
    // dynamically allocated between them by Kafka.
    //
    // If they use different group.ids, both consumers will read all
    // partitions independently, effectively duplicating consumption.
    Properties consumerProps1 = createConsumerProperties("consumer-group");
    Properties consumerProps2 = createConsumerProperties("consumer-group");

    // assign an id of the consumer inside the group so that it's
    // recognised as the same consumer when the broker re-balances.
    consumerProps1.put("group.instance.id", "c1");
    consumerProps2.put("group.instance.id", "c2");

    KafkaConsumer<String, String> consumer1 = new KafkaConsumer<>(consumerProps1);
    KafkaConsumer<String, String> consumer2 = new KafkaConsumer<>(consumerProps2);

    String topicName = "example-topic";
    for (int i = 0; i < 10; i++) {
      produceSomeKafkaMessages(Arrays.asList(topicName));
    }

    consumer1.subscribe(Arrays.asList(topicName));
    consumer2.subscribe(Arrays.asList(topicName));

    // Create separate threads for each consumer
    Thread thread1 = new Thread(() -> pollMessages(consumer1, "Consumer-1"));
    Thread thread2 = new Thread(() -> pollMessages(consumer2, "Consumer-2"));

    // Start both threads
    thread1.start();
    thread2.start();
  }

  // Method to create KafkaConsumer properties
  private static Properties createConsumerProperties(String groupId) {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092"); // Replace with your Kafka broker address
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("group.id", groupId); // Specify a unique group ID for dynamic partition allocation
    props.put("auto.offset.reset", "earliest"); // Start reading from the beginning of the topic

    // quickly-ish decide if an old consumer isn't available anymore.
    props.put("session.timeout.ms", "6000"); // default is 30, minimum is 6

    // recognisable member of group
    return props;
  }

  // Method to poll messages from the topic
  private static void pollMessages(KafkaConsumer<String, String> consumer, String consumerName) {
    try {
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord<String, String> record : records) {
          System.out.printf(
              "%s: Partition: %d, Offset: %d, Key: %s, Value: %s%n",
              consumerName, record.partition(), record.offset(), record.key(), record.value());
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      consumer.close();
    }
  }
}
