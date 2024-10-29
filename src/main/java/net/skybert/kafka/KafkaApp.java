package net.skybert.kafka;

import static java.lang.System.currentTimeMillis;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Write and read some Kafka messages with Java and Emacs!
 *
 * @author <a href="mailto:torstein@skybert.net">Torstein Krause Johansen</a>
 */
public class KafkaApp {

  public static void main(String[] args) throws InterruptedException, ExecutionException {
    KafkaApp app = new KafkaApp();
    List<String> topics = Arrays.asList(
        "hobbits-" + currentTimeMillis(),
        "orcs-" + currentTimeMillis(),
        "dragons-" + currentTimeMillis());

    app.produceSomeKafkaMessages(topics);
    app.consumeSomeKafkaMessages(topics);
  }

  private Duration readTimeout = Duration.ofSeconds(5);

  private void consumeSomeKafkaMessages(final List<String> pTopics) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    try (
        Consumer<String, String> consumer = new KafkaConsumer<>(
            props,
            new StringDeserializer(),
            new StringDeserializer())) {
      for (String topic : pTopics) {
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(topicPartition));
        consumer.seekToBeginning(Arrays.asList(topicPartition));
        ConsumerRecords<String, String> consumerRecords = consumer.poll(readTimeout);
        for (var r : consumerRecords) {
          System.out.println("Read Kafka message: " + r);
        }
      }
    }
  }

  private void produceSomeKafkaMessages(final List<String> pTopics)
    throws InterruptedException, ExecutionException {

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    try (
        Producer<String, String> producer = new KafkaProducer<>(
            props,
            new StringSerializer(),
            new StringSerializer())) {
      for (String topic : pTopics) {
        Future<RecordMetadata> futureRecord = producer.send(
            new ProducerRecord<String,String>(
                topic,
                "key",
                Long.toString(currentTimeMillis())));

        RecordMetadata recordMetadata = futureRecord.get();
        System.out.println(String.format("Created Kafka message: %s", recordMetadata));
      }
    }
  }


}
