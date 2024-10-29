package net.skybert.kafka;

import static java.lang.System.currentTimeMillis;

import java.time.Duration;
import java.util.Arrays;
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
 * Write and read some Kafka messages with Java and Emacs! This version has faster reads. Stay
 * tuned!
 *
 * @author <a href="mailto:torstein@skybert.net">Torstein Krause Johansen</a>
 */
public class KafkaAppFasterReads {
  private Duration readTimeout = Duration.ofSeconds(5);

  public static void main(String[] args) throws InterruptedException, ExecutionException {
    KafkaAppFasterReads app = new KafkaAppFasterReads();
    String topic = "dragons-" + currentTimeMillis();

    long start = currentTimeMillis();
    app.produceKafkaMessage(topic);
    app.produceKafkaMessage(topic);
    app.produceKafkaMessage(topic);
    System.out.println(String.format("🐉 Produce took: %d ms", (currentTimeMillis() - start)));

    start = currentTimeMillis();
    app.consumeSomeKafkaMessages(topic, consumerProperties());
    System.out.println(
        String.format(
            "🐉 Consume with default settings took: %d ms", (currentTimeMillis() - start)));

    start = currentTimeMillis();
    app.consumeSomeKafkaMessages(topic, consumerPropertiesFaster());
    System.out.println(
        String.format(
            "🐉 Consume with optimised settings took: %d ms", (currentTimeMillis() - start)));
  }

  private static Properties consumerProperties() {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    return properties;
  }

  private static Properties consumerPropertiesFaster() {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    // Min bytes: set this if you know how many bytes your typical
    // consume payload is.
    properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 200);
    // How long will the client wait around get to the aboeve
    // FETCH_MIN_BYTES_CONFIG?
    properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 2000);
    // How many records in one poll()?
    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 3);
    return properties;
  }

  private void consumeSomeKafkaMessages(final String pTopic, final Properties pProperties) {

    try (Consumer<String, String> consumer =
        new KafkaConsumer<>(pProperties, new StringDeserializer(), new StringDeserializer())) {
      TopicPartition topicPartition = new TopicPartition(pTopic, 0);
      consumer.assign(Arrays.asList(topicPartition));
      consumer.seekToBeginning(Arrays.asList(topicPartition));
      ConsumerRecords<String, String> consumerRecords = consumer.poll(readTimeout);
      for (var r : consumerRecords) {
        System.out.println("    🔥 Read Kafka message: " + r);
      }
    }
  }

  private void produceKafkaMessage(final String pTopic)
      throws InterruptedException, ExecutionException {

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    try (Producer<String, String> producer =
        new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
      Future<RecordMetadata> futureRecord =
          producer.send(
              new ProducerRecord<String, String>(
                  pTopic, "key", Long.toString(currentTimeMillis())));

      RecordMetadata recordMetadata = futureRecord.get();
      System.out.println(String.format("🐉 Created Kafka message: %s", recordMetadata));
    }
  }
}
