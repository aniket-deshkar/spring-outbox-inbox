package io.github.aniketdeshkar.outboxinbox;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aniketdeshkar.outboxinbox.kafka.KafkaEventPublisher;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers(disabledWithoutDocker = true)
class KafkaEventPublisherTest {
  @Container
  private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:3.9.1");

  @Test
  void publishesPayloadKeyAndTraceableOutboxHeaders() {
    var producerFactory =
        new DefaultKafkaProducerFactory<String, String>(
            Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class));
    KafkaEventPublisher publisher =
        new KafkaEventPublisher(new KafkaTemplate<>(producerFactory), message -> "orders");
    UUID id = UUID.randomUUID();
    publisher.publish(
        new OutboxMessage(
            id,
            "order",
            "order-77",
            "OrderPlaced",
            "{\"id\":\"order-77\"}",
            Map.of("trace-id", "trace-1"),
            Instant.now(),
            0,
            "owner"));

    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "publisher-test-" + UUID.randomUUID());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
      consumer.subscribe(List.of("orders"));
      var records = consumer.poll(Duration.ofSeconds(10));
      assertThat(records).hasSize(1);
      var record = records.iterator().next();
      assertThat(record.key()).isEqualTo("order-77");
      assertThat(record.value()).isEqualTo("{\"id\":\"order-77\"}");
      assertThat(header(record.headers().lastHeader("outbox-id"))).isEqualTo(id.toString());
      assertThat(header(record.headers().lastHeader("event-type"))).isEqualTo("OrderPlaced");
      assertThat(header(record.headers().lastHeader("trace-id"))).isEqualTo("trace-1");
    } finally {
      producerFactory.destroy();
    }
  }

  private static String header(org.apache.kafka.common.header.Header header) {
    return new String(header.value(), StandardCharsets.UTF_8);
  }
}
