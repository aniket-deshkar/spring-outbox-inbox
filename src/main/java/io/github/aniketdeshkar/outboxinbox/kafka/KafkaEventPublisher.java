package io.github.aniketdeshkar.outboxinbox.kafka;

import io.github.aniketdeshkar.outboxinbox.EventPublisher;
import io.github.aniketdeshkar.outboxinbox.OutboxMessage;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;

public final class KafkaEventPublisher implements EventPublisher {
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final TopicResolver topicResolver;

  public KafkaEventPublisher(
      KafkaTemplate<String, String> kafkaTemplate, TopicResolver topicResolver) {
    this.kafkaTemplate = Objects.requireNonNull(kafkaTemplate, "kafkaTemplate");
    this.topicResolver = Objects.requireNonNull(topicResolver, "topicResolver");
  }

  @Override
  public void publish(OutboxMessage message) {
    ProducerRecord<String, String> record =
        new ProducerRecord<>(
            topicResolver.topicFor(message), message.aggregateId(), message.payload());
    addHeader(record, "outbox-id", message.id().toString());
    addHeader(record, "event-type", message.eventType());
    addHeader(record, "aggregate-type", message.aggregateType());
    message.headers().forEach((name, value) -> addHeader(record, name, value));
    kafkaTemplate.send(record).join();
  }

  private static void addHeader(ProducerRecord<String, String> record, String name, String value) {
    record.headers().add(new RecordHeader(name, value.getBytes(StandardCharsets.UTF_8)));
  }
}
