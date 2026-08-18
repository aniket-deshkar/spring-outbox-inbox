package io.github.aniketdeshkar.outboxinbox;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public record OutboxEventRequest(
    UUID id,
    String aggregateType,
    String aggregateId,
    String eventType,
    String payload,
    Map<String, String> headers,
    Instant occurredAt) {
  public OutboxEventRequest {
    id = id == null ? UUID.randomUUID() : id;
    aggregateType = requireText(aggregateType, "aggregateType");
    aggregateId = requireText(aggregateId, "aggregateId");
    eventType = requireText(eventType, "eventType");
    payload = requireText(payload, "payload");
    headers = headers == null ? Map.of() : Map.copyOf(headers);
    occurredAt = occurredAt == null ? Instant.now() : occurredAt;
  }

  public static OutboxEventRequest create(
      String aggregateType, String aggregateId, String eventType, String payload) {
    return new OutboxEventRequest(
        null, aggregateType, aggregateId, eventType, payload, Map.of(), null);
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
