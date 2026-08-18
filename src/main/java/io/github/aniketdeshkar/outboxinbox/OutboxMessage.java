package io.github.aniketdeshkar.outboxinbox;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record OutboxMessage(
    UUID id,
    String aggregateType,
    String aggregateId,
    String eventType,
    String payload,
    Map<String, String> headers,
    Instant occurredAt,
    int attempts,
    String ownerToken) {}
