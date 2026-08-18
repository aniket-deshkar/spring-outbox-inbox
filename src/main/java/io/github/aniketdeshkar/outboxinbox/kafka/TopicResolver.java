package io.github.aniketdeshkar.outboxinbox.kafka;

import io.github.aniketdeshkar.outboxinbox.OutboxMessage;

@FunctionalInterface
public interface TopicResolver {
  String topicFor(OutboxMessage message);
}
