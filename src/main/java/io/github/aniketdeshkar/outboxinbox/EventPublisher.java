package io.github.aniketdeshkar.outboxinbox;

@FunctionalInterface
public interface EventPublisher {
  void publish(OutboxMessage message) throws Exception;
}
