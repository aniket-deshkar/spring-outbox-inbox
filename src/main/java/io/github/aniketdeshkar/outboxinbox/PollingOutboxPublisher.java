package io.github.aniketdeshkar.outboxinbox;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public final class PollingOutboxPublisher {
  private final OutboxStore store;
  private final EventPublisher publisher;
  private final Clock clock;
  private final int batchSize;
  private final int maxAttempts;
  private final Duration lockDuration;
  private final Duration retryDelay;
  private final OutboxMetrics metrics;

  public PollingOutboxPublisher(
      OutboxStore store,
      EventPublisher publisher,
      Clock clock,
      int batchSize,
      int maxAttempts,
      Duration lockDuration,
      Duration retryDelay,
      OutboxMetrics metrics) {
    this.store = Objects.requireNonNull(store, "store");
    this.publisher = Objects.requireNonNull(publisher, "publisher");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.batchSize = requirePositive(batchSize, "batchSize");
    this.maxAttempts = requirePositive(maxAttempts, "maxAttempts");
    this.lockDuration = requirePositive(lockDuration, "lockDuration");
    this.retryDelay = requirePositive(retryDelay, "retryDelay");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
  }

  public PublishBatchResult pollOnce() {
    Instant now = clock.instant();
    String owner = UUID.randomUUID().toString();
    List<OutboxMessage> messages = store.claimBatch(batchSize, owner, now, lockDuration);
    int published = 0;
    int retrying = 0;
    int deadLettered = 0;
    for (OutboxMessage message : messages) {
      try {
        publisher.publish(message);
        store.markPublished(message.id(), message.ownerToken(), clock.instant());
        metrics.published();
        published++;
      } catch (Exception exception) {
        int nextAttempt = message.attempts() + 1;
        boolean deadLetter = nextAttempt >= maxAttempts;
        Duration delay = retryDelay.multipliedBy(Math.max(1L, nextAttempt));
        store.markFailed(message, safeMessage(exception), clock.instant().plus(delay), deadLetter);
        if (deadLetter) {
          metrics.deadLetter();
          deadLettered++;
        } else {
          metrics.retry();
          retrying++;
        }
      }
    }
    return new PublishBatchResult(messages.size(), published, retrying, deadLettered);
  }

  private static int requirePositive(int value, String name) {
    if (value <= 0) {
      throw new IllegalArgumentException(name + " must be positive");
    }
    return value;
  }

  private static Duration requirePositive(Duration value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isZero() || value.isNegative()) {
      throw new IllegalArgumentException(name + " must be positive");
    }
    return value;
  }

  private static String safeMessage(Exception exception) {
    String message = exception.getMessage();
    return (message == null || message.isBlank()) ? exception.getClass().getName() : message;
  }
}
