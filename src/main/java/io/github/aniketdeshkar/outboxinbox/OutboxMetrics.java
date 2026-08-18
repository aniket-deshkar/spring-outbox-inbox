package io.github.aniketdeshkar.outboxinbox;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

public final class OutboxMetrics {
  private final Counter published;
  private final Counter retries;
  private final Counter deadLetters;
  private final Counter inboxProcessed;
  private final Counter inboxDuplicates;
  private final Counter inboxFailures;

  public OutboxMetrics(MeterRegistry registry) {
    published = registry.counter("outbox.events", "outcome", "published");
    retries = registry.counter("outbox.events", "outcome", "retry");
    deadLetters = registry.counter("outbox.events", "outcome", "dead_letter");
    inboxProcessed = registry.counter("inbox.messages", "outcome", "processed");
    inboxDuplicates = registry.counter("inbox.messages", "outcome", "duplicate");
    inboxFailures = registry.counter("inbox.messages", "outcome", "failed");
  }

  void published() {
    published.increment();
  }

  void retry() {
    retries.increment();
  }

  void deadLetter() {
    deadLetters.increment();
  }

  void inboxProcessed() {
    inboxProcessed.increment();
  }

  void inboxDuplicate() {
    inboxDuplicates.increment();
  }

  void inboxFailure() {
    inboxFailures.increment();
  }
}
