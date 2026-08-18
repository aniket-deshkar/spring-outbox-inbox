package io.github.aniketdeshkar.outboxinbox;

public enum OutboxStatus {
  PENDING,
  IN_PROGRESS,
  RETRY,
  PUBLISHED,
  DEAD_LETTER
}
