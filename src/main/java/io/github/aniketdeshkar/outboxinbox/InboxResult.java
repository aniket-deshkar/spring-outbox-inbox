package io.github.aniketdeshkar.outboxinbox;

public enum InboxResult {
  PROCESSED,
  DUPLICATE,
  IN_PROGRESS,
  DEAD_LETTER
}
