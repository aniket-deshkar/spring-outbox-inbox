package io.github.aniketdeshkar.outboxinbox;

public enum InboxClaim {
  ACQUIRED,
  COMPLETED,
  IN_PROGRESS,
  DEAD_LETTER
}
