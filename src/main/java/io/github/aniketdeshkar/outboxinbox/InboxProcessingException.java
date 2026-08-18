package io.github.aniketdeshkar.outboxinbox;

public final class InboxProcessingException extends RuntimeException {
  public InboxProcessingException(String message, Throwable cause) {
    super(message, cause);
  }
}
