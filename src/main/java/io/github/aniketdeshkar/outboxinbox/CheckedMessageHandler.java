package io.github.aniketdeshkar.outboxinbox;

@FunctionalInterface
public interface CheckedMessageHandler {
  void handle() throws Exception;
}
