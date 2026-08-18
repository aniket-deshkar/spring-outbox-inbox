package io.github.aniketdeshkar.outboxinbox;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

public final class InboxProcessor {
  private final InboxStore store;
  private final TransactionTemplate processingTransaction;
  private final TransactionTemplate failureTransaction;
  private final Clock clock;
  private final int maxAttempts;
  private final Duration retryDelay;
  private final OutboxMetrics metrics;

  public InboxProcessor(
      InboxStore store,
      PlatformTransactionManager transactionManager,
      Clock clock,
      int maxAttempts,
      Duration retryDelay,
      OutboxMetrics metrics) {
    this.store = Objects.requireNonNull(store, "store");
    processingTransaction = new TransactionTemplate(transactionManager);
    failureTransaction = new TransactionTemplate(transactionManager);
    failureTransaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    this.clock = Objects.requireNonNull(clock, "clock");
    this.maxAttempts = requirePositive(maxAttempts);
    this.retryDelay = Objects.requireNonNull(retryDelay, "retryDelay");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
  }

  public InboxResult process(String consumerName, String messageId, CheckedMessageHandler handler) {
    requireText(consumerName, "consumerName");
    requireText(messageId, "messageId");
    Objects.requireNonNull(handler, "handler");
    try {
      InboxResult result =
          processingTransaction.execute(
              status -> processInTransaction(consumerName, messageId, handler));
      return Objects.requireNonNull(result, "transaction result");
    } catch (RuntimeException exception) {
      metrics.inboxFailure();
      failureTransaction.executeWithoutResult(
          status ->
              store.recordFailure(
                  consumerName,
                  messageId,
                  safeMessage(exception),
                  clock.instant(),
                  retryDelay,
                  maxAttempts));
      throw exception;
    }
  }

  private InboxResult processInTransaction(
      String consumerName, String messageId, CheckedMessageHandler handler) {
    InboxClaim claim = store.claim(consumerName, messageId, clock.instant(), maxAttempts);
    if (claim == InboxClaim.COMPLETED) {
      metrics.inboxDuplicate();
      return InboxResult.DUPLICATE;
    }
    if (claim == InboxClaim.IN_PROGRESS) {
      metrics.inboxDuplicate();
      return InboxResult.IN_PROGRESS;
    }
    if (claim == InboxClaim.DEAD_LETTER) {
      metrics.inboxDuplicate();
      return InboxResult.DEAD_LETTER;
    }
    try {
      handler.handle();
    } catch (RuntimeException exception) {
      throw exception;
    } catch (Exception exception) {
      throw new InboxProcessingException("Inbox business handler failed", exception);
    }
    store.complete(consumerName, messageId, clock.instant());
    metrics.inboxProcessed();
    return InboxResult.PROCESSED;
  }

  private static int requirePositive(int value) {
    if (value <= 0) {
      throw new IllegalArgumentException("maxAttempts must be positive");
    }
    return value;
  }

  private static void requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
  }

  private static String safeMessage(RuntimeException exception) {
    String message = exception.getMessage();
    return (message == null || message.isBlank()) ? exception.getClass().getName() : message;
  }
}
