package io.github.aniketdeshkar.outboxinbox;

import java.time.Duration;
import java.time.Instant;

public interface InboxStore {
  InboxClaim claim(String consumerName, String messageId, Instant now, int maxAttempts);

  void complete(String consumerName, String messageId, Instant completedAt);

  void recordFailure(
      String consumerName,
      String messageId,
      String error,
      Instant now,
      Duration retryDelay,
      int maxAttempts);

  int deleteCompletedBefore(Instant cutoff);
}
