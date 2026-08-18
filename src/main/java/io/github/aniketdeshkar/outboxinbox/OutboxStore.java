package io.github.aniketdeshkar.outboxinbox;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface OutboxStore {
  UUID append(OutboxEventRequest event);

  List<OutboxMessage> claimBatch(
      int batchSize, String ownerToken, Instant now, Duration lockDuration);

  void markPublished(UUID id, String ownerToken, Instant publishedAt);

  void markFailed(OutboxMessage message, String error, Instant availableAt, boolean deadLetter);

  int deletePublishedBefore(Instant cutoff);
}
