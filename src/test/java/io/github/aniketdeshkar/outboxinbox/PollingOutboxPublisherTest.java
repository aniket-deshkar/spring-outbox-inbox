package io.github.aniketdeshkar.outboxinbox;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PollingOutboxPublisherTest {
  private static final Instant NOW = Instant.parse("2026-08-18T00:00:00Z");

  @Test
  void publishesClaimedMessagesAndMarksThemComplete() {
    FakeStore store = new FakeStore(message(0));
    List<UUID> published = new ArrayList<>();
    PollingOutboxPublisher polling = polling(store, event -> published.add(event.id()), 3);

    PublishBatchResult result = polling.pollOnce();

    assertThat(result).isEqualTo(new PublishBatchResult(1, 1, 0, 0));
    assertThat(published).containsExactly(store.message.id());
    assertThat(store.published).isTrue();
  }

  @Test
  void retriesThenDeadLettersAtConfiguredAttemptLimit() {
    FakeStore retryStore = new FakeStore(message(0));
    PollingOutboxPublisher retry =
        polling(
            retryStore,
            event -> {
              throw new IllegalStateException("broker offline");
            },
            3);

    assertThat(retry.pollOnce()).isEqualTo(new PublishBatchResult(1, 0, 1, 0));
    assertThat(retryStore.deadLetter).isFalse();
    assertThat(retryStore.error).isEqualTo("broker offline");

    FakeStore deadStore = new FakeStore(message(2));
    PollingOutboxPublisher dead =
        polling(
            deadStore,
            event -> {
              throw new IllegalStateException("still offline");
            },
            3);
    assertThat(dead.pollOnce()).isEqualTo(new PublishBatchResult(1, 0, 0, 1));
    assertThat(deadStore.deadLetter).isTrue();
  }

  private static PollingOutboxPublisher polling(
      OutboxStore store, EventPublisher publisher, int maxAttempts) {
    return new PollingOutboxPublisher(
        store,
        publisher,
        Clock.fixed(NOW, ZoneOffset.UTC),
        10,
        maxAttempts,
        Duration.ofSeconds(30),
        Duration.ofSeconds(5),
        new OutboxMetrics(new SimpleMeterRegistry()));
  }

  private static OutboxMessage message(int attempts) {
    return new OutboxMessage(
        UUID.randomUUID(),
        "order",
        "order-1",
        "OrderPlaced",
        "{}",
        java.util.Map.of(),
        NOW,
        attempts,
        "owner");
  }

  private static final class FakeStore implements OutboxStore {
    private final OutboxMessage message;
    private boolean published;
    private boolean deadLetter;
    private String error;

    private FakeStore(OutboxMessage message) {
      this.message = message;
    }

    @Override
    public UUID append(OutboxEventRequest event) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<OutboxMessage> claimBatch(
        int batchSize, String ownerToken, Instant now, Duration lockDuration) {
      return List.of(message);
    }

    @Override
    public void markPublished(UUID id, String ownerToken, Instant publishedAt) {
      published = true;
    }

    @Override
    public void markFailed(
        OutboxMessage message, String error, Instant availableAt, boolean deadLetter) {
      this.error = error;
      this.deadLetter = deadLetter;
    }

    @Override
    public int deletePublishedBefore(Instant cutoff) {
      return 0;
    }
  }
}
