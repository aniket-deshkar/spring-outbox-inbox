package io.github.aniketdeshkar.outboxinbox;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

public final class OutboxInboxCleanup {
  private final OutboxStore outbox;
  private final InboxStore inbox;
  private final Clock clock;
  private final Duration retention;

  public OutboxInboxCleanup(OutboxStore outbox, InboxStore inbox, Clock clock, Duration retention) {
    this.outbox = Objects.requireNonNull(outbox, "outbox");
    this.inbox = Objects.requireNonNull(inbox, "inbox");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.retention = Objects.requireNonNull(retention, "retention");
  }

  public CleanupResult cleanup() {
    var cutoff = clock.instant().minus(retention);
    return new CleanupResult(
        outbox.deletePublishedBefore(cutoff), inbox.deleteCompletedBefore(cutoff));
  }
}
