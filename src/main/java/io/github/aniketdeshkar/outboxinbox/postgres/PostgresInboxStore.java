package io.github.aniketdeshkar.outboxinbox.postgres;

import io.github.aniketdeshkar.outboxinbox.InboxClaim;
import io.github.aniketdeshkar.outboxinbox.InboxStore;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import org.springframework.jdbc.core.JdbcTemplate;

public final class PostgresInboxStore implements InboxStore {
  private final JdbcTemplate jdbc;

  public PostgresInboxStore(JdbcTemplate jdbc) {
    this.jdbc = Objects.requireNonNull(jdbc, "jdbc");
  }

  @Override
  public InboxClaim claim(String consumerName, String messageId, Instant now, int maxAttempts) {
    int inserted =
        jdbc.update(
            "INSERT INTO inbox_message "
                + "(consumer_name,message_id,status,attempts,received_at,available_at) "
                + "VALUES (?,?,'PROCESSING',0,?,?) ON CONFLICT DO NOTHING",
            consumerName,
            messageId,
            Timestamp.from(now),
            Timestamp.from(now));
    if (inserted == 1) {
      return InboxClaim.ACQUIRED;
    }
    int retried =
        jdbc.update(
            "UPDATE inbox_message SET status='PROCESSING' "
                + "WHERE consumer_name=? AND message_id=? AND status='FAILED' "
                + "AND attempts < ? AND available_at <= ?",
            consumerName,
            messageId,
            maxAttempts,
            Timestamp.from(now));
    if (retried == 1) {
      return InboxClaim.ACQUIRED;
    }
    String status =
        jdbc.queryForObject(
            "SELECT status FROM inbox_message WHERE consumer_name=? AND message_id=?",
            String.class,
            consumerName,
            messageId);
    return switch (Objects.requireNonNull(status)) {
      case "COMPLETED" -> InboxClaim.COMPLETED;
      case "DEAD_LETTER" -> InboxClaim.DEAD_LETTER;
      default -> InboxClaim.IN_PROGRESS;
    };
  }

  @Override
  public void complete(String consumerName, String messageId, Instant completedAt) {
    int updated =
        jdbc.update(
            "UPDATE inbox_message SET status='COMPLETED', completed_at=?, last_error=NULL "
                + "WHERE consumer_name=? AND message_id=? AND status='PROCESSING'",
            Timestamp.from(completedAt),
            consumerName,
            messageId);
    if (updated != 1) {
      throw new IllegalStateException("Inbox claim was lost: " + consumerName + "/" + messageId);
    }
  }

  @Override
  public void recordFailure(
      String consumerName,
      String messageId,
      String error,
      Instant now,
      Duration retryDelay,
      int maxAttempts) {
    jdbc.update(
        "INSERT INTO inbox_message "
            + "(consumer_name,message_id,status,attempts,received_at,available_at,last_error) "
            + "VALUES (?,?,CASE WHEN 1>=? THEN 'DEAD_LETTER' ELSE 'FAILED' END,1,?,?,?) "
            + "ON CONFLICT (consumer_name,message_id) DO UPDATE SET "
            + "attempts=inbox_message.attempts+1, "
            + "status=CASE WHEN inbox_message.attempts+1>=? THEN 'DEAD_LETTER' ELSE 'FAILED' END, "
            + "available_at=?, last_error=?",
        consumerName,
        messageId,
        maxAttempts,
        Timestamp.from(now),
        Timestamp.from(now.plus(retryDelay)),
        truncate(error),
        maxAttempts,
        Timestamp.from(now.plus(retryDelay)),
        truncate(error));
  }

  @Override
  public int deleteCompletedBefore(Instant cutoff) {
    return jdbc.update(
        "DELETE FROM inbox_message WHERE status='COMPLETED' AND completed_at < ?",
        Timestamp.from(cutoff));
  }

  private static String truncate(String value) {
    return value.length() <= 4000 ? value : value.substring(0, 4000);
  }
}
