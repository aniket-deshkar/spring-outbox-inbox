package io.github.aniketdeshkar.outboxinbox.postgres;

import io.github.aniketdeshkar.outboxinbox.OutboxEventRequest;
import io.github.aniketdeshkar.outboxinbox.OutboxMessage;
import io.github.aniketdeshkar.outboxinbox.OutboxStore;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

public final class PostgresOutboxStore implements OutboxStore {
  private static final TypeReference<Map<String, String>> HEADER_TYPE = new TypeReference<>() {};
  private final JdbcTemplate jdbc;
  private final ObjectMapper objectMapper;

  public PostgresOutboxStore(JdbcTemplate jdbc, ObjectMapper objectMapper) {
    this.jdbc = Objects.requireNonNull(jdbc, "jdbc");
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
  }

  @Override
  public UUID append(OutboxEventRequest event) {
    jdbc.update(
        "INSERT INTO outbox_event "
            + "(id,aggregate_type,aggregate_id,event_type,payload,headers,occurred_at,available_at,status) "
            + "VALUES (?,?,?,?,CAST(? AS jsonb),CAST(? AS jsonb),?,?,'PENDING')",
        event.id(),
        event.aggregateType(),
        event.aggregateId(),
        event.eventType(),
        event.payload(),
        objectMapper.writeValueAsString(event.headers()),
        Timestamp.from(event.occurredAt()),
        Timestamp.from(event.occurredAt()));
    return event.id();
  }

  @Override
  public List<OutboxMessage> claimBatch(
      int batchSize, String ownerToken, Instant now, Duration lockDuration) {
    String sql =
        """
        WITH candidates AS (
          SELECT id FROM outbox_event
          WHERE status IN ('PENDING','RETRY') AND available_at <= ?
            AND (locked_until IS NULL OR locked_until <= ?)
          ORDER BY occurred_at, id
          FOR UPDATE SKIP LOCKED
          LIMIT ?
        )
        UPDATE outbox_event event
        SET status='IN_PROGRESS', locked_by=?, locked_until=?
        FROM candidates
        WHERE event.id=candidates.id
        RETURNING event.*
        """;
    return jdbc.query(
        sql,
        (resultSet, rowNumber) -> map(resultSet),
        Timestamp.from(now),
        Timestamp.from(now),
        batchSize,
        ownerToken,
        Timestamp.from(now.plus(lockDuration)));
  }

  @Override
  public void markPublished(UUID id, String ownerToken, Instant publishedAt) {
    int updated =
        jdbc.update(
            "UPDATE outbox_event SET status='PUBLISHED', published_at=?, locked_by=NULL, "
                + "locked_until=NULL, last_error=NULL WHERE id=? AND status='IN_PROGRESS' AND locked_by=?",
            Timestamp.from(publishedAt),
            id,
            ownerToken);
    requireUpdated(updated, id);
  }

  @Override
  public void markFailed(
      OutboxMessage message, String error, Instant availableAt, boolean deadLetter) {
    int updated =
        jdbc.update(
            "UPDATE outbox_event SET status=?, attempts=attempts+1, available_at=?, "
                + "locked_by=NULL, locked_until=NULL, last_error=? "
                + "WHERE id=? AND status='IN_PROGRESS' AND locked_by=?",
            deadLetter ? "DEAD_LETTER" : "RETRY",
            Timestamp.from(availableAt),
            truncate(error),
            message.id(),
            message.ownerToken());
    requireUpdated(updated, message.id());
  }

  @Override
  public int deletePublishedBefore(Instant cutoff) {
    return jdbc.update(
        "DELETE FROM outbox_event WHERE status='PUBLISHED' AND published_at < ?",
        Timestamp.from(cutoff));
  }

  private OutboxMessage map(ResultSet resultSet) throws SQLException {
    return new OutboxMessage(
        resultSet.getObject("id", UUID.class),
        resultSet.getString("aggregate_type"),
        resultSet.getString("aggregate_id"),
        resultSet.getString("event_type"),
        resultSet.getString("payload"),
        objectMapper.readValue(resultSet.getString("headers"), HEADER_TYPE),
        resultSet.getTimestamp("occurred_at").toInstant(),
        resultSet.getInt("attempts"),
        resultSet.getString("locked_by"));
  }

  private static void requireUpdated(int updated, UUID id) {
    if (updated != 1) {
      throw new IllegalStateException("Outbox ownership was lost for event: " + id);
    }
  }

  private static String truncate(String value) {
    return value.length() <= 4000 ? value : value.substring(0, 4000);
  }
}
