package io.github.aniketdeshkar.outboxinbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aniketdeshkar.outboxinbox.postgres.PostgresInboxStore;
import io.github.aniketdeshkar.outboxinbox.postgres.PostgresOutboxStore;
import io.github.aniketdeshkar.outboxinbox.postgres.PostgresSchemaInitializer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.ObjectMapper;

@Testcontainers(disabledWithoutDocker = true)
class PostgresAcceptanceTest {
  @Container
  private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17-alpine");

  private JdbcTemplate jdbc;
  private PostgresOutboxStore outbox;
  private InboxProcessor inbox;
  private TransactionTemplate transaction;

  @BeforeEach
  void setUp() {
    DataSource dataSource =
        new DriverManagerDataSource(
            POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    jdbc = new JdbcTemplate(dataSource);
    new PostgresSchemaInitializer(dataSource).initialize();
    jdbc.update("TRUNCATE outbox_event, inbox_message");
    jdbc.execute("CREATE TABLE IF NOT EXISTS business_order (id VARCHAR(100) PRIMARY KEY)");
    jdbc.update("TRUNCATE business_order");
    outbox = new PostgresOutboxStore(jdbc, new ObjectMapper());
    var transactionManager = new DataSourceTransactionManager(dataSource);
    transaction = new TransactionTemplate(transactionManager);
    inbox =
        new InboxProcessor(
            new PostgresInboxStore(jdbc),
            transactionManager,
            Clock.systemUTC(),
            2,
            Duration.ZERO,
            new OutboxMetrics(new SimpleMeterRegistry()));
  }

  @Test
  void businessCommitAndOutboxAppendAreAtomic() {
    assertThatThrownBy(
            () ->
                transaction.executeWithoutResult(
                    status -> {
                      jdbc.update("INSERT INTO business_order(id) VALUES ('rolled-back')");
                      outbox.append(
                          OutboxEventRequest.create(
                              "order", "rolled-back", "OrderPlaced", "{\"id\":\"rolled-back\"}"));
                      throw new IllegalStateException("rollback");
                    }))
        .isInstanceOf(IllegalStateException.class);
    assertThat(jdbc.queryForObject("SELECT count(*) FROM business_order", Integer.class)).isZero();
    assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_event", Integer.class)).isZero();

    transaction.executeWithoutResult(
        status -> {
          jdbc.update("INSERT INTO business_order(id) VALUES ('committed')");
          outbox.append(
              OutboxEventRequest.create(
                  "order", "committed", "OrderPlaced", "{\"id\":\"committed\"}"));
        });
    assertThat(jdbc.queryForObject("SELECT count(*) FROM business_order", Integer.class)).isOne();
    assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_event", Integer.class)).isOne();
  }

  @Test
  void duplicateAndConcurrentMessagesExecuteBusinessLogicOnce() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    Callable<InboxResult> task =
        () ->
            inbox.process(
                "billing",
                "message-1",
                () -> {
                  calls.incrementAndGet();
                  jdbc.update("INSERT INTO business_order(id) VALUES ('from-message')");
                });
    Set<InboxResult> results;
    try (var pool = Executors.newFixedThreadPool(2)) {
      results =
          pool.invokeAll(java.util.List.of(task, task)).stream()
              .map(
                  future -> {
                    try {
                      return future.get();
                    } catch (Exception exception) {
                      throw new IllegalStateException(exception);
                    }
                  })
              .collect(Collectors.toSet());
    }

    assertThat(results).containsExactlyInAnyOrder(InboxResult.PROCESSED, InboxResult.DUPLICATE);
    assertThat(calls).hasValue(1);
    assertThat(jdbc.queryForObject("SELECT count(*) FROM business_order", Integer.class)).isOne();
  }

  @Test
  void failedMessagesRetryAndBecomeDeadLettersWithoutCommittingBusinessState() {
    for (int attempt = 0; attempt < 2; attempt++) {
      assertThatThrownBy(
              () ->
                  inbox.process(
                      "billing",
                      "poison",
                      () -> {
                        jdbc.update("INSERT INTO business_order(id) VALUES ('must-rollback')");
                        throw new IllegalArgumentException("poison payload");
                      }))
          .isInstanceOf(IllegalArgumentException.class);
    }

    assertThat(inbox.process("billing", "poison", () -> {})).isEqualTo(InboxResult.DEAD_LETTER);
    assertThat(jdbc.queryForObject("SELECT count(*) FROM business_order", Integer.class)).isZero();
    assertThat(
            jdbc.queryForObject(
                "SELECT status FROM inbox_message WHERE consumer_name='billing' AND message_id='poison'",
                String.class))
        .isEqualTo("DEAD_LETTER");
  }
}
