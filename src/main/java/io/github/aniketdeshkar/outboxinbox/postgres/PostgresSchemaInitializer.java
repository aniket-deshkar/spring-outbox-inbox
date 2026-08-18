package io.github.aniketdeshkar.outboxinbox.postgres;

import java.util.Objects;
import javax.sql.DataSource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ScriptUtils;

public final class PostgresSchemaInitializer {
  private final DataSource dataSource;

  public PostgresSchemaInitializer(DataSource dataSource) {
    this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
  }

  public void initialize() {
    try (var connection = dataSource.getConnection()) {
      ScriptUtils.executeSqlScript(
          connection,
          new ClassPathResource("io/github/aniketdeshkar/outboxinbox/postgresql-schema.sql"));
    } catch (java.sql.SQLException exception) {
      throw new IllegalStateException("Unable to initialize outbox/inbox schema", exception);
    }
  }
}
