package io.github.aniketdeshkar.outboxinbox.autoconfigure;

import io.github.aniketdeshkar.outboxinbox.EventPublisher;
import io.github.aniketdeshkar.outboxinbox.InboxProcessor;
import io.github.aniketdeshkar.outboxinbox.InboxStore;
import io.github.aniketdeshkar.outboxinbox.OutboxInboxCleanup;
import io.github.aniketdeshkar.outboxinbox.OutboxMetrics;
import io.github.aniketdeshkar.outboxinbox.OutboxStore;
import io.github.aniketdeshkar.outboxinbox.PollingOutboxPublisher;
import io.github.aniketdeshkar.outboxinbox.kafka.KafkaEventPublisher;
import io.github.aniketdeshkar.outboxinbox.kafka.TopicResolver;
import io.github.aniketdeshkar.outboxinbox.postgres.PostgresInboxStore;
import io.github.aniketdeshkar.outboxinbox.postgres.PostgresOutboxStore;
import io.github.aniketdeshkar.outboxinbox.postgres.PostgresSchemaInitializer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import javax.sql.DataSource;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import tools.jackson.databind.ObjectMapper;

@AutoConfiguration
@EnableConfigurationProperties(OutboxInboxProperties.class)
@ConditionalOnClass(JdbcTemplate.class)
public class OutboxInboxAutoConfiguration {
  @Bean
  @ConditionalOnMissingBean
  Clock outboxInboxClock() {
    return Clock.systemUTC();
  }

  @Bean
  @ConditionalOnMissingBean
  OutboxMetrics outboxMetrics(ObjectProvider<MeterRegistry> registry) {
    return new OutboxMetrics(registry.getIfAvailable(SimpleMeterRegistry::new));
  }

  @Bean
  @ConditionalOnMissingBean(OutboxStore.class)
  @ConditionalOnBean(JdbcTemplate.class)
  PostgresOutboxStore postgresOutboxStore(
      JdbcTemplate jdbc, ObjectProvider<ObjectMapper> objectMapper) {
    return new PostgresOutboxStore(jdbc, objectMapper.getIfAvailable(ObjectMapper::new));
  }

  @Bean
  @ConditionalOnMissingBean(InboxStore.class)
  @ConditionalOnBean(JdbcTemplate.class)
  PostgresInboxStore postgresInboxStore(JdbcTemplate jdbc) {
    return new PostgresInboxStore(jdbc);
  }

  @Bean(initMethod = "initialize")
  @ConditionalOnBean(DataSource.class)
  @ConditionalOnProperty(
      prefix = "spring.outbox-inbox",
      name = "initialize-schema",
      havingValue = "true")
  PostgresSchemaInitializer postgresSchemaInitializer(DataSource dataSource) {
    return new PostgresSchemaInitializer(dataSource);
  }

  @Bean
  @ConditionalOnMissingBean
  TopicResolver topicResolver(OutboxInboxProperties properties) {
    return message -> properties.getKafkaTopicPrefix() + message.aggregateType();
  }

  @Bean
  @ConditionalOnMissingBean(EventPublisher.class)
  @ConditionalOnBean(KafkaTemplate.class)
  KafkaEventPublisher kafkaEventPublisher(
      KafkaTemplate<String, String> kafkaTemplate, TopicResolver topicResolver) {
    return new KafkaEventPublisher(kafkaTemplate, topicResolver);
  }

  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnBean({OutboxStore.class, EventPublisher.class})
  PollingOutboxPublisher pollingOutboxPublisher(
      OutboxStore store,
      EventPublisher publisher,
      Clock clock,
      OutboxInboxProperties properties,
      OutboxMetrics metrics) {
    return new PollingOutboxPublisher(
        store,
        publisher,
        clock,
        properties.getBatchSize(),
        properties.getMaxAttempts(),
        properties.getLockDuration(),
        properties.getRetryDelay(),
        metrics);
  }

  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnBean({InboxStore.class, PlatformTransactionManager.class})
  InboxProcessor inboxProcessor(
      InboxStore store,
      PlatformTransactionManager transactionManager,
      Clock clock,
      OutboxInboxProperties properties,
      OutboxMetrics metrics) {
    return new InboxProcessor(
        store,
        transactionManager,
        clock,
        properties.getMaxAttempts(),
        properties.getRetryDelay(),
        metrics);
  }

  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnBean({OutboxStore.class, InboxStore.class})
  OutboxInboxCleanup outboxInboxCleanup(
      OutboxStore outbox, InboxStore inbox, Clock clock, OutboxInboxProperties properties) {
    return new OutboxInboxCleanup(outbox, inbox, clock, properties.getRetention());
  }
}
