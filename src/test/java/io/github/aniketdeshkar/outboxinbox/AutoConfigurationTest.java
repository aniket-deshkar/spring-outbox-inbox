package io.github.aniketdeshkar.outboxinbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.github.aniketdeshkar.outboxinbox.autoconfigure.OutboxInboxAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;

class AutoConfigurationTest {
  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(OutboxInboxAutoConfiguration.class))
          .withBean(JdbcTemplate.class, () -> mock(JdbcTemplate.class))
          .withBean(PlatformTransactionManager.class, () -> mock(PlatformTransactionManager.class));

  @Test
  void configuresPostgresStoresInboxAndCleanupWithoutAProducer() {
    runner.run(
        context -> {
          assertThat(context).hasSingleBean(OutboxStore.class);
          assertThat(context).hasSingleBean(InboxStore.class);
          assertThat(context).hasSingleBean(InboxProcessor.class);
          assertThat(context).hasSingleBean(OutboxInboxCleanup.class);
          assertThat(context).doesNotHaveBean(PollingOutboxPublisher.class);
        });
  }

  @Test
  void addsKafkaPublisherAndPollerWhenTemplateExists() {
    runner
        .withBean("kafkaTemplate", KafkaTemplate.class, () -> mock(KafkaTemplate.class))
        .run(
            context -> {
              assertThat(context).hasSingleBean(EventPublisher.class);
              assertThat(context).hasSingleBean(PollingOutboxPublisher.class);
            });
  }
}
