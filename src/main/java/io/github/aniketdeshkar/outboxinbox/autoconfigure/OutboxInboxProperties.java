package io.github.aniketdeshkar.outboxinbox.autoconfigure;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("spring.outbox-inbox")
public class OutboxInboxProperties {
  private boolean initializeSchema;
  private int batchSize = 100;
  private int maxAttempts = 5;
  private Duration lockDuration = Duration.ofSeconds(30);
  private Duration retryDelay = Duration.ofSeconds(10);
  private Duration retention = Duration.ofDays(7);
  private String kafkaTopicPrefix = "domain.";

  public boolean isInitializeSchema() {
    return initializeSchema;
  }

  public void setInitializeSchema(boolean initializeSchema) {
    this.initializeSchema = initializeSchema;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public int getMaxAttempts() {
    return maxAttempts;
  }

  public void setMaxAttempts(int maxAttempts) {
    this.maxAttempts = maxAttempts;
  }

  public Duration getLockDuration() {
    return lockDuration;
  }

  public void setLockDuration(Duration lockDuration) {
    this.lockDuration = lockDuration;
  }

  public Duration getRetryDelay() {
    return retryDelay;
  }

  public void setRetryDelay(Duration retryDelay) {
    this.retryDelay = retryDelay;
  }

  public Duration getRetention() {
    return retention;
  }

  public void setRetention(Duration retention) {
    this.retention = retention;
  }

  public String getKafkaTopicPrefix() {
    return kafkaTopicPrefix;
  }

  public void setKafkaTopicPrefix(String kafkaTopicPrefix) {
    this.kafkaTopicPrefix = kafkaTopicPrefix;
  }
}
