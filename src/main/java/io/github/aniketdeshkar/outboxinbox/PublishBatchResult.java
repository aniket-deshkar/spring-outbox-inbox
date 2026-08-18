package io.github.aniketdeshkar.outboxinbox;

public record PublishBatchResult(int claimed, int published, int retrying, int deadLettered) {}
