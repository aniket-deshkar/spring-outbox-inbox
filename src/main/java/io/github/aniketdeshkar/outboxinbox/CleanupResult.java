package io.github.aniketdeshkar.outboxinbox;

public record CleanupResult(int outboxDeleted, int inboxDeleted) {}
