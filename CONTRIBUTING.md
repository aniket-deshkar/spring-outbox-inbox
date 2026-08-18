# Contributing

## Development Workflow

1. Create a focused branch from `main`.
2. Preserve explicit at-least-once and ordering semantics in API and documentation changes.
3. Add deterministic state-machine tests and PostgreSQL/Kafka acceptance tests where relevant.
4. Run `./mvnw verify` on JDK 21 or newer with Docker available.
5. Open a pull request describing transaction, retry, ordering, and compatibility impact.

Use conventional commit subjects. Do not commit credentials, broker configuration, database dumps, IDE state, or generated build output.

## Code Standards

Spotless enforces Google Java Format and PMD performs static analysis. Storage transitions must remain conditional and concurrency-safe; errors must not be silently discarded.
