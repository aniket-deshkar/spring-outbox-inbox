# Changelog

All notable changes are documented here. This project follows Semantic Versioning.

## 0.1.0 - 2026-08-18

### Added

- PostgreSQL transactional outbox writer and `SKIP LOCKED` polling claims.
- Synchronous Kafka adapter with stable aggregate keys and traceable headers.
- Transactional inbox deduplication with retry and dead-letter state.
- Cleanup services, Micrometer counters, and Spring Boot auto-configuration.
- Deterministic and PostgreSQL/Kafka Testcontainers acceptance tests.
