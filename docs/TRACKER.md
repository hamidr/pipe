# Document Tracker

## ADRs

| # | Title | Status | Phase | Depends On | Notes |
|---|-------|--------|-------|------------|-------|
| [ADR-001](ADR-001-operator-fusion-strategy.md) | Operator Fusion Strategy | Deferred | -- | ADR-002 | Deferred in favor of connectors; revisit after real users |
| [ADR-002](ADR-002-connector-strategy.md) | Connector Strategy | Proposed | Phase 1: SSE | -- | SSE -> WebSocket -> Kafka/NATS |
| [ADR-003](ADR-003-grpc-connector.md) | gRPC Connector | Proposed | Phase 1: Server streaming source | ADR-002 | Server streaming source -> bidi -> server handlers (deferred) |

Next ADR number: 004

## PRDs

| # | Title | Status | Phase | Depends On | Notes |
|---|-------|--------|-------|------------|-------|

Next PRD number: 001
