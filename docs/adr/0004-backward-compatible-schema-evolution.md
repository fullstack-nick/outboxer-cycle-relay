# ADR 0004: Evolve the cycle contract with an explicit v2 schema

- Status: Accepted
- Date: 2026-09-04

## Context

The final requirement adds energy consumption after v1 already has reliability evidence. Editing the v1 schema in place would make previously invalid bytes valid and would erase the ability to reason about what a declared version means.

## Decision

Keep v1 wire schemas unchanged. Add separate source and canonical v2 schemas that permit optional, non-negative `energyConsumptionWh`. Select validation by the declared `schemaVersion` and reject unsupported versions or unknown fields.

Add a nullable, checked PostgreSQL column through Flyway. Keep the existing `/api/v1` endpoint shapes stable by always exposing `energyConsumptionWh`: `null` for v1 or absent v2 data, numeric when v2 supplies it. Configure the simulator to emit a deterministic version percentage.

## Consequences

- Existing v1 senders and stored rows remain valid without modification.
- Consumers can distinguish wire capabilities from field presence.
- Mixed-version traffic is testable in one run.
- Each new wire version requires an explicit schema and compatibility suite.
- The HTTP representation is intentionally broader than the v1 wire payload.

## Alternatives considered

- Adding the field to v1 was rejected because it mutates an established strict contract.
- Making the database column non-null was rejected because existing v1 rows have no value.
- Creating a new HTTP endpoint version was rejected because this additive nullable representation does not break current response consumers.
