# test_observer_overload

## Purpose
Verifies bounded exploratory-query admission and API recovery on a readonly observer.

## Tests (1)
- `test_observer_exploratory_overload_recovers` — occupies the exploratory executor, sends eight excess requests, and verifies fail-fast overload responses followed by a successful query.

## Setup
A fresh three-validator shard with one readonly observer.

## Key assertions
- Excess requests return HTTP 503 with the `observer_busy` error kind.
- The observer remains ready while rejecting overload.
- A normal exploratory query succeeds after the slow query releases the permit.

## Infrastructure used
`Shard.create`, the observer HTTP API, and `/api/status`.
