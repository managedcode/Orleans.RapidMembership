# ADR-0002: Native TimeProvider qualification

## Decision

The owner selected System.TimeProvider and Microsoft.Extensions.TimeProvider.Testing FakeTimeProvider for provider timing qualification on 2026-10-19. Runtime DI defaults to TimeProvider.System; tests can register their own TimeProvider. Discovery timestamps and expiry, bootstrap observation, refresh and retry intervals, operation deadlines and peer cancellation use the supplied clock. Native RAPID receives the same clock through its existing TimeProvider API.

Actual UDP and HTTP/2 tests exercise time boundaries and cancellation with healthy follow-up. Aspire-owned Orleans silo processes, four continuing external clients, native IManagementGrain host sets and nonce-checked grain execution remain primary acceptance. TimeProvider controls time; it does not promise exhaustive thread-interleaving or race exploration.

## Upstream preservation

The unchanged upstream Clockwork suite remains in RapidCluster.UpstreamSimulation.slnx for supplemental research. Its 423 original cases and pinned submodule are preserved. It is absent from the main solution and provider CI. Earlier Windows native Out of memory failures and MCP allocation captures identify a Clockwork race-tracker resource limitation; those failed results remain failed and are not represented as provider qualification.

The package projects and published runtime have no Clockwork dependency. Native compiler/analyzer gates, coverage thresholds, real protocol tests and all configured Orleans scale cases remain required. Release still only builds, packs and pushes; feed availability is verified separately before consumer adoption.
