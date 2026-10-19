# ManagedCode.Orleans.RapidMembership

This repository follows [Managed Code AI Framework (MCAF)](https://mcaf.managed-code.com/). Root rules apply throughout the repository; project-local `AGENTS.md` files describe their actual ownership. User instructions control scope and authorization.

## Product and ownership

This is the ManagedCode fork of Reuben Bond's RapidCluster. Preserve the upstream MIT license, copyright, NOTICE, protocol implementation and original test suites. Do not relabel third-party source as an independent ManagedCode invention. Clockwork is an upstream Git submodule; do not modify it to obtain green results.

Microsoft Orleans owns silo lifecycle, status transitions, probes, placement, directory and grain execution. The provider implements native `IMembershipTable` and `IGatewayListProvider`. Peers discover one another automatically through local-network UDP multicast; require no seed IP list, advertised IP or storage directory. All membership and coordination state is in memory. RAPID observation does not by itself supply atomic membership-table CAS or replace the Orleans runtime.

- `src/ManagedCode.Orleans.RapidMembership/`: provider, automatic discovery, in-memory coordination, admission and hosting.
- `src/RapidCluster.Core/`: upstream RAPID protocol and native hosted lifecycle.
- `src/RapidCluster.Grpc/`: upstream protocol transport.
- `tests/ManagedCode.Orleans.RapidMembership.{AppHost,Node,Tests}/`: actual silo-process membership qualification.
- `tests/RapidCluster.*`: retained upstream unit, real-network, deterministic simulation and Aspire suites.
- `docs/Architecture.md`: canonical ownership map; `docs/Features/` owns requirements and acceptance criteria; `docs/ADR/` records decisions.
- `tasks/{todo,in-progress,done}/`: bounded work items. Move to done only after the stated acceptance and delivery gates actually pass.

## Correctness contracts

Preserve atomic native row/version ETags, truthful operation outcomes, bounded history and isolation. A new process incarnation cannot reuse lost coordination promises as though it retained them. Total process loss loses the in-memory membership table. Follow RAPID's consistent-configuration protocol: require a majority for classic consensus and both old and new majorities for table handoff. Never shrink a quorum from discovery expiry, silently restart an isolated minority as the same cluster, or let a fresh incarnation reuse forgotten promises. Arbitrary survivors cannot continue without the required coordination quorum.

Validate service/cluster identity and the shared secret before admitting peer work. Keep byte, row, admission, history, time, cancellation and discovery-peer bounds explicit. Do not log secrets or application payloads. No membership state files or directory configuration are permitted. Test reports and diagnostics remain ordinary ignored artifacts. Keep startup, failure and joined shutdown visible at their actual entry points.

Do not add consumer-side workarounds for defects in an owned library. Fix them at the owner with focused failure and healthy-follow-up tests. Do not introduce runtime fallbacks, compatibility paths, fake success, or duplicate cluster runtimes without explicit owner approval.

## Working tree and style

Work in the current checkout, preserve unrelated edits, and never stash, force-push, create copy checkouts or private isolation folders. Use `rg` for searches. Generated test state and diagnostics belong under ignored repository-owned `artifacts/`.

Use .NET 10, centrally pinned packages and the root `.editorconfig`. Use file-scoped namespaces, Allman executable blocks, braces, nullable contracts and cancellation propagation. Keep responsibilities cohesive: target 400 lines per file, 200 per type, 64 executable lines per method and nesting depth three for new provider code. Do not rewrite unrelated upstream code merely to meet a new style target. Never disable analyzers, lower severities, exclude product code from coverage or weaken assertions for a passing gate.

Repository documentation and source comments are English. Report progress in the user's language. Separate source inspection, focused tests, full build, real-process qualification, CI, package publication and consumer integration evidence. Keep public claims tied to original artifacts.

## Required checks

```sh
dotnet restore RapidCluster.slnx
dotnet format RapidCluster.slnx --no-restore
dotnet format RapidCluster.slnx --no-restore --verify-no-changes
dotnet build RapidCluster.slnx --no-restore --configuration Release -warnaserror
```

Every build invocation uses `-warnaserror`. Run tests through their native xUnit v3 / Microsoft.Testing.Platform entry points with `--configuration Release --no-build -- --output Detailed --report-trx`. Do not replace the retained framework or invent test counts. See [CONTRIBUTING.md](CONTRIBUTING.md) and `.github/workflows/ci.yml` for exact suites.

Focused provider tests use real native operations and HTTP/2 transports. Aspire owns real process startup, readiness, discovered endpoints, resource commands and joined cleanup. Abrupt fault tests must actually kill the target process; graceful stopping is a separate case. Keep native external clients making real nonce-checked grain calls during membership changes. Assert exact active silo identities and generations on every surviving node, eventual recovery, execution coverage and resource cleanup. After each live topology change, each external client must call native Orleans IManagementGrain.GetHosts(true) and verify the exact expected active address/generation set. With no running silos, management RPC must be unavailable.

The scale gate covers zero, ten, twenty and thirty silos, abrupt and graceful departures, simultaneous joins/departures, full process loss, client reconnection and loss/restoration of a voter majority. All configured cases remain mandatory. Protocol simulations do not substitute for actual Orleans processes.

Use native System.TimeProvider for provider time, timers and cancellation deadlines. Inject it through DI; TimeProvider.System is the runtime default and Microsoft.Extensions.TimeProvider.Testing supplies FakeTimeProvider in tests. Real UDP/HTTP2 tests verify expiration boundaries, cancelled work and healthy follow-up. Actual Orleans process/client tests remain the primary acceptance gate. The owner selected this qualification path instead of Clockwork. Preserve the original, unchanged upstream simulation suite separately in RapidCluster.UpstreamSimulation.slnx; its results are supplemental and never qualify the provider or replace actual silo execution. No Clockwork dependency belongs in the main solution or provider CI.

Collect native coverage: changed code must meet 80% line / 70% branch; the critical provider source files require 90% line coverage through `eng/verify-coverage.py`. Failure, cancellation, concurrent writes and recovery assertions matter more than implementation-mirroring tests. Keep Linux, Windows and macOS CI, full zero-to-thirty scale qualification on each of those three operating systems, and CodeQL enabled.

## Versions and delivery

`Directory.Build.props` is the sole package-version owner. All three packages share a version: Orleans major/minor plus the ManagedCode patch. Version increases require explicit owner authorization for that increase. Batch all confirmed defects in this repository, add their tests, review the complete batch and complete the relevant checks and publish once for the combined batch.

CI owns verification. Release runs on main changes to the canonical version file or manual dispatch, builds the three package projects and pushes their packages to NuGet. It does not depend on CI, poll feed indexing or run qualification tests; this separation is the owner's explicit instruction. A push or a local package is not delivery. Update consumers only after the published package is available; run their focused checks and preserve their own policies. Never bypass branch protections or publish an intermediate repair while known defects remain in the batch.
