# ManagedCode.Orleans.RapidMembership

This repository follows [Managed Code AI Framework (MCAF)](https://mcaf.managed-code.com/). Root rules apply throughout the repository; project-local `AGENTS.md` files describe their actual ownership. User instructions control scope and authorization.

## Product and ownership

This is the ManagedCode fork of Reuben Bond's RapidCluster. Preserve the upstream MIT license, copyright, NOTICE, protocol implementation and original test suites. Do not relabel third-party source as an independent ManagedCode invention. Clockwork is an upstream Git submodule; do not modify it to obtain green results.

Microsoft Orleans owns silo lifecycle, status transitions, probes, placement, directory and grain execution. The provider implements native `IMembershipTable` and `IGatewayListProvider`. RAPID observes the fixed seed voters; it never supplies atomic membership-table CAS or replaces the Orleans runtime.

- `src/ManagedCode.Orleans.RapidMembership/`: provider, durable voter storage, majority consensus, admission and hosting.
- `src/RapidCluster.Core/`: upstream RAPID protocol and native hosted lifecycle.
- `src/RapidCluster.Grpc/`: upstream protocol transport.
- `tests/ManagedCode.Orleans.RapidMembership.{AppHost,Node,Tests}/`: actual silo-process membership qualification.
- `tests/RapidCluster.*`: retained upstream unit, real-network, deterministic simulation and Aspire suites.
- `docs/Architecture.md`: canonical ownership map; `docs/Features/` owns requirements and acceptance criteria; `docs/ADR/` records decisions.
- `tasks/{todo,in-progress,done}/`: bounded work items. Move to done only after the stated acceptance and delivery gates actually pass.

## Correctness contracts

Preserve durable promises, accepted values, committed snapshots, atomic native row/version ETags, bounded mutation-outcome history and quorum intersection. A successful acknowledgement requires the actual configured voter majority. Loss of that majority cannot be repaired by changing the voter list or discovering an arbitrary IP. Never convert cancellation, corruption or ambiguous commit outcomes into success or a fabricated CAS rejection.

Validate service/cluster identity, ordered voter configuration and shared secret before admitting peer work. Keep byte, row, admission, history, time and cancellation bounds explicit. Do not log secrets or application payloads. One process exclusively owns each voter directory. Keep startup, failure and joined shutdown visible at the actual owning entry point.

Do not add consumer-side workarounds for defects in an owned library. Fix them at the owner with focused failure and healthy-follow-up tests. Do not introduce runtime fallbacks, compatibility paths, fake success, or duplicate cluster runtimes without explicit owner approval.

## Working tree and style

Work in the current checkout, preserve unrelated edits, and never stash, force-push, create copy checkouts or private isolation folders. Use `rg` for searches. Generated test state and diagnostics belong under ignored repository-owned `artifacts/`.

Use .NET 10, centrally pinned packages and the root `.editorconfig`. Use file-scoped namespaces, Allman executable blocks, braces, nullable contracts and cancellation propagation. Keep responsibilities cohesive: target 400 lines per file, 200 per type, 64 executable lines per method and nesting depth three for new provider code. Do not rewrite unrelated upstream code merely to meet a new style target. Never disable analyzers, lower severities, exclude product code from coverage or weaken assertions for a passing gate.

Repository documentation and source comments are English. Report progress in the user's language. Separate source inspection, focused tests, full build, real-process qualification, CI, package publication and consumer integration evidence. Keep public claims tied to original artifacts.

## Required checks

```sh
git submodule update --init --recursive
dotnet restore RapidCluster.slnx
dotnet format RapidCluster.slnx --no-restore --exclude src/Clockwork
dotnet format RapidCluster.slnx --no-restore --exclude src/Clockwork --verify-no-changes
dotnet build RapidCluster.slnx --no-restore --configuration Release -warnaserror
```

Every build invocation uses `-warnaserror`. Run tests through their native xUnit v3 / Microsoft.Testing.Platform entry points with `--configuration Release --no-build -- --output Detailed --report-trx`. Do not replace the retained framework or invent test counts. See [CONTRIBUTING.md](CONTRIBUTING.md) and `.github/workflows/ci.yml` for exact suites.

Focused provider tests use real native operations and HTTP/2 transports. Aspire owns real process startup, readiness, discovered endpoints, resource commands and joined cleanup. Abrupt fault tests must actually kill the target process; graceful stopping is a separate case. Keep native external clients making real nonce-checked grain calls during membership changes. Assert exact active silo identities and generations on every surviving node, eventual recovery, execution coverage and resource cleanup.

The scale gate covers zero, ten, twenty and fifty silos, abrupt and graceful departures, simultaneous joins/departures, full process loss, client reconnection and loss/restoration of a voter majority. All configured cases remain mandatory. Protocol simulations do not substitute for actual Orleans processes.

`python3 eng/test-simulations.py --workers 2` discovers the compiled instrumented inventory, executes all native cases in bounded groups, retains every theory row and reconciles original TRX names and outcomes with discovery. Missing, skipped, timed-out or failed cases fail qualification. Preserve reproducible seeds and Clockwork artifacts.

Collect native coverage: changed code must meet 80% line / 70% branch; the thirteen critical provider source files require 90% line coverage through `eng/verify-coverage.py`. Failure, cancellation, concurrent writes and recovery assertions matter more than implementation-mirroring tests. Keep Linux, Windows and macOS CI, Linux scale qualification and CodeQL enabled.

## Versions and delivery

`Directory.Build.props` is the sole package-version owner. All three packages share a version: Orleans major/minor plus the ManagedCode patch. Version increases require explicit owner authorization for that increase. Batch all confirmed defects in this repository, add their tests, review the complete batch and complete the relevant checks and publish once for the combined batch.

CI owns verification. Release runs on main changes to the canonical version file or manual dispatch, builds the three package projects and pushes their packages to NuGet. It does not depend on CI, poll feed indexing or run qualification tests; this separation is the owner's explicit instruction. A push or a local package is not delivery. Update consumers only after the published package is available; run their focused checks and preserve their own policies. Never bypass branch protections or publish an intermediate repair while known defects remain in the batch.
