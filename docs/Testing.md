# Testing and qualification

The owner requires automatic discovery and entirely in-memory membership. The process/consensus inventory below now uses discovered peers and RAM membership; prior fixed-seed results do not qualify the corrected runtime. The new MulticastDiscoveryTests use actual UDP discovery and TCP nonce exchanges without configured peer IPs, including isolation and disappearance.

[CONTRIBUTING](../CONTRIBUTING.md) owns executable local commands. [Membership requirements](Features/RapidMembership.md) maps native contracts to cases. CI and Release are independent: CI verifies behavior and coverage; Release builds and pushes packages without waiting for CI or feed indexing.

## Main acceptance: actual Orleans processes and clients

The principal acceptance tests use native Aspire `AddProject` resources running actual Orleans silos and external native Orleans clients. They invoke generated grain contracts over Orleans transport. They do not synthesize membership rows, replace grain execution or use an in-memory test cluster as evidence of process recovery.

| Case | Actual behavior checked |
| --- | --- |
| `AspireMembershipTests` | Three silos form a cluster; a real external client discovers gateways and calls a grain before/after abrupt loss, coordinator restart, non-voter join and graceful departure. |
| `FiftyRealSilosRecoverFromZeroAndConcurrentChurnWithContinuousClients` | Zero → ten → twenty → five; return to twenty; concurrent ten joins and ten crashes; total loss; existing clients recover with ten; grow to fifty; graceful reduction to twenty-five and return to fifty. |
| `LostVoterMajorityRejectsFreshIncarnationsUntilCompleteClusterRestart` | Real coordinator crashes remove a table quorum; network reads/writes cannot acknowledge; a fresh incarnation cannot restore lost promises. Complete process loss then permits a fresh group and existing-client recovery. |
| `FiftyRealSilosStartTogetherFromZeroAndServeExternalClients` | Fifty real silos start directly from zero, agree on exact active membership and execute actual external grain calls on every live silo. |
| `RealSiloShutdownJoinsZeroExitAndRestartRecoversExternalClients` | Aspire invokes native application shutdown on a real non-voter, requires process exit code zero, then restarts it while the same external clients continue grain calls. |
| `AllCoordinatorsGracefullyLeaveTogetherAndTheirReplacementsKeepServingTheSameClients` | Three actual coordinators leave a six-silo cluster together; replacements must hold the table and the same external clients must continue before and after the departed processes return. |
| `NativeHostTests` | Public provider registration starts an actual silo and a separate Orleans client host; gateway discovery and grain calls use real transports. This in-process host test also measures public startup code coverage. |

After each live topology change, all four external clients invoke native Orleans `IManagementGrain.GetHosts(true, cancellationToken)` and require the exact expected active silo addresses and generations, with every returned status Active. Counts and original host sets are recorded per client. While all silos are stopped, management RPC must be unavailable; an empty fabricated host list cannot satisfy that phase.

Four continuing clients verify request nonces, native silo addresses, activation IDs and increasing sequences for each unchanged activation. Convergence requires exact native silo address/generation sets on every live process. Each live process must execute a grain call. A count-only match cannot pass. The zero phase drains in-flight work, checks no subsequent successes and keeps the same clients for recovery.

Scale cases allow 45 minutes for the complete churn schedule, 15 minutes for a cold fifty-silo start and 10 minutes for voter-majority recovery. Each convergence/client-progress phase allows 10 minutes. The native scale runner allows 75 minutes; its CI job allows 90 minutes including build and artifact collection. Initial automatic formation has a separate five-minute deadline. The actual test-node and external-client membership operation deadline is two minutes and the peer deadline is five seconds. HTTP state observations allow three minutes, within the ten-minute phase deadline. These deadlines preserve every membership and real grain-call assertion.

AppHost owns startup, native resource commands and shutdown. Crash commands kill the actual child executable and await its exit. Graceful stop uses an Aspire HTTP resource command that calls the test node’s native StopApplication after the response completes, waits for process exit and requires exit code zero. The test-node host allows three minutes for joined shutdown. Resource readiness precedes observations. Convergence observations run with bounded concurrency of five; every configured live process is still read and checked against the complete expected native identity set. Each session keeps RAM membership and bounded rotating native silo logs. Cleanup joins clients, log watchers and AppHost resources after success, failure or cancellation.

## Supporting evidence

Real HTTP/2 consensus tests cover concurrent CAS, twenty/fifty independent writers, majority-accepted recovery before commit notification, identity/secret rejection, unavailable quorum, bounded history and cancellation. RAM replica tests protect promises, detached snapshot content, incarnation fencing and old/new quorum state handoff. A native gRPC interceptor observes cancellation of an actual unfinished join; the same endpoint must answer a subsequent real request.

Windows, Linux and macOS each run the complete zero-to-fifty scenario in a separate GitHub job with real local silo processes and external clients. The macOS jobs use GitHub’s standard `macos-15-intel` runner with 14 GB RAM ([runner specifications](https://docs.github.com/en/actions/reference/runners/github-hosted-runners)); the full original simulation inventory and every large-cluster theory row remain mandatory. Retained upstream unit, network, Clockwork and Aspire suites are distinct gates. Each native simulation group allows thirty minutes with a thirty-one-minute external process bound; the matrix qualification job allows ninety minutes. Simulations discover every compiled case, retain all theory rows and reconcile native TRX outcomes. The runner verifies original Clockwork instrumentation manifests and application DLL hashes. Windows simulation children explicitly reserve 8 MiB stacks for managed scheduler threads, recorded in each receipt; original native stack-overflow exits remain in the earlier artifacts. No Clockwork code or case inventory is changed. The native MTP entry remains outside Clockwork's rewrite boundary as required by its build integration. Simulated faults do not substitute for actual silo/client tests.

## Coverage

`eng/verify-coverage.py` reads the original native Cobertura report for `ManagedCode.Orleans.RapidMembership`. The whole assembly requires at least 80% line and 70% branch coverage, including its generated code. Every critical source listed in `eng/verify-coverage.py` requires 90% line coverage. The gate merges line observations for compiler-generated state machines by original source line and retains the input report SHA-256 in `coverage-gate.json`; it removes no executable product code.

The native collector configuration enables static managed instrumentation with automatic restoration so small native API members are instrumented before JIT inlining. It adds no source exclusions and changes no threshold. Original reports remain the gate input.

The provider percentage is not the coverage of the upstream protocol, transport, test nodes or an entire distributed process tree. CI retains separate original unit/network reports. Child silo execution is proved by process observations and real grain responses; a parent-process coverage report alone does not establish it.

## Artifacts and limits

CI retains `qualification-ubuntu-latest`, `qualification-windows-latest`, `qualification-macos-15-intel` and `qualification-scale-ubuntu-latest`, `qualification-scale-windows-latest`, `qualification-scale-macos-15-intel` artifacts with original TRX, coverage, simulation inventory/receipts and native phase/log evidence. A failed, cancelled, incomplete or skipped case does not qualify a gate. Local focused reports and platform CI are reported separately.

Process-kill restart proves the tested process contract. It does not establish arbitrary power-loss durability, filesystem behavior on an untested deployment, performance improvement or production readiness. Use original report/run identities when stating a result.

The real-silo test profile uses five-second native Orleans probes and table refresh, and thirty-second heartbeat publication. The former one-second test profile caused live silos to be declared dead under Linux runner pressure. Native failure detection remains enabled; exact process identity, membership, abrupt loss, recovery and client-progress assertions remain mandatory.
