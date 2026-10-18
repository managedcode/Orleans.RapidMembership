<img src="https://raw.githubusercontent.com/managedcode/Orleans.RapidMembership/main/logo.png" alt="ManagedCode Orleans Rapid Membership" width="300" />

# ManagedCode.Orleans.RapidMembership

[![CI](https://github.com/managedcode/Orleans.RapidMembership/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/managedcode/Orleans.RapidMembership/actions/workflows/ci.yml)
[![Release](https://github.com/managedcode/Orleans.RapidMembership/actions/workflows/release.yml/badge.svg?branch=main)](https://github.com/managedcode/Orleans.RapidMembership/actions/workflows/release.yml)
[![CodeQL](https://github.com/managedcode/Orleans.RapidMembership/actions/workflows/codeql-analysis.yml/badge.svg?branch=main)](https://github.com/managedcode/Orleans.RapidMembership/actions/workflows/codeql-analysis.yml)
[![NuGet](https://img.shields.io/nuget/v/ManagedCode.Orleans.RapidMembership.svg?style=flat-square&logo=nuget)](https://www.nuget.org/packages/ManagedCode.Orleans.RapidMembership)
[![Downloads](https://img.shields.io/nuget/dt/ManagedCode.Orleans.RapidMembership.svg?style=flat-square)](https://www.nuget.org/packages/ManagedCode.Orleans.RapidMembership)
[![License](https://img.shields.io/github/license/managedcode/Orleans.RapidMembership?style=flat-square)](https://github.com/managedcode/Orleans.RapidMembership/blob/main/LICENSE)

Self-hosted membership tables and dynamic gateway discovery for Microsoft Orleans 10.4 on .NET 10, without Azure Storage or an external database.

**Based on [RapidCluster by Reuben Bond](https://github.com/ReubenBond/RapidCluster).** This repository is a fork. The original RAPID implementation, protocol tests and Clockwork simulations are retained. Original MIT copyright and license are preserved in [LICENSE](https://github.com/managedcode/Orleans.RapidMembership/blob/main/LICENSE); [NOTICE](https://github.com/managedcode/Orleans.RapidMembership/blob/main/NOTICE) identifies the ManagedCode additions. The [RAPID protocol documentation](https://github.com/managedcode/Orleans.RapidMembership/blob/main/docs/RAPID.md) describes the underlying peer discovery and observation protocol.

## Packages

| Package | Version | Downloads | Purpose |
| --- | --- | --- | --- |
| [ManagedCode.Orleans.RapidMembership](https://www.nuget.org/packages/ManagedCode.Orleans.RapidMembership) | ![Version](https://img.shields.io/nuget/v/ManagedCode.Orleans.RapidMembership.svg?style=flat-square) | ![Downloads](https://img.shields.io/nuget/dt/ManagedCode.Orleans.RapidMembership.svg?style=flat-square) | Orleans membership table, silo hosting and client gateway discovery. |
| [ManagedCode.Orleans.RapidMembership.Protocol](https://www.nuget.org/packages/ManagedCode.Orleans.RapidMembership.Protocol) | ![Version](https://img.shields.io/nuget/v/ManagedCode.Orleans.RapidMembership.Protocol.svg?style=flat-square) | ![Downloads](https://img.shields.io/nuget/dt/ManagedCode.Orleans.RapidMembership.Protocol.svg?style=flat-square) | The original RAPID discovery and observation protocol. |
| [ManagedCode.Orleans.RapidMembership.Grpc](https://www.nuget.org/packages/ManagedCode.Orleans.RapidMembership.Grpc) | ![Version](https://img.shields.io/nuget/v/ManagedCode.Orleans.RapidMembership.Grpc.svg?style=flat-square) | ![Downloads](https://img.shields.io/nuget/dt/ManagedCode.Orleans.RapidMembership.Grpc.svg?style=flat-square) | HTTP/2 gRPC transport for the RAPID protocol. |

The provider brings its protocol and transport dependencies transitively. Badges report the public feed and GitHub workflow state; they are not a claim that an unpublished version is available.

[Architecture](https://github.com/managedcode/Orleans.RapidMembership/blob/main/docs/Architecture.md) · [Testing and coverage](https://github.com/managedcode/Orleans.RapidMembership/blob/main/docs/Testing.md) · [Membership requirements](https://github.com/managedcode/Orleans.RapidMembership/blob/main/docs/Features/RapidMembership.md) · [Design decision](https://github.com/managedcode/Orleans.RapidMembership/blob/main/docs/ADR/0001-self-hosted-membership.md) · [Contributing](https://github.com/managedcode/Orleans.RapidMembership/blob/main/CONTRIBUTING.md) · [Repository rules](https://github.com/managedcode/Orleans.RapidMembership/blob/main/AGENTS.md)

## Contract

RAPID discovers and observes the configured seed voters from explicit IP endpoints. Ordinary silos register through the native Orleans membership table and do not join the RAPID voting group. Orleans continues to own silo statuses, peer probes, activation placement and the grain directory. Reachability alone never authorizes membership: every peer exchange validates the service/cluster identity, voter configuration and shared secret.

Orleans requires atomic row/version compare-and-swap, which a RAPID peer view alone does not provide. This provider hosts a durable majority Paxos membership table inside the seed silo processes. No permanently designated table leader is required: any caller can coordinate a proposal. Each voter records its promise and accepted value on its own disk before acknowledging. Reads cross a quorum barrier; writes atomically change the native row and table ETags. Heartbeats preserve the table version. Concurrent CAS attempts on one table version have one winner.

The fixed, ordered seed list defines the table voters. Three voters tolerate one unavailable voter; five tolerate two. Other silos join dynamically without becoming voters. A 20-to-5 abrupt reduction can remain available when the surviving five include a majority of the configured voters; losing a voter majority cannot be made safe by discovering more IPs. Loss of a majority prevents table reads and writes from succeeding. Membership does not persist application grain state.

## Configure a silo

```sh
dotnet add package ManagedCode.Orleans.RapidMembership --version 10.4.0
```

```csharp
using ManagedCode.Orleans.RapidMembership;
using Orleans.Configuration;

builder.Host.UseOrleans(silo =>
{
    silo.Configure<ClusterOptions>(cluster =>
    {
        cluster.ServiceId = "my-service";
        cluster.ClusterId = "production";
    });
    silo.UseRapidMembership(options =>
    {
        options.ListenEndpoint = "http://10.0.0.1:11112";
        options.SeedEndpoints = [
            "http://10.0.0.1:11112",
            "http://10.0.0.2:11112",
            "http://10.0.0.3:11112"];
        options.StorageDirectory = "/var/lib/my-service/membership";
        options.SharedSecret = configuration["MembershipSecret"]!;
    });
});
```

Each voter advertises its own reachable literal IP and membership port. Ordinary silos identify their role with an endpoint outside the seed list and bind only their native Orleans and application endpoints. All participants use the same ordered seed list, service ID, cluster ID and a secret of at least 32 UTF-8 bytes. A single voter is supported for development; it has no voter-failure tolerance. An additional non-voter silo uses its own `ListenEndpoint` and the existing seed list.

Each configured voter binds its own HTTP/2 membership socket before Orleans initializes its table. Non-voters validate availability against the same durable quorum without starting another RAPID participant. Voters use RAPID's native hosted lifecycle for advisory discovery; the durable table starts when its configured storage quorum is available, independently of a stale RAPID observation view. No endpoint mapping is required in the application's web server. The default membership port is **TCP 11112**. Orleans silo-to-silo traffic (commonly TCP 11111) and client gateway traffic (commonly TCP 30000) remain separate. Allow bidirectional connectivity between participants on their configured ports. HTTP/2 traffic must run on a trusted private network or an encrypted network tunnel; shared-secret admission does not encrypt traffic. No certificate-validation bypass is installed.

There is no IP-range scan, broadcast, implicit merging of unrelated clusters or public Internet discovery. At least one seed must be reachable for RAPID join, and a majority of seed membership voters must be reachable for table operations.

## Configure an external Orleans client

```csharp
builder.UseOrleansClient(client =>
{
    client.Configure<ClusterOptions>(cluster =>
    {
        cluster.ServiceId = "my-service";
        cluster.ClusterId = "production";
    });
    client.UseRapidMembership(options =>
    {
        options.SeedEndpoints = [
            "http://10.0.0.1:11112",
            "http://10.0.0.2:11112",
            "http://10.0.0.3:11112"];
        options.SharedSecret = configuration["MembershipSecret"]!;
    });
});
```

Clients discover only active native Orleans gateway rows. They do not start a membership listener or become voters.

## Operations and failure boundaries

- Preserve every voter's `StorageDirectory` across process restarts. One process exclusively owns each directory. Corrupt state, a changed voter configuration or a different cluster identity fails closed.
- Seed/voter reconfiguration is not supported online. Never change the voter list or delete/restore a voter directory while any participant from that cluster can still run. Start a separate cluster identity for a deliberately new deployment.
- Local writes use flushed files and atomic replacement. Process-kill recovery and loss of one voter are qualification targets; they do not establish power-loss durability for arbitrary disks or filesystems.
- `OperationTimeout`, `PeerTimeout`, `MaximumSnapshotBytes`, `MaximumEntries`, `MaximumOperationHistory` and `MaximumAdmissions` bound work. Cancellation stops observation and remaining exchanges; a timeout after quorum acceptance can leave a committed result, as with other distributed CAS providers. Read the table before deciding how to retry.
- Mutation outcomes are retained in a bounded durable history (4,096 by default) so an internal retry can settle an already chosen write. If concurrent traffic evicts an unresolved outcome, the operation reports an indeterminate result and requires a fresh native row/version read; it never reports an invented CAS failure. Reads repair a committed snapshot to a majority and recover pending accepted proposals before returning when necessary.
- RAPID's initial bootstrap coordinator is the first configured seed. It is not the membership-table leader. Table consensus has a separate fixed voter quorum, so a stale or divided RAPID view cannot create two independently writable tables.

## Build and qualification

```sh
git submodule update --init --recursive
dotnet restore RapidCluster.slnx
dotnet format RapidCluster.slnx --exclude src/Clockwork --verify-no-changes
dotnet build RapidCluster.slnx --no-restore --configuration Release -warnaserror
dotnet run --project tests/ManagedCode.Orleans.RapidMembership.Tests --configuration Release --no-build -- --filter-not-trait Category=Scale --timeout 600s
```

The new tests cover real network CAS contention, recovery of a majority-accepted write before commit notification, unavailable quorum cancellation, identity/secret isolation and durable acceptor restart. Aspire runs three actual Orleans silo processes, a native Orleans client, grain calls, abrupt process loss and rejoin. An independent cold-start case forms all fifty real silos from zero and checks external grain calls on every silo. A separate real-process scale scenario starts from zero and exercises 10, 20, 5, concurrent ten-node joins and ten-node crashes, total cluster loss, recovery to 10, growth to 50, graceful reduction to 25 and return to 50. Four external Orleans clients keep issuing nonce-checked grain calls throughout; each live silo must agree on the exact active membership set and execute a grain call. Zero-node periods require bounded failures and subsequent recovery by the same clients. Run this gate with `dotnet run --project tests/ManagedCode.Orleans.RapidMembership.Tests --configuration Release --no-build -- --filter-trait Category=Scale --timeout 4500s --output Detailed --report-trx --results-directory artifacts/tests/churn`. Linux CI runs this gate independently of Release. Upstream unit, network, simulation and Aspire suites remain separate gates. `python3 eng/test-simulations.py --workers 2` executes the complete compiled simulation inventory in bounded native MTP processes and reconciles every original TRX result with discovery; cases are neither skipped nor reduced. CI artifacts, rather than this test inventory, establish which gates passed for a release.

Version `major.minor.patch` tracks the Orleans major/minor line; the patch is the ManagedCode library revision. All three packages (`ManagedCode.Orleans.RapidMembership`, `.Protocol`, `.Grpc`) share one release version. CI runs qualification separately. Release builds and pushes the three packages directly to NuGet when the canonical version file changes on main or through manual dispatch; it does not wait for CI or poll NuGet indexing.
