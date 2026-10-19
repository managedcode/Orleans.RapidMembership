<img src="https://raw.githubusercontent.com/managedcode/Orleans.RapidMembership/main/logo.png" alt="ManagedCode Orleans Rapid Membership" width="300" />

# ManagedCode.Orleans.RapidMembership

[![CI](https://github.com/managedcode/Orleans.RapidMembership/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/managedcode/Orleans.RapidMembership/actions/workflows/ci.yml)
[![Release](https://github.com/managedcode/Orleans.RapidMembership/actions/workflows/release.yml/badge.svg?branch=main)](https://github.com/managedcode/Orleans.RapidMembership/actions/workflows/release.yml)
[![CodeQL](https://github.com/managedcode/Orleans.RapidMembership/actions/workflows/codeql-analysis.yml/badge.svg?branch=main)](https://github.com/managedcode/Orleans.RapidMembership/actions/workflows/codeql-analysis.yml)
[![NuGet](https://img.shields.io/nuget/v/ManagedCode.Orleans.RapidMembership.svg?style=flat-square&logo=nuget)](https://www.nuget.org/packages/ManagedCode.Orleans.RapidMembership)
[![Downloads](https://img.shields.io/nuget/dt/ManagedCode.Orleans.RapidMembership.svg?style=flat-square)](https://www.nuget.org/packages/ManagedCode.Orleans.RapidMembership)
[![License](https://img.shields.io/github/license/managedcode/Orleans.RapidMembership?style=flat-square)](https://github.com/managedcode/Orleans.RapidMembership/blob/main/LICENSE)

Automatic, in-memory membership and gateway discovery for Microsoft Orleans 10.4 on .NET 10, based on RapidCluster.

**Verification and delivery:** actual Orleans silo/client scenarios, protocol/network suites and native coverage gates run in [CI](https://github.com/managedcode/Orleans.RapidMembership/actions/workflows/ci.yml). The NuGet badge reports the published version; [testing requirements](https://github.com/managedcode/Orleans.RapidMembership/blob/main/docs/Testing.md) describe the scenarios and qualification scope.

**Based on [RapidCluster by Reuben Bond](https://github.com/ReubenBond/RapidCluster).** This repository is a fork. The original RAPID implementation, protocol tests and Clockwork simulations are retained. Original MIT copyright and license are preserved in [LICENSE](https://github.com/managedcode/Orleans.RapidMembership/blob/main/LICENSE); [NOTICE](https://github.com/managedcode/Orleans.RapidMembership/blob/main/NOTICE) identifies the ManagedCode additions. The [RAPID protocol documentation](https://github.com/managedcode/Orleans.RapidMembership/blob/main/docs/RAPID.md) describes the underlying peer discovery and observation protocol.

## Packages

| Package | Version | Downloads | Purpose |
| --- | --- | --- | --- |
| [ManagedCode.Orleans.RapidMembership](https://www.nuget.org/packages/ManagedCode.Orleans.RapidMembership) | ![Version](https://img.shields.io/nuget/v/ManagedCode.Orleans.RapidMembership.svg?style=flat-square) | ![Downloads](https://img.shields.io/nuget/dt/ManagedCode.Orleans.RapidMembership.svg?style=flat-square) | Orleans membership table, silo hosting and client gateway discovery. |
| [ManagedCode.Orleans.RapidMembership.Protocol](https://www.nuget.org/packages/ManagedCode.Orleans.RapidMembership.Protocol) | ![Version](https://img.shields.io/nuget/v/ManagedCode.Orleans.RapidMembership.Protocol.svg?style=flat-square) | ![Downloads](https://img.shields.io/nuget/dt/ManagedCode.Orleans.RapidMembership.Protocol.svg?style=flat-square) | The original RAPID discovery and observation protocol. |
| [ManagedCode.Orleans.RapidMembership.Grpc](https://www.nuget.org/packages/ManagedCode.Orleans.RapidMembership.Grpc) | ![Version](https://img.shields.io/nuget/v/ManagedCode.Orleans.RapidMembership.Grpc.svg?style=flat-square) | ![Downloads](https://img.shields.io/nuget/dt/ManagedCode.Orleans.RapidMembership.Grpc.svg?style=flat-square) | HTTP/2 gRPC transport for the RAPID protocol. |

The provider brings its protocol and transport dependencies transitively. Badges report the public feed and GitHub workflow state; they are not a claim that an unpublished version is available.

[Architecture](https://github.com/managedcode/Orleans.RapidMembership/blob/main/docs/Architecture.md) · [Testing and coverage](https://github.com/managedcode/Orleans.RapidMembership/blob/main/docs/Testing.md) · [Membership requirements](https://github.com/managedcode/Orleans.RapidMembership/blob/main/docs/Features/RapidMembership.md) · [Design decision](https://github.com/managedcode/Orleans.RapidMembership/blob/main/docs/ADR/0001-self-hosted-membership.md) · [Contributing](https://github.com/managedcode/Orleans.RapidMembership/blob/main/CONTRIBUTING.md) · [Repository rules](https://github.com/managedcode/Orleans.RapidMembership/blob/main/AGENTS.md)

## Required runtime contract

- Discover other participants through reachable local multicast/broadcast and optional bounded subnet/range probes; do not require individual peer IPs.
- Keep the membership table and coordination state entirely in memory. A complete loss of all processes forms a fresh membership table on restart.
- Implement native Orleans membership and client gateway discovery. Orleans owns silo status, probing, directory, placement and grain execution.
- Follow RAPID's consistent configurations and majority recovery. Freeze an old majority and install on a new majority during table handoff; discovery expiry cannot reduce a quorum. A partition without the required quorum cannot acknowledge writes as the same cluster.
- Retain bounded requests, cancellation, authentication, memory and joined shutdown. Membership state is separate from application grain storage.

## Silo and client configuration

Install `ManagedCode.Orleans.RapidMembership` after its initial publication. Configure the same Orleans service/cluster identity and shared secret on silos and clients:

```csharp
using ManagedCode.Orleans.RapidMembership;
using Orleans.Configuration;

builder.UseOrleans(silo =>
{
    silo.Configure<ClusterOptions>(options =>
    {
        options.ServiceId = "my-service";
        options.ClusterId = "production";
    });
    silo.UseRapidMembership(options =>
    {
        options.SharedSecret = configuration["MembershipSecret"]!;
    });
});
```

An external client uses `UseRapidMembership` on its `IClientBuilder` with the same identity and secret. The secret must contain at least 32 UTF-8 bytes and come from deployment secret configuration. No IP list, advertised address or directory is required. Each silo hosts its own private membership listener; ordinary Orleans silo/gateway endpoint configuration remains separate.

Optional settings bound directed discovery to a known subnet or inclusive IPv4 range:

```csharp
options.DiscoveryNetworks = ["192.168.1.0/24", "10.20.0.10-10.20.0.50"];
options.MaximumDiscoveryAddresses = 4096;
// Optional fixed membership listener port for firewall rules:
options.ListenPort = 11112;
```

`ListenPort` defaults to an automatically allocated TCP port, allowing multiple silos on one machine. A fixed port requires distinct host addresses or network namespaces. Configured ranges are bounded and probed in rotating batches of at most 256 addresses per second. They supplement automatic discovery; they are not seed-voter lists.

## Network scope

Discovery sends authenticated, service/cluster-scoped IPv4 multicast to **239.192.44.44:11113/UDP** on active interfaces, plus localhost/broadcast and directed UDP probes. UDP supports multicast/broadcast discovery without a known peer address; announcements are repeated, and a missed packet does not authorize membership changes. Optional CIDR/range probes also use UDP. After discovery, RAPID consensus and membership-table operations use HTTP/2 over TCP; native Orleans calls use their own TCP connections. It learns actual addresses and membership TCP ports from responses, retaining observations for 30 seconds independently of RAPID failure detection. Each discovery socket has a bounded 256 KiB receive buffer. Localhost discovery works between processes on the same host. Cross-host discovery needs reachable multicast/broadcast or an explicit CIDR/range, UDP discovery reachability and a return route. It cannot enumerate arbitrary addresses on the Internet or cross an inaccessible router/firewall.

Allow the configured discovery UDP port and the advertised membership TCP port between trusted peers. The membership listener serves HTTP/2 for both RAPID and the table protocol. Native Orleans silo and gateway ports must also be reachable under their own configuration. Identity and secret authentication protect protocol admission; HTTP/2 here does not encrypt traffic. Deploy on a trusted private network or an encrypted network overlay.

## Coordination and process lifetime

Configured discovery ranges complete their first bounded probe sweep before cold bootstrap can begin. Probes visit at most 256 configured addresses per round, with a one-second announcement cadence. Allow enough `StartupTimeout` for the full range and subsequent formation.

Three discovered silos form the steady-state native RAPID coordination group when at least three are available; other silos host discovery and serve ordinary Orleans workloads. Graceful handoff can temporarily add replacements before departing coordinators close. Cold bootstrap waits for a stable lowest process-incarnation candidate and agreement from all currently discovered peers. A learned founding candidate cannot move upward when discovery observations expire. If it disappears before forming a cluster, restart all remaining processes to begin a new formation attempt. Independently formed groups with the same service identity are rejected rather than merged with conflicting tables. Discovery is a network observation mechanism, not proof of global network connectivity.

The native RAPID protocol agrees coordinator configurations. The separate RAM ballot replica implements atomic Orleans row/version CAS. Each transition permanently freezes an old majority, recovers committed and accepted work, and installs on a new majority. A retained replica can catch up using that evidence; a restarted process has a fresh identity and cannot claim its forgotten promises. Retrying an installation preserves newer promises and table operations.

Initial formation has a separate five-minute `StartupTimeout`; ordinary table operations retain their two-minute deadline. Three coordinators tolerate loss of one while a quorum remains reachable and can recruit another discovered silo. Losing a majority stops membership reads/writes that require agreement; arbitrary surviving silos cannot recreate that majority by themselves. Graceful departure transfers state before closing its listener. All membership state disappears after complete process loss; a fresh group with a new RAPID root can then form, including on reused TCP ports. This does not persist or restore application grain data.

## Qualification and delivery

The primary acceptance remains actual Aspire-owned Orleans silo processes and continuing external Orleans clients: zero → ten → twenty → five, simultaneous ten-node joins/departures, complete loss and restart, cold thirty, graceful reduction/rejoin, and real grain execution on every live silo. Assertions use exact native silo identities and generations. After each live topology change, every external client also calls native Orleans `IManagementGrain.GetHosts(true)` and verifies the complete active set. Separate Windows, Linux and macOS GitHub jobs run the full scenario.

Original RAPID unit, network, Clockwork and Aspire suites are retained. Previous fixed-seed/durable-table results do not qualify automatic in-memory membership. Provider qualification uses native TimeProvider/FakeTimeProvider and real Orleans processes. The unchanged Clockwork suite is supplemental in RapidCluster.UpstreamSimulation.slnx. CI and original artifacts determine each native gate independently; repository work items retain qualification and delivery evidence.

All three package versions follow Orleans major/minor and the ManagedCode patch, initially **10.4.0**. CI owns verification. Release independently builds, packs and pushes the packages without waiting for CI or feed indexing. The version tag belongs to that completed release.

See [architecture](https://github.com/managedcode/Orleans.RapidMembership/blob/main/docs/Architecture.md), [requirements](https://github.com/managedcode/Orleans.RapidMembership/blob/main/docs/Features/RapidMembership.md), [testing](https://github.com/managedcode/Orleans.RapidMembership/blob/main/docs/Testing.md), [contributing](https://github.com/managedcode/Orleans.RapidMembership/blob/main/CONTRIBUTING.md) and [work items](https://github.com/managedcode/Orleans.RapidMembership/tree/main/tasks).
