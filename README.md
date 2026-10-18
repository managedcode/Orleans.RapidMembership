<img src="https://raw.githubusercontent.com/managedcode/Orleans.RapidMembership/main/logo.png" alt="ManagedCode Orleans Rapid Membership" width="300" />

# ManagedCode.Orleans.RapidMembership

[![CI](https://github.com/managedcode/Orleans.RapidMembership/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/managedcode/Orleans.RapidMembership/actions/workflows/ci.yml)
[![Release](https://github.com/managedcode/Orleans.RapidMembership/actions/workflows/release.yml/badge.svg?branch=main)](https://github.com/managedcode/Orleans.RapidMembership/actions/workflows/release.yml)
[![CodeQL](https://github.com/managedcode/Orleans.RapidMembership/actions/workflows/codeql-analysis.yml/badge.svg?branch=main)](https://github.com/managedcode/Orleans.RapidMembership/actions/workflows/codeql-analysis.yml)
[![NuGet](https://img.shields.io/nuget/v/ManagedCode.Orleans.RapidMembership.svg?style=flat-square&logo=nuget)](https://www.nuget.org/packages/ManagedCode.Orleans.RapidMembership)
[![Downloads](https://img.shields.io/nuget/dt/ManagedCode.Orleans.RapidMembership.svg?style=flat-square)](https://www.nuget.org/packages/ManagedCode.Orleans.RapidMembership)
[![License](https://img.shields.io/github/license/managedcode/Orleans.RapidMembership?style=flat-square)](https://github.com/managedcode/Orleans.RapidMembership/blob/main/LICENSE)

Automatic, in-memory membership and gateway discovery for Microsoft Orleans 10.4 on .NET 10, based on RapidCluster.

**Development status:** the provider is being changed to discover peer addresses itself and keep all membership state in RAM. No seed IP list, advertised IP or storage directory is part of the required configuration. The fixed-seed, disk-backed provider in earlier checkpoints is superseded and has not been released to NuGet. The automatic in-memory provider is not yet qualified end to end.

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

- Discover other participants automatically on the reachable local multicast network; do not require users to know their IP addresses.
- Keep the membership table and coordination state entirely in memory. A complete loss of all processes forms a fresh membership table on restart.
- Implement native Orleans membership and client gateway discovery. Orleans owns silo status, probing, directory, placement and grain execution.
- Follow RAPID's consistent configurations and majority recovery. Freeze an old majority and install on a new majority during table handoff; discovery expiry cannot reduce a quorum. A partition without the required quorum cannot acknowledge writes as the same cluster.
- Retain bounded requests, cancellation, authentication, memory and joined shutdown. Membership state is separate from application grain storage.

The configuration example will be published with the implemented in-memory API. The former IP/directory example has been removed because it does not satisfy this contract.

## Automatic discovery implemented so far

The current discovery module sends cluster-scoped, authenticated UDP multicast beacons on port **11113** by default. It obtains peer addresses from network responses and advertises the actual TCP service port. It stores observations only in RAM, expires departed peers and lets an external client observe without announcing itself as a server. This discovers peers on the participating multicast network; routed networks must actually carry that multicast traffic.

Native socket tests start two real TCP listeners without configuring their IPs, discover them through UDP multicast and exchange nonce bytes with both discovered endpoints. They also check foreign-secret/cluster isolation, invalid packets and expiry after a node stops. These tests qualify discovery, not the unfinished automatic membership-table integration.

## In-memory coordination implemented so far

The RAM replica implements native row snapshots, ballot promises, process-incarnation fencing and old/new configuration handoff. It recovers accepted work, rejects stale or conflicting transfers, preserves promises on repeated installation and bounds requests and retained state. A frozen configuration cannot be retargeted to skip an intermediate configuration.

These components have native serializer and state-transition tests. They are not yet connected to automatic host bootstrap and the silo/client transport; the existing adapters still use the earlier prototype. Real-process qualification of the corrected runtime remains open.

## Qualification and delivery

The primary acceptance remains actual Aspire-owned Orleans silo processes and continuing external Orleans clients: zero → ten → twenty → five, simultaneous ten-node joins/departures, complete loss and restart, cold fifty, graceful reduction/rejoin, and real grain execution on every live silo. Assertions use exact native silo identities and generations.

Original RAPID unit, network, Clockwork and Aspire suites are retained. Previous fixed-seed/durable-table results do not qualify automatic in-memory membership. CI and original artifacts determine each gate independently. No package publication or KeyLoad adoption has occurred.

All three package versions follow Orleans major/minor and the ManagedCode patch, initially **10.4.0**. CI owns verification. Release independently builds, packs and pushes the packages without waiting for CI or feed indexing. The version tag belongs to that completed release.

See [architecture](https://github.com/managedcode/Orleans.RapidMembership/blob/main/docs/Architecture.md), [requirements](https://github.com/managedcode/Orleans.RapidMembership/blob/main/docs/Features/RapidMembership.md), [testing](https://github.com/managedcode/Orleans.RapidMembership/blob/main/docs/Testing.md), [contributing](https://github.com/managedcode/Orleans.RapidMembership/blob/main/CONTRIBUTING.md) and the [current work item](https://github.com/managedcode/Orleans.RapidMembership/blob/main/tasks/in-progress/001_OrleansRapidMembership.md).
