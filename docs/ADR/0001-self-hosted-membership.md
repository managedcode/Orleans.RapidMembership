# ADR 0001: Automatic, entirely in-memory Orleans membership

Status: Owner-approved requirements; runtime integration and partition decision pending.

## Context

The required ManagedCode fork of Reuben Bond's RapidCluster replaces external Azure membership for Orleans 10.4. Deployments do not know peer IPs and provide no membership directories. The earlier fixed-seed, disk-backed prototype does not satisfy those requirements and has not been published as a NuGet release.

## Decision

Discover participants automatically through local-network UDP multicast and learn their addresses from actual responses. Keep the membership table, observations and coordination state entirely in RAM. Require no seed IP list, advertised IP or storage directory; do not preserve the disk-backed prototype as another runtime mode.

Expose native IMembershipTable and IGatewayListProvider contracts. Microsoft Orleans continues to own silo statuses, probes, directory, placement and grain execution. Keep authentication, cluster identity, bounded memory/requests, cancellation and joined cleanup. Total process loss creates fresh membership on restart; it does not preserve application grain data.

## Open decision

Specify whether network partitions preserve one consistent table by requiring a majority, or permit isolated surviving parts to operate as separate clusters. This determines reconfiguration and whether any arbitrary five survivors of twenty can continue. Discovery alone cannot safely provide both guarantees. Do not weaken atomic native CAS or implement a partition policy without resolving this choice.

## Evidence

Two native socket tests prove multicast peer discovery, actual TCP nonce exchange using discovered addresses, client observation, secret/cluster isolation, packet validation and departure expiry. The discovery component is not yet connected to the native membership adapters. Prior fixed-voter process and coverage results do not qualify the corrected provider. See the [work item](../../tasks/in-progress/001_OrleansRapidMembership.md) for current remaining gates.
