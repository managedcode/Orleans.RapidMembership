# ADR 0001: Automatic, entirely in-memory Orleans membership

Status: Owner-approved automatic RAM requirements and RAPID consistency decision; runtime integration implemented, final qualification in progress.

## Context

The required ManagedCode fork of Reuben Bond's RapidCluster replaces external Azure membership for Orleans 10.4. Deployments do not know peer IPs and provide no membership directories. The earlier fixed-seed, disk-backed prototype does not satisfy those requirements and has not been published as a NuGet release.

## Decision

Discover participants through local-network UDP multicast/broadcast and optional bounded IPv4 subnet/range probes; learn addresses from actual responses. Keep the membership table, observations and coordination state entirely in RAM. Require no seed IP list, advertised IP or storage directory; do not preserve the disk-backed prototype as another runtime mode.

Expose native IMembershipTable and IGatewayListProvider contracts. Microsoft Orleans continues to own silo statuses, probes, directory, placement and grain execution. Keep authentication, cluster identity, bounded memory/requests, cancellation and joined cleanup. Total process loss creates fresh membership on restart; it does not preserve application grain data.

## Partition and handoff decision

The owner requested the correct RAPID protocol behavior. Preserve one consistent configuration: use RAPID's fast consensus path and classic majority recovery, and require a majority of the old and new coordination configurations during membership-table handoff. Freeze old table proposals before recovering committed/accepted work and installing it in the new configuration. Identify each process incarnation separately; RAM loss cannot preserve a previous vote. Discovery expiry never authorizes a smaller voting group. A partition without the required quorum waits instead of acknowledging independent writes as the same cluster.

These requirements follow sections 3-5 of the [original RAPID paper](https://www.usenix.org/system/files/conference/atc18/atc18-suresh.pdf). RAPID agrees membership views; the provider's separate atomic Orleans table needs the explicit state handoff. Up to three discovered silos form the cohosted coordination group. This trades resilience for overhead; it does not provide arbitrary-survivor availability. Automatic multicast bootstrap is an extension, not an original RAPID guarantee of global discovery across arbitrary networks. Detecting and resolving independently formed cold-start groups remains an explicit qualification concern.

## Evidence

Native socket tests prove multicast peer discovery, actual TCP nonce exchange using discovered addresses, client observation, secret/cluster isolation, packet validation and departure expiry. Discovery, RAM replicas and handoff are connected to native membership hosting. Native integration found and repaired wildcard address advertisement and lost metadata during RAPID rejoin. Platform, coverage and real-process qualification outcomes are recorded in original CI artifacts and the tracked work item. Each old replica grants only one successor vote. A validated old-majority certificate selects the successor; a minority freeze cannot prevent that replica learning the majority decision. A majority-frozen transfer must complete before a later transition. Installed configurations and newer ballot promises cannot be bypassed by delayed requests. Prior fixed-voter process and coverage results do not qualify the corrected provider. See the [work item](../../tasks/in-progress/001_OrleansRapidMembership.md) for current remaining gates.
