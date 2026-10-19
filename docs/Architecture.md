# Architecture

The required provider uses automatic peer discovery and keeps membership entirely in memory. It requires no known IP list, advertised IP or storage directory. [Requirements](Features/RapidMembership.md) define observable acceptance; [ADR 0001](ADR/0001-self-hosted-membership.md) records the corrected design scope. Discovery, native hosting, RAM table replication and configuration handoff are wired together; final qualification remains in progress.

## Ownership

| Owner | Responsibility and current state |
| --- | --- |
| `Discovery/MulticastDiscovery` | Actual IPv4 multicast/broadcast and directed CIDR/range probes, addresses learned from replies, bounded observations, expiry and joined socket shutdown. |
| `Discovery/DiscoveryPacket` | Fixed-size authenticated packets, service/cluster isolation and validation. Implemented. |
| `RapidMembershipTable`, `RapidGatewayListProvider` | Native Orleans adapters over discovered configurations and RAM table consensus. |
| `CoordinationConfiguration`, `ReplicaConfigurationFence`, `ConfigurationHandoff` | Canonical configurations, permanent old-view barriers, accepted/committed-state recovery and old/new quorum checks. Integrated with native hosting and transport; final process/coverage qualification remains open. |
| `MemoryMembershipReplica`, `MembershipBallot` | Entirely RAM-backed serialized ballot/table ownership with incarnation fencing, bounded state, repeated-install safety and detached native messages. Integrated with native hosting and transport; final process/coverage qualification remains open. |
| Provider consensus/hosting/transport | Atomic native table operations, incarnation identity and peer admission. PeerDirectory resolves actual installed configurations; ControllerRuntime owns cold bootstrap, coordinator recruitment, native observations and joined departure. The fixed-voter/file-backed implementation was removed. |
| `RapidCluster.Core`, `RapidCluster.Grpc` | Original upstream RAPID protocol and gRPC transport, with attributed owner repairs and retained native suites. |
| Microsoft Orleans | Silo statuses, probes, directory, placement, activation lifecycle and real grain execution. |

## Discovery

A participant joins local IPv4 multicast on active interfaces and announces its process-incarnation ID and actual TCP port. Receivers use the network source address instead of accepting a configured or caller-supplied advertised IP. Beacons carry a cluster-scoped identity and authentication. A client listens without becoming an announced server. Discovery retains bounded observations in RAM and expires disappeared peers.

Discovery does not provide atomic table CAS, voter reconfiguration or proof that another process is permanently dead. Localhost/broadcast probes and optional bounded IPv4 subnet/range probes supplement multicast. Network routes and UDP/TCP reachability remain necessary; discovery cannot infer endpoints across inaccessible networks. See README for ports and configuration.

## In-memory table and partition contract

A complete process loss discards membership state; a fresh group must form on restart. Application grain persistence is separate. A restarted process has a new incarnation and cannot present lost coordination promises as retained state. During cold formation, authenticated observations and peer replies can only lower the founding candidate. Expiry cannot promote another candidate and create an independent root. Losing that candidate before formation requires restarting the remaining processes. A configured address range must complete its initial bounded probe sweep before bootstrap, so later batches are not omitted from initial discovery.

The owner selected RAPID's consistent-configuration protocol. Classic consensus requires a majority. A table transition freezes a majority of the previous configuration, preserves the highest committed/accepted work, and requires installation on a majority of the new configuration before acknowledging table operations. Discovery expiry is not a reconfiguration decision. An isolated minority cannot acknowledge operations by electing itself as the same cluster. Up to three silos coordinate; losing their majority stops quorum operations. Independently formed roots are rejected. A stable discovery interval reduces cold-start races but cannot establish global connectivity across a partition. Final zero-to-thirty process qualification remains open.

## Qualification and delivery

Aspire owns actual silo processes, resource commands and joined cleanup. Tests must require exact native address/generation sets and nonce-checked grain execution from continuing external clients across the full zero-to-thirty fault schedule. The process topology supplies a shared secret and cluster identity, plus listener ports allocated by Aspire. It supplies no seed IPs or directories.

Original RAPID simulations qualify their instrumented protocol scope, separately from actual Orleans processes. Original reports retain failures and incomplete runs. Prior fixed-seed/durable-table results are not acceptance of automatic in-memory membership. CI, package publication and published-package adoption in KeyLoad remain separate gates. No NuGet release has occurred.

Table handoff follows every RAPID-agreed configuration in order. The controller consumes the native `IRapidCluster.ViewUpdates` stream and retains at most 10,000 pending member references until their configuration prefix is installed. Old voters freeze only for the immediately following agreed view; they never split their votes across skipped successors. Peer descriptions expose that next pending configuration so any reachable proposer can complete its old/new quorum handoff before advancing again. Observation remains active through graceful departure and is cancelled and joined after the handoff.
