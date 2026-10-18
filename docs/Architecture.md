# Architecture

The required provider uses automatic peer discovery and keeps membership entirely in memory. It requires no known IP list, advertised IP or storage directory. [Requirements](Features/RapidMembership.md) define observable acceptance; [ADR 0001](ADR/0001-self-hosted-membership.md) records the corrected design scope. Automatic discovery is implemented; table/bootstrap/partition integration is still in progress.

## Ownership

| Owner | Responsibility and current state |
| --- | --- |
| `Discovery/MulticastDiscovery` | Actual IPv4 multicast announcements, addresses learned from replies, bounded in-memory observations, expiry and joined socket shutdown. Implemented and tested with actual UDP/TCP sockets. |
| `Discovery/DiscoveryPacket` | Fixed-size authenticated packets, service/cluster isolation and validation. Implemented. |
| `RapidMembershipTable`, `RapidGatewayListProvider` | Native Orleans adapters. They still use the prior coordination implementation and must be rewired to automatic in-memory membership. |
| `CoordinationConfiguration`, `ReplicaConfigurationFence`, `ConfigurationHandoff` | Canonical configurations, permanent old-view barriers, accepted/committed-state recovery and old/new quorum checks. Implemented as components; automatic runtime integration is open. |
| `MemoryMembershipReplica`, `MembershipBallot` | Entirely RAM-backed serialized ballot/table ownership with incarnation fencing, bounded state, repeated-install safety and detached native messages. Implemented as components; automatic runtime integration is open. |
| Provider consensus/hosting/transport | Atomic native table operations, incarnation identity and peer admission. The former fixed-voter/file-backed prototype is superseded; it must be replaced rather than retained as a second mode. |
| `RapidCluster.Core`, `RapidCluster.Grpc` | Original upstream RAPID protocol and gRPC transport, with attributed owner repairs and retained native suites. |
| Microsoft Orleans | Silo statuses, probes, directory, placement, activation lifecycle and real grain execution. |

## Discovery

A participant joins local IPv4 multicast on active interfaces and announces its process-incarnation ID and actual TCP port. Receivers use the network source address instead of accepting a configured or caller-supplied advertised IP. Beacons carry a cluster-scoped identity and authentication. A client listens without becoming an announced server. Discovery retains bounded observations in RAM and expires disappeared peers.

Discovery does not provide atomic table CAS, voter reconfiguration or proof that another process is permanently dead. Its network scope is the participating multicast network; it cannot infer endpoints across a network that does not carry the discovery traffic.

## In-memory table and partition contract

A complete process loss discards membership state; a fresh group must form on restart. Application grain persistence is separate. A restarted process has a new incarnation and cannot present lost coordination promises as retained state.

The owner selected RAPID's consistent-configuration protocol. Classic consensus requires a majority. A table transition freezes a majority of the previous configuration, preserves the highest committed/accepted work, and requires installation on a majority of the new configuration before acknowledging table operations. Discovery expiry is not a reconfiguration decision. An isolated minority cannot acknowledge operations by electing itself as the same cluster. Bootstrap, incarnation fencing and actual network handoff still require runtime integration and native-process qualification.

## Qualification and delivery

Aspire owns actual silo processes, resource commands and joined cleanup. Tests must require exact native address/generation sets and nonce-checked grain execution from continuing external clients across the full zero-to-fifty fault schedule. The current process topology still reflects the prior prototype and must be adapted to use discovery without IP/directory inputs.

Original RAPID simulations qualify their instrumented protocol scope, separately from actual Orleans processes. Original reports retain failures and incomplete runs. Prior fixed-seed/durable-table results are not acceptance of automatic in-memory membership. CI, package publication and published-package adoption in KeyLoad remain separate gates. No NuGet release has occurred.
