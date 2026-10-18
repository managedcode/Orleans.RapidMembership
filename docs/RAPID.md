# RAPID protocol in this fork

The protocol implementation is based on [Reuben Bond's RapidCluster](https://github.com/ReubenBond/RapidCluster), an implementation of RAPID membership. Original MIT copyright is preserved in [LICENSE](../LICENSE); [NOTICE](../NOTICE) describes this fork. The upstream repository and its history retain the original documentation and implementation provenance.

RAPID supplies peer discovery, monitoring and agreement on an observed participant view. In this Orleans provider, its participants are the explicitly configured seed voters. Ordinary silos use the native Orleans membership table. A RAPID view does not implement the durable row/version CAS needed by Orleans; see [architecture](Architecture.md) and [ADR 0001](ADR/0001-self-hosted-membership.md).

## Protocol stages

1. Seed discovery supplies known explicit endpoints. The first configured seed coordinates initial bootstrap. Returning nodes try existing seeds before forming an initial view.
2. Multiple deterministic monitoring rings assign observers. Peer observations feed the multi-process cut detector, reducing the effect of one observer's suspicion.
3. RAPID's consensus protocol selects participant-view changes. This protocol is separate from the provider's durable fixed-quorum membership-table consensus.

The code owners are [RapidCluster.Core](../src/RapidCluster.Core/) for protocol and hosted lifecycle, and [RapidCluster.Grpc](../src/RapidCluster.Grpc/) for HTTP/2 transport. Their ManagedCode packages are `ManagedCode.Orleans.RapidMembership.Protocol` and `ManagedCode.Orleans.RapidMembership.Grpc`; original assembly and namespace identities remain visible. Install the [Orleans provider](../README.md) for native membership and gateway integration.

## Observation and runtime ownership

The native RAPID hosted service owns startup, observation subscriptions and joined shutdown. The provider binds the listener before observation starts. Its `VoterView` is advisory. Orleans' native table rows and silo probes determine active silo membership; an ordinary silo has no RAPID participant. Stale RAPID observations never change the fixed durable table quorum or authorize independent writable tables.

The default provider membership socket is private HTTP/2 on TCP 11112. Identity and secret headers protect admission on both RAPID and membership-table services. Orleans silo and gateway ports are separate. Full deployment, persistence and failure limits are documented once in the [README](../README.md).

## Protocol qualification

Retained upstream unit, real-network, property-based, deterministic Clockwork and Aspire suites exercise the original protocol. `eng/test-simulations.py` discovers the compiled instrumented test inventory and reconciles every native result. These tests do not substitute for the provider's actual Orleans process and continuing-client tests. Run the commands in [CONTRIBUTING.md](../CONTRIBUTING.md); actual CI artifacts establish the qualification result.
