# RAPID protocol in this fork

Based on [RapidCluster by Reuben Bond](https://github.com/ReubenBond/RapidCluster), an implementation of the [RAPID membership protocol](https://www.usenix.org/system/files/conference/atc18/atc18-suresh.pdf). Original MIT copyright is preserved in [LICENSE](../LICENSE); [NOTICE](../NOTICE) identifies this fork's additions.

## Protocol stages and ownership

1. Authenticated multicast/broadcast and optional bounded IPv4 CIDR/range probes discover actual peer addresses and listener ports. The provider waits for a stable discovered peer set before choosing the lowest process-incarnation ID for cold bootstrap. Other coordinators join through discovered installed configurations.
2. Native RAPID monitoring rings assign observers. Multi-process cut detection combines observations into proposed participant-view changes.
3. RAPID fast consensus and classic recovery agree coordinator configurations. A separate RAM ballot protocol implements atomic Orleans table operations and freezes an old majority before installing recovered state on a new majority.

The native protocol is in [RapidCluster.Core](../src/RapidCluster.Core/); HTTP/2 transport is in [RapidCluster.Grpc](../src/RapidCluster.Grpc/). Their ManagedCode packages preserve original assembly and namespace identities. The [Orleans provider](../README.md) owns discovery, admission, RAM table replication and configuration handoff. Automatic IP discovery is a provider extension, not an upstream guarantee of global discovery across partitioned networks.

## Runtime contract

Up to three silos form the coordination group. Ordinary silos still announce their listeners and can be recruited; Microsoft Orleans owns their statuses, probes, distributed directory, placement and grain execution. The native RAPID participant view is not the Orleans active-silo table. Discovery expiry never shrinks a table quorum. A new process incarnation cannot reuse forgotten votes; independently bootstrapped roots with the same service identity are rejected.

Everything membership-related stays in RAM. Complete process loss permits fresh cluster formation and discards the old table. This is separate from application persistence. See [architecture](Architecture.md) for owners and [README](../README.md#network-scope) for discovery, HTTP/2 and native Orleans ports.

## Qualification

Retained upstream unit, real-network, deterministic Clockwork and Aspire suites exercise the original protocol. `eng/test-simulations.py` discovers the instrumented native inventory and reconciles every original result. Clockwork is supplemental in RapidCluster.UpstreamSimulation.slnx; the owner selected native TimeProvider tests and actual Orleans silo-process/continuing-client qualification for provider CI. [Testing](Testing.md) identifies those separate gates and [CONTRIBUTING](../CONTRIBUTING.md) owns executable commands.
