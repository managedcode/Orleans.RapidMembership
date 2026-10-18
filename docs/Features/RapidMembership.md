# Self-hosted Orleans membership

The provider implements Orleans 10.4 membership and gateway discovery on .NET 10 using durable seed voters hosted inside silo processes. It requires no Azure table or external membership database. See [architecture](../Architecture.md), [configuration](../../README.md) and [ADR 0001](../ADR/0001-self-hosted-membership.md).

## Requirements and acceptance

| Requirement | Observable acceptance | Source and qualification |
| --- | --- | --- |
| RM-001 Native ownership | Orleans remains responsible for statuses, probes, grain placement and execution; real clients invoke a native grain after formation. | `RapidMembershipTable`, `RapidGatewayListProvider`; `NativeProviderTests`, `AspireMembershipTests`. |
| RM-002 Atomic table | Concurrent row/table CAS attempts have one winner; heartbeats preserve table version; cleanup retains required death votes. | `MembershipMutations`; `MutationTests`, `NetworkConsensusTests`. |
| RM-003 Durable quorum | Promises and accepted values survive voter restart; a majority-accepted value is recovered before competing work; a missing majority cannot acknowledge reads or mutations. | `MembershipAcceptor`, `VoterStorage`; `AcceptorTests`, `NetworkConsensusTests`, voter-loss Aspire case. |
| RM-004 Admission isolation | Wrong identity, ordered voter configuration or secret cannot mutate this cluster. Invalid limits and endpoints fail before startup. | `PeerTransport`, `MembershipHost`, `RapidMembershipOptions`; `OptionsTests`, `NetworkConsensusTests`. |
| RM-005 Bounded operations | Deadlines, cancellation, snapshot/row/admission/history limits remain effective. An aged unresolved outcome reports ambiguity instead of false CAS rejection. | Proposer, transport and snapshot contracts; `NetworkConsensusTests`, `LifecycleCancellationTests`. |
| RM-006 Dynamic processes | Exact native membership converges after joins, graceful departures, abrupt kills and full restart; each live silo executes an actual grain operation. | `AspireMembershipTests`, `AspireChurnTests`, `AspireChurnCluster`. |
| RM-007 Continuing clients | Four existing clients issue nonce-checked calls through churn, observe bounded failures at zero, and recover without being replaced. | `ContinuousClients`, fifty-process Aspire case. |
| RM-008 Delivery | CI executes the original mandatory suites and platform/scale gates. Independent Release pushes three packages sharing one authorized version; consumer adoption uses the published feed. | CI, Release, original TRX, coverage and release artifacts. |

## Real-process scale schedule

```mermaid
flowchart LR
    Z[0] --> A[10]
    A --> B[20]
    B -->|Abrupt loss| C[5]
    C --> D[20]
    D -->|Join ten and crash ten concurrently| E[20 changed identities]
    E -->|Crash every live process| F[0]
    F --> G[10 restarted]
    G --> H[50]
    H -->|Graceful stop| I[25]
    I --> J[50 returned]
```

An independent cold-start case also starts fifty real silo processes directly from zero and requires exact membership and grain execution on every node.

A three-voter majority survives the partial-loss phases. Every live node must report the exact expected native address and generation set; count alone is insufficient. Each client must demonstrate progress in each live phase. The zero phase checks absence of successful calls after draining in-flight work. Full restart reuses each voter's durable directory.

A separate case starts ten real silos, loses one seed voter, then loses a majority. Actual network table reads and mutations must cancel within their deadline. Returning one voter restores the two-of-three durable quorum and native client progress; returning the third restores the full seed group. This distinguishes table-quorum availability from RAPID observation convergence.

## Limits and evidence

This is explicit seed discovery, not address-range scanning or automatic merging of independent deployments. Ordinary silos join the native table without becoming RAPID voters. Online voter-set reconfiguration is unsupported. Shared-secret admission requires a trusted private or encrypted network and does not encrypt HTTP/2.

Requirements and test inventory describe what must be checked, not an assertion that every gate already passed. The current work item remains [in progress](../../tasks/in-progress/001_OrleansRapidMembership.md) until full qualification, publication and consumer integration finish. CI retains native reports; failed or incomplete runs remain failed or incomplete. Process-kill evidence does not establish power-loss durability.
