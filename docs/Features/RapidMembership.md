# Automatic in-memory Orleans membership

The required provider implements Orleans 10.4 membership and gateway discovery on .NET 10, discovering peer addresses itself and keeping all membership state in RAM. It requires no Azure table, external membership database, seed IP list, advertised IP or storage directory. [Architecture](../Architecture.md) identifies current implementation boundaries; [ADR 0001](../ADR/0001-self-hosted-membership.md) records the corrected scope.

## Requirements and acceptance

| Requirement | Observable acceptance |
| --- | --- |
| RM-001 Native ownership | Orleans owns statuses, probes, placement and execution; actual external clients invoke native grain contracts. |
| RM-002 Atomic table | Concurrent native row/version CAS has one winner; heartbeat preserves table version; cleanup preserves required death votes. |
| RM-003 In-memory lifetime | No membership state files or directory configuration. A restart has a fresh process incarnation; complete process loss permits fresh membership formation. No lost promise is represented as retained coordination state. |
| RM-004 Admission isolation | Wrong service/cluster identity or secret cannot join or mutate this cluster. Invalid bounds fail before startup. |
| RM-005 Bounded operations | Deadlines, cancellation, byte/row/admission/history/peer limits and joined cleanup remain effective. Unknown outcomes never become fake success or fabricated CAS rejection. |
| RM-006 Dynamic processes | Exact native membership converges after joins, graceful departure, abrupt kills and full restart; every live silo executes real grain calls. |
| RM-007 Continuing clients | Four existing clients issue nonce-checked calls through churn, stop succeeding at zero and recover without replacement. |
| RM-008 Delivery | Retain native upstream and platform/scale gates, original artifacts and one authorized three-package release. Adopt the actual published package in KeyLoad. |
| RM-009 Automatic discovery | Real UDP discovery finds actual TCP endpoints without configured IPs. Configured ranges complete their initial bounded scan before bootstrap. A learned founding candidate cannot be promoted after expiry; its pre-formation loss stays closed until complete restart. External clients observe without becoming servers; disappeared peers expire; foreign clusters/secrets stay isolated. |
| RM-010 Partition contract | Follow RAPID consistent configurations and majority recovery. Require old/new majorities for atomic table handoff; reject minority writes, stale configurations and forgotten incarnations. Discovery expiry cannot reduce the quorum. |

## Principal real-process schedule

Use actual Aspire AddProject silo processes and actual external Orleans clients. Exercise zero → ten → twenty → five, return to twenty, concurrent ten joins and ten crashes, full loss to zero, restart ten, grow to thirty, graceful reduction to fifteen and return to thirty. Independently start thirty directly from zero.

Every live node must report the complete expected native address/generation set; matching counts alone cannot pass. After each live topology change, every external client calls native Orleans IManagementGrain.GetHosts(true) and must see that same exact active set. The zero phase requires management RPC to be unavailable. Windows, Linux and macOS each run the full scenario in a separate GitHub job. Every live silo must execute a grain call and every continuing client must progress in each live phase. The zero phase drains in-flight work and verifies absence of further successful calls. Complete loss creates fresh in-memory membership rather than reopening voter files.

The tests now use discovered peers, entirely in-memory membership and actual coordinator identities. Prior fixed-seed results do not qualify the corrected behavior. Preserve their actual fault injection, exact identity assertions, client operations and joined cleanup.

## Evidence

Native UDP/TCP, RAM consensus, native silo/client hosting and focused Aspire recovery checks have run. Runtime integration is implemented. Current qualification and delivery outcomes are recorded in original CI artifacts and the tracked work item. Retain original failed, interrupted and incomplete reports; never substitute simulation, documentation or a parent coverage report for actual silo execution.
