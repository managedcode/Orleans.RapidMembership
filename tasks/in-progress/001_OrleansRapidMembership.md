# Orleans RAPID membership provider

Implement an Orleans 10.4 membership and gateway provider without Azure Table.
Keep RAPID discovery; use durable majority Paxos over a fixed, explicitly configured voter set for atomic native membership-table operations.
Voters run inside silo processes. Preserve upstream MIT attribution, protocol tests and simulation qualification.
Acceptance: atomic row/version CAS, concurrency, quorum loss, process death/rejoin, cluster/key isolation, bounded cancellation and joined disposal; real three-silo Aspire grain operations and external client discovery.
Release 10.4.0 only after combined checks; verify NuGet, then update KeyLoad.

Owner scope extension: real-process transitions 0→10→20→5, concurrent ten-node join/crash, complete loss and recovery, and 50-node scale with continuous external grain callers. Added a dedicated mandatory Linux qualification job; execution evidence is required before release.

Repository presentation: README includes upstream attribution, all three package badges, CI/Release/CodeQL/license links, configuration, network ports, quorum and persistence limits. Root and project-local AGENTS, CLAUDE wrapper, contributing commands, canonical architecture, requirements and ADR are added. Relative documentation links were checked.

Qualification findings remain open: the full churn run reached complete process loss and cold recovery to ten but timed out growing to fifty. Majority-restoration restored native operations but exposed a passive RAPID voter that discarded consensus traffic until its own proposal. The owning protocol repair and deterministic/process tests are in the same initial release batch. No package has been published and KeyLoad is not yet changed.

Owner-requested Roslynk and diagnostics analysis: diagnostics-mcp was installed from the specified ManagedCode skill source; Roslynk was already installed with matching skill content and is connected. All 22 solution projects loaded; analyzer errors/warnings are now zero. Style fixes used native Roslynk actions. Transport channel ownership, handler disposal and one-way completion tracking were repaired; a real connection-closure/healthy-new-client case was added.

Current full Release build passes with warnings as errors; all 41 non-scale provider tests pass. Original provider coverage is 92.37% line / 91.55% branch, with all critical-source gates passing. All three 10.4.0 packages pack successfully with MIT, LICENSE, NOTICE, README and icon metadata. Original failure evidence remains.

The owner requested larger timeouts and emphasized standalone library behavior, rather than interpreting Orleans allocations alone as defects. Scale limits are now 45 minutes for churn, 15 minutes for cold fifty and 10 minutes for voter recovery, with ten-minute phase limits. Test-node membership operations allow two minutes and peer exchanges five seconds. Cold-fifty requalification with these settings is running; full scale and final upstream suites remain open. Native exact-membership and external grain-call assertions remain unchanged.

Repository presentation follow-up plan: (1) retain the root EditorConfig ownership and every analyzer severity, repair invalid inline comments and define non-C# indentation consistent with sibling Orleans libraries; (2) update GitHub description/topics and publish the current implementation checkpoint to main without starting an intermediate package release; (3) validate configuration with Roslynk diagnostics, native formatter and Release build after the active test lane joins. CI retains qualification status; publish one combined NuGet release and its version tag after remaining checks.

Public source checkpoint: publish provider, tests, documentation, EditorConfig, CI and CodeQL to the existing public ManagedCode fork. The NuGet Release workflow is retained locally for the final combined batch after qualification, preventing this source-visibility checkpoint from publishing an intermediate package. GitHub description and topics are now configured; the version tag belongs to the completed release.
