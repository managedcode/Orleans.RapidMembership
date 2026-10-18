# Orleans RAPID membership provider

Implement Orleans 10.4 membership and gateway discovery without Azure Table. Use durable majority Paxos over explicitly configured fixed voters inside silo processes. RAPID observes those voters; native Orleans owns silo status, probing and grain execution. Preserve Reuben Bond's RapidCluster MIT attribution and original protocol suites.

## Acceptance and delivery

Require atomic native row/version CAS, concurrent writers, durable recovery, quorum loss, cluster/key isolation, bounded cancellation and joined disposal. Actual Aspire-owned Orleans processes and external clients must execute nonce-checked grain calls throughout 0→10→20→5, simultaneous ten-node joins/departures, complete loss/restart, fifty-node growth, cold fifty and graceful reduction/rejoin. Keep exact active identities/generations and every-live-silo execution assertions.

Publish the combined initial batch as 10.4.0 after the remaining qualification gates. Release builds/packs/pushes all three packages independently of CI and does not poll indexing. Create the version tag with that release. Update KeyLoad only from published NuGet packages and run its own focused and mandatory qualification gates. No package has been published and KeyLoad remains unchanged.

## Completed evidence

- Public checkpoint 5a40740 is on main at managedcode/Orleans.RapidMembership. README attribution/badges, root/project policies, architecture, requirements, ADR, contributing commands and EditorConfig are published. GitHub description and topics are configured. CodeQL passed for that checkpoint.
- Owner-requested diagnostics-mcp is installed; the installed Roslynk skill matches the specified source. Roslynk loaded all 22 projects and reports zero errors/warnings. Retained profiling distinguishes whole-process allocations from provider allocations and makes no unsupported framework defect or speedup claim.
- Full Release build with warnings as errors passed. After the protocol departure correction: 534 upstream unit tests, 42 non-scale provider tests, 34 network tests and the original LeaveImmediatelyAfterJoin case passed. Original provider coverage is 92.37% line / 91.55% branch; all thirteen critical source files exceed 90%. Three local 10.4.0 packages packed with original MIT/LICENSE, NOTICE, README and icon metadata.
- Cold fifty and voter-majority loss/restoration passed with real silos and four continuous clients. The full churn run reached exact membership and execution on all fifty silos, recording 49,934 cumulative successful calls, then failed native DCP resource-stop. A native Aspire HTTP command now requests actual host shutdown and requires exit code zero. Its focused four-silo stop/restart test passed; full churn requalification remains open.

## Open qualification work

- Complete protocol simulations: the native full run exposed LeaveImmediatelyAfterJoin failing because StopAsync cancelled the leaving node's consensus before its departure could be learned. The owner repair keeps consensus alive until bounded departure completion and prevents shutdown rejoin. The original focused case now passes in 208 ms; complete simulation requalification remains pending. Original failed and interrupted artifacts remain retained.
- Re-run the full Orleans churn case with actual graceful host shutdown, then qualify final source on Linux CI. Keep the 45-minute churn, 15-minute cold-fifty, 10-minute voter/phase and increased test-operation deadlines requested by the owner. Linux artifacts show native Orleans declaring live silos dead under the former one-second test profile; test probes/refresh now use five seconds and heartbeat uses the native thirty-second default. These profile changes still require final-source process qualification.
- Confirm the Windows async unit assertion fix and explicit Aspire CLI preparation on GitHub runners. These changes preserve assertions and analyzer severity.
- Review and publish remaining source, package descriptions/tags and workflows. Final formatting/build, native test evidence, package metadata, publication and consumer adoption are separate gates.
