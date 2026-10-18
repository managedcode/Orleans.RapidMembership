# Provider qualification

Follow the repository root `AGENTS.md`.

Use native xUnit v3 / Microsoft.Testing.Platform with real provider and transport implementations. Support code stays reachable from actual cases. AppHost owns process lifecycle. Assert real grain results, exact address/generation sets, quorum rejection, continuing-client progress and joined cleanup. Keep Category=Scale separate but mandatory; never omit a phase or weaken membership equality to count-only. Original reports belong under artifacts/tests.

Use the root restore/format/Release build gates with `-warnaserror` and the relevant native test commands in `CONTRIBUTING.md`. Preserve unrelated changes and keep documentation aligned with actual source and original test evidence.
