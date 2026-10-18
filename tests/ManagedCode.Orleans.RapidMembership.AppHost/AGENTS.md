# Aspire test topology

Follow the repository root `AGENTS.md`.

Keep real DistributedApplication composition, AddProject resources, explicit-start scale parameters and native fault commands visible in Program.cs. Abrupt crash kills only the actual target process identified by its native PID and start identity, then waits for exit. Do not start independent containers or disguise graceful stop as crash. Every test session owns its resources and shutdown.

Use the root restore/format/Release build gates with `-warnaserror` and the relevant native test commands in `CONTRIBUTING.md`. Preserve unrelated changes and keep documentation aligned with actual source and original test evidence.
