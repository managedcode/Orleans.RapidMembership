# Orleans membership provider

Follow the repository root `AGENTS.md`.

Own native table/gateway adapters, automatic multicast discovery, in-memory coordination, admission and hosting here. Require no seed IPs, advertised IP or membership directories. Keep public configuration in `RapidMembershipOptions` and registration in `RapidMembershipExtensions`. Fix table/CAS/discovery defects here, not in consumers. Preserve serializer IDs/aliases and truthful atomic operation outcomes. Follow RAPID quorum consistency and old/new quorum handoff; discovery expiry never changes the voting configuration. Run provider native network and Aspire cases; public and wire contract changes need explicit review.

Use the root restore/format/Release build gates with `-warnaserror` and the relevant native test commands in `CONTRIBUTING.md`. Preserve unrelated changes and keep documentation aligned with actual source and original test evidence.
