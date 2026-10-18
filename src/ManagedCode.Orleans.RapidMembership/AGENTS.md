# Orleans membership provider

Follow the repository root `AGENTS.md`.

Own native table/gateway adapters, majority consensus, exclusive durable storage, admission and hosting here. Keep public configuration in `RapidMembershipOptions` and registration in `RapidMembershipExtensions`. Fix table/CAS/storage defects here, not in consumers. Preserve serializer IDs/aliases and fail-closed quorum behavior. Run provider native network and Aspire cases; public and durable contract changes need explicit review.

Use the root restore/format/Release build gates with `-warnaserror` and the relevant native test commands in `CONTRIBUTING.md`. Preserve unrelated changes and keep documentation aligned with actual source and original test evidence.
