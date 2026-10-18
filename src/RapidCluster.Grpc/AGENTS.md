# RAPID transport

Follow the repository root `AGENTS.md`.

Own the original RAPID HTTP/2 gRPC transport, headers and normal/join timeouts here. Preserve cancellation, channel cleanup and original protocol contracts. Table-specific envelopes and acceptor service belong to the Orleans provider. Run upstream network/Aspire plus provider isolation and lifecycle tests for affected transport changes.

Use the root restore/format/Release build gates with `-warnaserror` and the relevant native test commands in `CONTRIBUTING.md`. Preserve unrelated changes and keep documentation aligned with actual source and original test evidence.
