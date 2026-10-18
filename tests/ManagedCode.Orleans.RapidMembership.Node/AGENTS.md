# Actual Orleans test process

Follow the repository root `AGENTS.md`.

Keep host composition and native Orleans configuration visible in Program.cs. Use the real membership provider and generated grain contracts. State endpoints report actual native table addresses/generations and the separate advisory voter view. Probe grains return nonce, native silo, activation identity and sequence; never synthesize successful calls or membership rows.

Use the root restore/format/Release build gates with `-warnaserror` and the relevant native test commands in `CONTRIBUTING.md`. Preserve unrelated changes and keep documentation aligned with actual source and original test evidence.
