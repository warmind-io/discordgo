# AGENTS.md

This is the Warmind fork of `discordgo`, a low-level Go binding for the Discord API. Treat it as infrastructure: small compatibility changes are fine, but app-specific bot behavior belongs elsewhere.

## Start Here

- Read `README.md`, `CONTRIBUTING.md`, and the package files closest to the API surface you are changing.
- Check `examples/` for expected public usage.
- For Warmind context, use `/Users/aether/obsidian/notes/INDEX.md` only as a routing map.

Current code, tests, and Discord API compatibility are implementation truth. Preserve upstream-style APIs unless a breaking change is explicitly requested.

## Safety

- Never print Discord tokens or auth headers.
- Do not add Warmind-specific behavior to this generic library.
- Do not change exported structs, JSON tags, endpoint paths, rate-limit behavior, or websocket semantics without focused tests and explicit rationale.
- Do not replace this fork with upstream or change module identity as part of unrelated work.
- Do not revert unrelated dirty changes.

## Commands

- Test all packages: `go test ./...`
- Focused tests: `go test -run <TestName> ./...`
- Format Go edits: `gofmt`

## Coding Patterns

- Keep the package a direct, low-level mapping of Discord REST, websocket, voice, interaction, and event APIs.
- Prefer typed structs with explicit JSON tags matching Discord payloads.
- Keep REST endpoint construction, rate-limit handling, state/cache behavior, and websocket handling in their existing files.
- Add table-driven tests for payload encoding/decoding, endpoint helpers, rate-limit behavior, and utility functions.
- Document non-obvious Discord API constraints near the code they affect.
- Follow the Go standards in `/Users/aether/obsidian/notes/Projects/Charlemagne/Charlemagne coding standards.md`, while preserving the existing upstream `discordgo` style.

## Verification

Run the focused package tests first, then `go test ./...` before calling behavior changes done. For generated or API-shape edits, verify examples still compile where practical.
