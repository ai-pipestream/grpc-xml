# AGENTS.md: grpc-xml

**Status: v1 is implemented.** The definition of done below is met; see
`README.md` for what the server does today and for the v1 gaps that remain
(XBRL label linkbases, the inferred DocLang schema, CALS `namest`/`nameend`).
The rest of this file is the brief the implementation was built against, and
the specs are still the source of truth for changing it.

Originally: you are implementing **grpc-xml** from scratch in this repo. There
is no application code yet. Specs are the source of truth.

## Read this first, in order

1. This file
2. `docs/architecture.md`: fleet boundary, language, what we refuse to own
3. `docs/design.md`: wire API sketch, Document mapping, tests
4. `docs/guidelines.md`: fleet rules (streaming, proto, git, tests)

Do not start coding until those four are in your context. If architecture
and an existing sibling disagree on *process* (diskless, health, buf),
follow the sibling. If they disagree on *product* (live stream, Document
plane), follow architecture.md.

## This service

gRPC collector for JATS, USPTO, XBRL, and DocLang XML, projecting into the gRParse Document data plane

- **Language:** Rust (tonic + quick-xml). No libxml2, no lxml.
- **Copy from:** /work/main/grpc-services/grpc-calamine and /work/main/grpc-services/fastwarc-grpc
- **Stack:** One process, dialect enum on the request, sniff if unspecified. XML parser: no network, no DTD, no entity expansion.
- **Live stream:** XmlInfo first, then content events / rows as yielded, HtmlIsland for XHTML fragments, ParseStatus last.

## Definition of done (v1)

ParseXml stream, four dialect fixtures, XXE+entity-bomb tests, buf proto, health+reflection, read-only image.

Also: README with build/run; proto lint clean; tests that fail if someone
turns the stream back into a batch (assert an event before the input is
fully consumed, or per-item events before Complete).

## Workspace

Checkout path: `/work/main/grpc-services/grpc-xml`.
Git: `origin` = Forgejo (push `main` here). `github` = GitHub mirror.
Never merge GitHub `main`. See `docs/guidelines.md`.

gRParse wiring (`COLLECTOR_*` enum, endpoint env) is a **follow-up**.
Ship a working server in this repo first.
