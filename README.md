# grpc-xml

gRPC collector for JATS, USPTO, XBRL, and DocLang XML, projecting into the gRParse Document data plane

This repo is a spec plus (soon) a standalone gRPC server. It is not
PipeStream core and not a Docling Python wrapper.

## Start here (humans and LLMs)

1. [`AGENTS.md`](AGENTS.md) — read order, definition of done, git
2. [`docs/architecture.md`](docs/architecture.md) — where this sits, language, live stream vs Docling
3. [`docs/design.md`](docs/design.md) — wire API, Document mapping, tests
4. [`docs/guidelines.md`](docs/guidelines.md) — fleet rules (streaming, proto, diskless, git)

Implementation is greenfield. Copy operational patterns from
`/work/main/grpc-services/grpc-calamine and /work/main/grpc-services/fastwarc-grpc`.

## Docs

- [Architecture](docs/architecture.md) — where this sits in the collector fleet
- [Design](docs/design.md) — wire API, Document mapping, tests
- [Guidelines](docs/guidelines.md) — how to build it so it matches the fleet

## Remotes

- **Forgejo** (`git.rokkon.com/ai-pipestream/grpc-xml`) is the source of truth. `main` lives here.
- **GitHub** is a public push-mirror of `main`. Do not merge to GitHub `main`.
- GitHub's default branch is `development` so LLM / `gh` work lands there instead of clobbering the mirror.

Push Forgejo first. GitHub `main` updates from the Forgejo push-mirror.
