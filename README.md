# grpc-xml

gRPC collector for JATS, USPTO, XBRL, and DocLang XML, projecting into the gRParse Document data plane


## Docs

- [Architecture](docs/architecture.md) — where this sits in the collector fleet
- [Design](docs/design.md) — wire API, Document mapping, tests

## Remotes

- **Forgejo** (`git.rokkon.com/ai-pipestream/grpc-xml`) is the source of truth. `main` lives here.
- **GitHub** is a public push-mirror of `main`. Do not merge to GitHub `main`.
- GitHub's default branch is `development` so LLM / `gh` work lands there instead of clobbering the mirror.

Push Forgejo first. GitHub `main` updates from the Forgejo push-mirror.
