# grpc-xml

gRPC collector for JATS, USPTO, XBRL, and DocLang XML, projecting into the gRParse Document data plane

One Rust process reads declarative XML with [`quick-xml`](https://github.com/tafia/quick-xml)
and **streams typed document items as the parser yields them**. Docling's XML
backends build a whole `DoclingDocument` and hand it back at the end; this
service sends the title before it has read the body, and the trailer only
carries counts. It is not PipeStream core and not a Docling Python wrapper.

## Build and run

```bash
cargo build --release          # build.rs compiles proto/ with tonic-prost-build
cargo test                     # unit + integration; no network, no fixtures on disk
cargo clippy --all-targets --no-deps -- -D warnings -D clippy::pedantic
cargo fmt --check
buf lint && buf format --diff --exit-code

./target/release/grpc-xml      # listens on 0.0.0.0:50051
```

Container, with the tests gating the image and a read-only root filesystem:

```bash
docker build -t grpc-xml .
docker run --rm --read-only -p 50051:50051 grpc-xml
```

Poke at it with reflection, no local protos required:

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext localhost:50051 ai.pipestream.xml.v1.XmlParseService/GetServiceInfo
grpcurl -plaintext localhost:50051 grpc.health.v1.Health/Check
```

`protoc` is required to build (the build script invokes it through
tonic-prost-build). `buf` is only needed to lint the contract.

## Wire API

Package `ai.pipestream.xml.v1`, service `XmlParseService`, contract in
[`proto/ai/pipestream/xml/v1`](proto/ai/pipestream/xml/v1). Every message,
field, enum value and RPC carries a documentation comment; `buf lint` runs
`STANDARD` + `COMMENTS` with comment ignores disallowed.

```text
rpc ParseXml(stream ParseXmlRequest) returns (stream ParseXmlResponse);
rpc GetServiceInfo(GetServiceInfoRequest) returns (GetServiceInfoResponse);
```

**Request.** The first frame sets `options`; every frame after it carries a
`chunk` of document bytes, concatenated in stream order.

| Option | Meaning |
|---|---|
| `dialect` | `JATS` / `USPTO` / `XBRL` / `DOCLANG`, or unset to sniff |
| `max_document_mib` | Per-request byte cap; 0 takes the server default, over the ceiling clamps |
| `taxonomy` | XBRL taxonomy package bytes. Accepted, unused in v1 (see below) |
| `emit_html_islands` | Hand XHTML subtrees to the HTML collector instead of flattening them |
| `include_attributes` | Attach unconsumed source attributes to every item |

**Response.** Exactly one `info` first, content events in document order,
exactly one `status` last.

| Event | Carries |
|---|---|
| `info` | `XmlInfo`: resolved dialect and the evidence for it, root namespace and name, DOCTYPE identifiers, encoding |
| `text_item` | One unit of text: title, heading, paragraph, list item, caption, reference, author, patent claim. `label` is structural, `role` is the dialect's own vocabulary |
| `table_start` / `table_row` / `table_end` | A table, streamed a row at a time as each row's end tag is read |
| `fact` | One XBRL fact with its context and unit resolved inline |
| `html_island` | An XHTML fragment, re-serialized, for the HTML collector |
| `status` | `ParseStatus`: dialect, counts, aggregated warnings, bytes consumed, elapsed |

Every item carries a `CollectorSource` (`collector = "xml"`,
`model` = the dialect) and a positional `path` such as
`/article/body/sec[2]/p[3]`, so a coordinator can merge this parse with a PDF
collector's parse of the same paper without either overwriting the other.

**Errors.** A failed parse ends the stream with a status and no `status`
event:

| Condition | Code |
|---|---|
| Over the byte cap, or past the concurrency limit | `RESOURCE_EXHAUSTED` |
| Malformed, truncated, entity-declaring or ambiguous input | `INVALID_ARGUMENT` |
| A dialect this service does not map | `UNIMPLEMENTED` |
| A parser fault | `INTERNAL` |

## Live stream is the product

Content events go out as the parser reaches them, while the client is still
uploading. That is a property of *when* bytes leave the server, which no
assertion about a finished stream can check — so
[`tests/live_stream.rs`](tests/live_stream.rs) holds the upload open, asserts
that content has already arrived, and only then sends the rest. An
implementation that buffered the parse and flushed at the end would hang
there rather than fail an equality check. That test was verified against a
deliberately batching build before being committed.

## Security

The parser is a public attack surface, and [`src/security.rs`](src/security.rs)
is the whole policy:

- **No entity expansion.** quick-xml declares no entities and resolves none;
  general references surface as their own event and this server never looks
  up a replacement. A `<!ENTITY …>` declaration is refused outright with
  `INVALID_ARGUMENT`, because a document that depends on expansion will not
  get it and silently dropping its content is worse than saying so. Billion
  laughs and quadratic blowup are both refused in the prolog.
- **No fetching, of anything.** No DTD, schema, XInclude or XBRL `schemaRef`
  is ever dereferenced. A DOCTYPE system identifier with a scheme
  (`file:`, `http:`, …), an absolute path, or a UNC prefix is refused as the
  XXE payload it is; a bare relative DTD filename — what real USPTO grants
  carry, and what the dialect sniff reads — is recorded on `XmlInfo`,
  reported as a warning, and never opened.
- **No disk.** Document bytes go from the request stream into an in-memory
  channel and straight into the pull parser. The image runs `--read-only`
  with no tmpfs.
- **Bounded.** A byte cap enforced by the reader, so it trips on the chunk
  that crosses it rather than after the upload finishes; a cap on concurrent
  parses, refused rather than queued; small bounded channels in both
  directions, so a client that stops reading stops the parse.

`GetServiceInfo` reports `entity_expansion_disabled` so an operator can
assert the policy from outside instead of trusting this section.

## Environment

| Variable | Default | Meaning |
|---|---|---|
| `GRPC_XML_ADDR` | `0.0.0.0:50051` | Listen address |
| `GRPC_XML_WORKERS` | CPU count | Tokio worker threads |
| `GRPC_XML_BLOCKING_THREADS` | 512 | Blocking pool that runs the parsers |
| `GRPC_XML_MAX_CONCURRENT_PARSES` | 64 | Parses admitted at once; past it, `RESOURCE_EXHAUSTED` |
| `GRPC_XML_MAX_DOCUMENT_MIB` | 256 | Byte cap when a request asks for 0 |
| `GRPC_XML_MAX_DOCUMENT_MIB_CEILING` | 1024 | Hard cap a request cannot exceed |
| `GRPC_XML_METRICS_INTERVAL_SECS` | 60 | Seconds between metrics lines; 0 disables |
| `GRPC_XML_WINDOW_BYTES` | 16 MiB | HTTP/2 initial stream and connection window |

Metrics are a line on stdout on that interval — parses started, ok, failed,
refused, capped, bytes in, events out, and a per-dialect count. The counters
live in [`src/metrics.rs`](src/metrics.rs) if a Prometheus endpoint is ever
wanted.

## Dialect coverage

| Dialect | Sniffed by | Mapped items |
|---|---|---|
| JATS | `http://jats.nlm.nih.gov*` namespace, `//NLM//` or JATS public id, root `article` | title, contributors, affiliations, abstract, keywords, nested sections, paragraphs, lists, formulas, figures, captioned tables, references |
| USPTO | `//USPTO//` public id, ST.96 namespace, root `us-patent-grant` / `us-patent-application` / `patent-document` | title, inventors, assignees, document and application numbers, abstract, headings, description, drawing descriptions, numbered claims, drawing references, CALS tables |
| XBRL | `http://www.xbrl.org/2003/instance` namespace, root `xbrl` | contexts (entity, period, segment/scenario dimensions), units (simple and divide), facts with `contextRef` / `unitRef` resolved inline, `decimals`, `precision`, `sign`, `xsi:nil` |
| DocLang | `http://docling-project.org/ns/doclang/v1` namespace, root `doclang` / `docling-document` | typed decode of label-named elements and of a generic `item` carrying a `DocItemLabel` short name |

Sniffing order is the one [`docs/design.md`](docs/design.md) fixes: an
explicit request wins, then the root namespace, then the DOCTYPE public
identifier, then a well-known root element name as a fallback. Two *strong*
signals that disagree fail closed with both names in the message rather than
being resolved by precedence.

### Known v1 gaps

- **XBRL label linkbases are not resolved.** `taxonomy` bytes are accepted and
  ignored; `Fact.label` is the concept local name and the trailer carries a
  `TAXONOMY_IGNORED` warning saying so. Facts are complete without it, which
  is what design.md requires.
- **The DocLang schema here is inferred.** Docling's serialization is not
  pinned by a published DTD this repo can point at, so the mapper accepts a
  documented, permissive shape (see [`src/dialect.rs`](src/dialect.rs)). Point
  it at a real corpus before trusting it.
- **CALS `namest`/`nameend` column spans** are not expanded through
  `colspec`; `colspan`, `rowspan` and `morerows` are.
- Nested tables are flattened into the outer table's cell text.

## Layout

```text
proto/ai/pipestream/xml/v1/   the contract; buf lint STANDARD + COMMENTS
src/security.rs               what the parser refuses and what it records
src/sniff.rs                  dialect resolution and its evidence
src/dialect.rs                per-family mapping rules, one pure function each
src/parse.rs                  the streaming driver: XML events to protobuf events
src/service.rs                tonic wiring, byte cap, admission control
src/metrics.rs                counters and the interval line
tests/dialects.rs             golden mappings for all four families
tests/security.rs             XXE, entity bombs, truncation, caps, refusals
tests/live_stream.rs          the tests that fail if the stream becomes a batch
```

## Docs

- [AGENTS.md](AGENTS.md) — read order, definition of done, git
- [Architecture](docs/architecture.md) — where this sits in the collector fleet
- [Design](docs/design.md) — wire API, Document mapping, tests
- [Guidelines](docs/guidelines.md) — how to build it so it matches the fleet

## Remotes

- **Forgejo** (`git.rokkon.com/ai-pipestream/grpc-xml`) is the source of truth. `main` lives here.
- **GitHub** is a public push-mirror of `main`. Do not merge to GitHub `main`.
- GitHub's default branch is `development` so LLM / `gh` work lands there instead of clobbering the mirror.

Push Forgejo first. GitHub `main` updates from the Forgejo push-mirror.
