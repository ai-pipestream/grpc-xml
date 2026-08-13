# grpc-xml architecture

**Status:** implemented (v1); this file remains the contract the code answers to
**Updated:** 2026-08-13

Implementers start at [`AGENTS.md`](../AGENTS.md), then this file, `design.md`, and `guidelines.md`.

## Where this sits

Docling ships four XML *declarative* backends (JATS, USPTO, XBRL,
DocLang). They turn tagged trees into a `DoclingDocument` with no
pixels. One gRPC service with a format enum covers all four so we do
not run four almost-identical servers.

```text
.xml / .nxml / .xbrl / .dclg
        │
        ▼
   grpc-xml            schema plugin per dialect
        │
        ▼
   gRParse coordinator (COLLECTOR_XML)
        ▼
   Document
```

HTML islands inside JATS (or XHTML in XBRL labels) are **not**
re-parsed with a second XML stack. They are handed to the HTML
collector as opaque fragments, the same way email HTML bodies are.

## Live results (vs Docling)

Docling's XML backends build a complete `DoclingDocument` and return
it. We stream sections, paragraphs, tables, and fact rows **as the
parser yields them** so a UI can show a USPTO grant or an XBRL
instance filling in, not a spinner until the last closing tag. Title
and `XmlInfo` go out first. `ParseStatus` is a trailer.

## What this process owns

- Secure XML: no network, no DTD fetch, no entity expansion. XXE and
  billion-laughs die at the parser, matching Docling's lxml flags.
- Dialect detection: explicit option wins; otherwise sniff root
  namespace / public id. Ambiguous `.xml` without a hint is
  `INVALID_ARGUMENT`, not a guess.
- Projection to sections, paragraphs, tables, lists, citations, and
  (for XBRL) fact tables.
- USPTO: the ST.36 / ST.96 grant and application XML families Docling
  already maps.
- XBRL: instance facts + optional taxonomy **bytes** on the request
  (not a filesystem path — this service is diskless).

## What this process does not own

| Concern | Owner |
|---|---|
| Arbitrary XML → Document | out of scope; unknown dialects fail |
| HTML/CSS layout of JATS bodies | HTML collector |
| PDF of the same paper | gRParse CV, a different collector on the same parse |
| Taxonomy hosting | client sends the package; we do not fetch from the web |

## Language

**Rust**. `quick-xml` (streaming) plus small typed mappers per
dialect. Streaming matters for USPTO grants and XBRL instances that
are tens of megabytes of repeated elements. No `libxml2`, no Python
lxml.

Java/Woodstox is a fine plan B if a dialect's schema story is
unmanageable in Rust; the wire contract does not care.

## Security

The XML parser is a public attack surface. Default:

- `max_entity_expansion = 0`
- `max_document_mib` cap
- no `http://` or `file://` resolution of XInclude / DTD / schema
- XBRL taxonomy arrives as a request blob or is omitted
