# grpc-xml design

## 1. Goals

- Feature parity with Docling `XML_JATS`, `XML_USPTO`, `XML_XBRL`,
  `XML_DOCLANG`.
- One process, one port, format selected on the request.
- Stream elements of large instances (USPTO claims, XBRL facts) as
  table rows / paragraphs rather than materializing a DOM of the whole
  file when the dialect allows it. Live UI is the reason: Docling
  waits for the document; we paint each yielded item. A unary Document
  convenience RPC is allowed; it is not the product path.
- Identical `Document` projection gRParse can merge with a PDF
  collector of the same paper.

## 2. Non-goals (v1)

- Generic XML-to-JSON or XSLT hosting.
- Fetching remote taxonomies, DTDs, or XInclude.
- METS/GBS (`tar.gz` of many files) — that is an unpacker that fans
  inner XML/PDF into this service and gRParse, not an XML dialect.
- Pretty-printing or canonicalizing the source XML as an output
  format (the sink might, later).

## 3. Wire API (sketch)

`ai.pipestream.xml.v1.XmlParseService`

```text
rpc ParseXml(stream ParseXmlRequest) returns (stream ParseXmlEvent);
rpc GetServiceInfo(GetServiceInfoRequest) returns (ServiceInfo);
```

Options:

- `dialect` — `JATS` / `USPTO` / `XBRL` / `DOCLANG` / `UNSPECIFIED`
  (sniff)
- `taxonomy` — optional bytes (XBRL only), a zip of schemas/linkbases
- `max_document_mib`

Events:

1. `XmlInfo` — resolved dialect, root namespace, title if known
2. Native content events **or** a single `Document` (v1 can emit
   Document directly; these dialects are declarative and usually
   smaller than office files)
3. `HtmlIsland` — xpath/id + XHTML bytes, for the HTML collector
4. `ParseStatus`

## 4. Mapping to Document

**Implemented in-repo** as of the `emit_document` option: the fold lives in
[`src/document_fold.rs`](../src/document_fold.rs), consumes the same
`ParseXmlResponse` events the service writes, and the server sends the result
as a `document` event immediately before the `status` trailer. The typed
stream stays the product; the Document is a lossy projection of it. The
vendored schema is `proto/ai/pipestream/document/v1/document.proto`, copied
byte-identical from gRParse and never edited here.

Follow Docling's backends, not a 1:1 XML clone:

| Dialect | Typical items |
|---|---|
| JATS | title, authors, abstract, sections, paragraphs, tables, refs |
| USPTO | title, inventors, abstract, claims (numbered), description, drawings as pictures if present as embedded images |
| XBRL | fact table(s): concept, context, unit, value; taxonomy labels when provided |
| DocLang | already-close-to-Document; mostly a typed decode |

Every item: `CollectorSource.collector = "xml"`, `model` = dialect
name, `version` = this server's version, `confidence` unset — a declarative
mapping is deterministic, so a confidence would be noise. No fake page bboxes.

### 4.1 What the fold builds

- **Shape.** Flat arenas plus refs: `#/texts/N`, `#/tables/N`, each item's
  `parent` is `#/body` and each is listed in `body.children`. No groups: the
  typed stream carries nesting only as a heading `level`, and inventing
  section groups from it would be a guess the merge cannot undo.
  `field_regions` and `field_items` stay empty — the coordinator's merge does
  not renumber them.
- **Identity.** `schema_name = "docling_document_v2"`,
  `origin.mimetype = "application/xml"`, `name` = `XmlInfo.title` when the
  dialect exposed one, otherwise the first `TITLE` item's text (none of the
  four dialects currently fill `XmlInfo.title`, so in practice it is the title
  item). Root namespace, root local name and dialect go on the body's
  `meta.custom_fields` as `xml.root_namespace`, `xml.root_local_name`,
  `xml.dialect` — root meta is **first-writer-wins** in the coordinator's
  merge, so treat those as a hint; the per-item `CollectorSource.model`
  carries the dialect authoritatively.
- **Text items.** `TITLE` → `TitleItem`, `SECTION_HEADER` →
  `SectionHeaderItem` (level from the event, defaulting to 1), `LIST_ITEM` →
  `ListItem` (`enumerated` when the source numbered it), `CODE` → `CodeItem`
  (which **inlines** its base fields — it has no `TextItemBase` wrapper),
  `FORMULA` → `FormulaItem`, everything else → `TextItem` with the matching
  `DocItemLabel`. Both `text` and `orig` are set. Per item,
  `meta.custom_fields` carries `xml.path`, plus `xml.role`, `xml.element_id`
  and `xml.ordinal` when the event has them. **No `prov`**: these dialects
  have no pages and no boxes, and the path is the honest locator.
- **Tables.** `table_start` opens one, each `table_row` becomes a grid row,
  `table_end` finalizes `num_rows`/`num_cols` and appends the `TableItem`.
  Both `grid` and the flat `table_cells` are populated; a cell's
  `start/end_row/col_offset_idx` are computed from the running grid position
  honoring the spans already in flight, so a row under a `rowspan` starts at
  the first free column. A `table_start` caption becomes a `CAPTION` text item
  created *first* and referenced from the table's `captions[]`.
- **XBRL facts.** One `TableItem` (`meta.custom_fields["xml.table"] = "facts"`)
  created lazily on the first fact: header row `concept | context | period |
  unit | value | decimals`, one row per fact. Concept is `prefix:localName`,
  period is an instant, an ISO `start/end` interval or `forever`, unit is the
  resolved measures (`numerator/denominator` for a divide unit) or the bare
  `unitRef`. A large instance makes a large table — the row count is bounded
  only by the input, and the request byte cap is what bounds both.

### 4.2 Deliberately not mapped

- **`html_island` events.** An XHTML fragment is the HTML collector's job;
  re-parsing it with an XML stack would produce a worse result than that
  collector gets. The fold counts what it skipped in
  `body.meta.custom_fields["xml.html_islands"]` so the omission is visible.
- **`PictureItem`s.** An XML picture is a filename or an `xlink:href`, never
  pixels, so it stays a text item labelled `PICTURE`. A `PictureItem` with no
  `ImageRef` would claim an image this collector does not have.
- **Unconsumed source attributes** (`include_attributes`). They are an
  inspection aid on the typed stream, not document structure.
- **Warnings and counts** from the trailer. They describe the stream, not the
  document.
- **USPTO claims as list items.** They stream as `TEXT` with `role = "claim"`
  and an ordinal, and that is what they fold to; the claim numbering is in
  `xml.ordinal`.

## 5. Sniffing

Order: request dialect if set → root xmlns → DOCTYPE public id →
well-known root local-name (`article`+JATS ns, `us-patent-grant`,
`xbrl`, DocLang root). Two matches that disagree →
`INVALID_ARGUMENT` with both names in the message.

## 6. Tests

- One fixture per dialect, asserted against a golden `Document`
  (item labels + text, not full protobuf equality).
- XXE payload (`<!DOCTYPE … SYSTEM "file:///etc/passwd">`) → parse
  error, no file read.
- Entity bomb → `RESOURCE_EXHAUSTED` / parse error, bounded time.
- XBRL without taxonomy still returns facts; labels stay local-name.
- Sniff tests for each root; ambiguous tiny `<root/>` fails closed.
