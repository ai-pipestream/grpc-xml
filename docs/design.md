# grpc-xml design

## 1. Goals

- Map the JATS, USPTO, XBRL, and DocLang XML dialects, plus the two
  archive formats that carry XML: `DCLX` (a DocLang OPC zip) and
  `METS_GBS` (a Google Books `tar.gz`).
- One process, one port, format selected on the request.
- Stream elements of large instances (USPTO claims, XBRL facts) as
  table rows / paragraphs rather than materializing a DOM of the whole
  file when the dialect allows it. Live UI is the reason: the client
  paints each yielded item instead of waiting for the whole document. A
  unary Document convenience RPC is allowed; it is not the product path.
- Identical `Document` projection gRParse can merge with a PDF
  collector of the same paper.

## 2. Non-goals (v1)

- Generic XML-to-JSON or XSLT hosting.
- Fetching remote taxonomies, DTDs, or XInclude.
- Decoding archive images. A METS/GBS export and a DCLX archive both carry
  scans; this service maps their XML text planes and counts the image bytes
  against the cap without ever decoding a pixel. Fanning page images out to
  other collectors is a coordinator's job.
- Pretty-printing or canonicalizing the source XML as an output
  format (the sink might, later).

## 3. Wire API (sketch)

`ai.pipestream.xml.v1.XmlParseService`

```text
rpc ParseXml(stream ParseXmlRequest) returns (stream ParseXmlEvent);
rpc GetServiceInfo(GetServiceInfoRequest) returns (ServiceInfo);
```

Options:

- `dialect`: `JATS` / `USPTO` / `XBRL` / `DOCLANG` / `DCLX` /
  `METS_GBS` / `UNSPECIFIED` (sniff)
- `taxonomy`: optional bytes (XBRL only), a zip of schemas/linkbases
- `max_document_mib`

Events:

1. `XmlInfo`: resolved dialect, root namespace, title if known
2. Native content events **or** a single `Document` (v1 can emit
   Document directly; these dialects are declarative and usually
   smaller than office files)
3. `HtmlIsland`: xpath/id + XHTML bytes, for the HTML collector
4. `ParseStatus`

## 4. Mapping to Document

**Implemented in-repo** as of the `emit_document` option: the fold lives in
[`src/document_fold.rs`](../src/document_fold.rs), consumes the same
`ParseXmlResponse` events the service writes, and the server sends the result
as a `document` event immediately before the `status` trailer. The typed
stream stays the product; the Document is a lossy projection of it. The
vendored schema is `proto/ai/pipestream/document/v1/document.proto`, copied
byte-identical from gRParse and never edited here.

The mapping follows the document model, not a 1:1 XML clone:

| Dialect | Typical items |
|---|---|
| JATS | title, authors, abstract, sections, paragraphs, tables, refs |
| USPTO | title, inventors, abstract, claims (numbered), description, drawings as placeholder pictures |
| XBRL | fact table(s): concept, context, unit, value; taxonomy labels when provided |
| DocLang | already-close-to-Document; mostly a typed decode |
| DCLX | the zip's root `document.xml`, mapped exactly as DocLang; images stay compressed |
| METS_GBS | one text item per hOCR `ocr_line`, pages in manifest order, `x_wconf` as source confidence; no geometry, no pixels |

Every item: `CollectorSource.collector = "xml"`, `model` = dialect
name, `version` = this server's version, `confidence` unset, because a
declarative mapping is deterministic and a confidence would be noise. No
fake page bboxes.

### 4.1 What the fold builds

**Shape.** Flat arenas plus refs: `#/texts/N`, `#/pictures/N`, `#/tables/N`,
each item naming its `parent` and each parent listing it in `children`.
`field_regions` and `field_items` stay empty; the coordinator's merge does
not renumber them.

**Nesting: heading-as-parent.** A `SECTION_HEADER` of level N is parented
to the nearest open header of a level below N (`#/body` when there is
none), and every content item after it (text, table, picture) names that
header as its parent. `SectionHeaderItem.level` stays populated even though
the nesting now says the same thing. Content that arrives before the first
heading sits on `#/body`. **No section `GroupItem`s**: the levels come from
the parser rather than from tag names, so there is nothing to fill.

**Identity.** `schema_name` is set from the `SCHEMA_NAME` constant in
`src/document_fold.rs`, the upstream v2 document schema identifier this
plane stays compatible with. `origin.mimetype = "application/xml"`, `name`
= `XmlInfo.title` when the dialect exposed one, otherwise the first `TITLE`
item's text (none of the four dialects currently fill `XmlInfo.title`, so
in practice it is the title item). Root namespace, root local name and
dialect go on the body's `meta.custom_fields` as `xml.root_namespace`,
`xml.root_local_name`, `xml.dialect`. Root meta is **first-writer-wins** in
the coordinator's merge, so treat those as a hint; the per-item
`CollectorSource.model` carries the dialect authoritatively.

**Text items.** `TITLE` → `TitleItem`, `SECTION_HEADER` →
`SectionHeaderItem` (level from the event, defaulting to 1), `LIST_ITEM` →
`ListItem` (`enumerated` when the source numbered it), `CODE` → `CodeItem`
(which **inlines** its base fields; it has no `TextItemBase` wrapper),
`FORMULA` → `FormulaItem`, everything else → `TextItem` with the matching
`DocItemLabel`. Both `text` and `orig` are set. Per item,
`meta.custom_fields` carries `xml.path`, plus `xml.role`, `xml.element_id`
and `xml.ordinal` when the event has them. **No `prov`**: these dialects
have no pages and no boxes, and the path is the honest locator.

**Pictures.** A `PICTURE` event becomes a placeholder `PictureItem` in the
picture arena with `image` unset: no bytes, no uri, no size. An XML picture
is a reference, never pixels. The text such an event carries is never
prose: every dialect lifts it from an attribute (the JATS `xlink:href`, the
USPTO drawing `file`, the DocLang `uri`), so it is a reference and it lands
in `meta.custom_fields["xml.href"]` beside the `xml.path` / `xml.role` /
`xml.element_id` locators the item would have had as a text item.
`captions` stays empty: a figure's real caption reaches the fold as its own
`CAPTION` event.

**Tables.** `table_start` opens one, each `table_row` becomes a grid row,
`table_end` finalizes `num_rows`/`num_cols` and appends the `TableItem`.
Both `grid` and the flat `table_cells` are populated; a cell's
`start/end_row/col_offset_idx` are computed from the running grid position
honoring the spans already in flight, so a row under a `rowspan` starts at
the first free column. A `table_start` caption becomes a `CAPTION` text item
created *first* and referenced from the table's `captions[]`.

**XBRL facts.** One `TableItem` (`meta.custom_fields["xml.table"] = "facts"`)
created lazily on the first fact: header row `concept | context | period |
unit | value | decimals`, one row per fact. Concept is `prefix:localName`,
period is an instant, an ISO `start/end` interval or `forever`, unit is the
resolved measures (`numerator/denominator` for a divide unit) or the bare
`unitRef`. A large instance makes a large table: the row count is bounded
only by the input, and the request byte cap is what bounds both.

### 4.2 Deliberately not mapped

**`html_island` events.** An XHTML fragment is the HTML collector's job;
re-parsing it with an XML stack would produce a worse result than that
collector gets. The fold counts what it skipped in
`body.meta.custom_fields["xml.html_islands"]` so the omission is visible.

**Image payloads.** An XML picture is a filename or an `xlink:href`, never
pixels: the `PictureItem` is a placeholder and its `image` stays unset. This
collector does not fetch what the href names, and never invents an
`ImageRef`.

**Pairing a figure's own caption with its picture.** A `<fig>` caption
reaches the fold as a standalone `CAPTION` event after the graphic, so it
folds as a caption item under the same heading rather than into the picture's
`captions[]`. Attaching it would be a wire-order guess. Nothing else is put
in `captions[]` in its place: the picture event's own text is an attribute
value, and rendering a filename as a caption would be worse than an empty
one.

**Unconsumed source attributes** (`include_attributes`). They are an
inspection aid on the typed stream, not document structure.

**Warnings and counts** from the trailer. They describe the stream, not the
document.

**USPTO claims as list items.** They stream as `TEXT` with `role = "claim"`
and an ordinal, and that is what they fold to; the claim numbering is in
`xml.ordinal`.

## 5. Sniffing

Order: request dialect if set → archive magic bytes (`PK\x03\x04` means
DCLX, `\x1f\x8b` means METS_GBS; checked before any XML is read, since an
archived document is not XML at byte 0) → root xmlns → DOCTYPE public id →
well-known root local-name (`article`+JATS ns, `us-patent-grant`,
`xbrl`, DocLang root). Two matches that disagree →
`INVALID_ARGUMENT` with both names in the message; that includes a stated
dialect against contradicting archive magic, and a stated archive dialect
on a payload without its magic.

## 6. Tests

One fixture per dialect, asserted against a golden `Document` (item labels
+ text, not full protobuf equality). An XXE payload
(`<!DOCTYPE … SYSTEM "file:///etc/passwd">`) must produce a parse error
with no file read. An entity bomb must produce `RESOURCE_EXHAUSTED` or a
parse error in bounded time. XBRL without taxonomy still returns facts,
with labels staying local-name. Sniff tests cover each root, and an
ambiguous tiny `<root/>` fails closed. Archive fixtures are constructed in
the test with the zip/tar/flate2 crates rather than committed as binaries:
happy path per format, a zip or tar that is not the format, a
small-on-the-wire bomb that must trip the inflated-byte cap, and the
explicit-request mismatches in both directions.
