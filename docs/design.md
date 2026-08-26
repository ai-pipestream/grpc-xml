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
4. `OutlineItem`: one entry of the source's own table of contents
5. `ParseStatus`

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
| METS_GBS | one text item per hOCR `ocr_line`, pages in manifest order, `x_wconf` as source confidence, per-line and per-word boxes, the `structMap` as the outline and the `dmdSec` as the catalogue record; no pixels |

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

**Lists are groups.** A list is declared by its container, not by its items:
a JATS `list/@list-type` and an `ol` element both say "the items under me
are numbered" without any item saying so. The driver counts the open list
containers on its element stack, so a `LIST_ITEM` carries `list_depth` (1
for a list that is not inside another) and `enumerated` (the kind of the
list it is in, which is a different statement from `ordinal`, the number the
source gave one item). Nothing but a list item carries either.

The fold turns that into one `GROUP_LABEL_LIST` / `GROUP_LABEL_ORDERED_LIST`
`GroupItem` per contiguous list, nested lists as nested groups, each item
parented to its own list. Any content that is not a list item closes the
whole stack, which is what makes a group one contiguous list: a paragraph
between two runs of bullets means two lists, not one. A same-depth list of
the other kind also closes: a numbered list after a bulleted one is a new
list. `ListItem.enumerated` reads the container first and falls back to the
item's own ordinal, because an item the source numbered is in a numbered
list even when the container never said so.

A dialect with no list vocabulary sends no depth, and those items fold as
one top-level list, which is the honest reading of a run of list items with
nothing saying they nest. A list written *inside* an item rather than beside
it (a JATS `list` within a `list-item`) still flattens into that item's
text, because everything inside a capture does; that is the same rule as for
any other nested block structure, not a rule about lists.

**Pages and provenance.** A source is located in whatever space it actually
has, which is what `ProvenanceItem`'s several coordinate slots are for.

A `page` event becomes a `PageItem` in `Document.pages`, keyed by the
manifest `ORDER`, with the extent the hOCR states and `unit = "px"`. A
`TextItem` that carries a `bbox` and a `page_no` gets one `ProvenanceItem`
with that box, `COORD_ORIGIN_TOPLEFT` (which is how hOCR writes coordinates)
and a `charspan` covering the whole item, since a line's box bounds the line
rather than any part of it.

**Word boxes.** hOCR marks each word inside a line with its own box and its
own `x_wconf` (`ocrx_word`), and those boxes are the only thing in the
format that says where *inside* a line a word is: a consumer highlighting a
search hit on the page scan needs the word, and the line's box cannot give
it. `TextItem.words` carries one `WordBox` per marked word: its run in the
item's text (code points, translated onto the collapsed text the same way an
inline run is), its box, and its own confidence when the source states one.
A word with no `bbox` clause states no geometry rather than a fabricated
one, and a word with no `x_wconf` leaves `confidence` unset rather than
claiming certainty, unlike the *line*, which defaults to 1.0 as it always
has. The fold turns each into its own `ProvenanceItem` after the line's,
which is what `prov` being repeated is for: the line entry's `charspan`
covers the item, each word entry's `charspan` is the word, and the pairing
of that span with that box is the whole content of the entry.

The single-document dialects have no page and no box, and they do have the
byte range their element occupies: `TextItem.byte_start` / `byte_end` name
the first byte of the start tag and the byte past the end tag, and they fold
into `ProvenanceItem.byte_range` with `page_no` left at zero, which is the
truth about a document with no pages. Offsets count bytes of the UTF-8 the
parser consumed, which is the source itself for a UTF-8 document and its
decoded form for any other, so a document in another encoding gives a
faithful position in the decoded stream rather than a wrong one in the
source. An item whose text came from an attribute (a `graphic/@href`) claims
no range: its text is not a run of source bytes, and the element's own range
would name bytes the text is not in. The archive dialects claim none either:
their items come from many member documents, so an offset into the uploaded
payload would name a byte in a different file.

**Structured metadata** (`emit_source_metadata`). A `meta_item` event folds
into whatever the Document schema has a field for: a publication date
becomes the document's creation declaration and a revision date its
modification declaration, in three fields that say three different things:
`created_civil` always, because an XML publication date is a wall-clock date
with no time zone in it; `created` only for a whole calendar date, read as
midnight UTC; `created_raw` for the source's own spelling as far as it goes
(a `pub-date` with only a year yields `2026`, never a fabricated first of
January). A cited-reference entry becomes a `REFERENCE` item like any other
bibliography entry. Identifiers, classification codes, licence terms and
funding awards land on `source_meta.identifiers`, `.classifications`,
`.license` and `.funding` field for field. Nothing here goes through
`custom_fields`: a CPC code is a scheme and a code, not a string.

**The volume's own contents page.** A METS `structMap` is a labelled tree
over the pages: the divisions a cataloguer identified and the page each
begins on. That is the book's own table of contents, stated in the manifest
rather than recovered from the text, so it streams as its own `OutlineItem`
event and folds into `Document.outline`. A division the source labelled is
an entry; one it did not label is structure holding the labelled ones, and
inventing a title for it would put a name in the contents page the book
never had. Entries carry no `target`: an entry names a *page*, and a page is
not an item in any arena, so a `FineRef` would point at nothing. The
manifest is read whole before the first page is, so the outline goes out
before the first line rather than after the last.

**The catalogue record.** A METS `dmdSec` holds a Dublin Core or MODS
record. Its leaf statements are read into the shapes that already exist for
them: a date the source wrote as a calendar date becomes a `MetaDate` (prose
about a date stays prose rather than being coerced into numbers), an
`identifier` becomes a `MetaIdentifier` with the scheme its `@type` names,
a `rights` statement becomes a `MetaLicense`, and everything else becomes a
`MetaDescriptive` naming the field. Only the innermost element that states a
field counts, so a MODS `titleInfo/title` and `subject/topic` are one
statement each rather than two. The record is gated by
`emit_source_metadata` like every other metadata decode, and a record left
undecoded is named on the trailer so the omission is visible.

On the Document plane a statement lands in the typed slot `DocumentMeta` has
for it (`title`, `authors` for a `creator` or a `contributor`, `keywords`
because a catalogue `subject` is a topical term and there may be several,
which the single-string `subject` field could not hold, and `language`), and
in `extra`
only when the schema has no field at all, which today means `publisher` and
`description`. `extra` is documented as being for genuinely open vocabulary,
so nothing with a slot goes in it.

**Source metadata.** `Document.source_meta` carries what the document
declares about itself in typed slots: `title` from the `TITLE` item,
`language` from the root `xml:lang`, `keywords` from `role = "keyword"`
items, `authors` from `role = "author" | "contributor" | "inventor"`. The
abstract becomes `body.meta.summary`, quoted rather than generated. It is
attached only when the source said something, because an all-default
message would claim an empty declaration rather than an absent one. The
root's `xsi:schemaLocation` goes to `source_meta.schema_location` in the
source's own spelling and to `source_meta.schema_locations` as the decoded
namespace-and-location pairs; the namespace bindings go to
`source_meta.namespaces`, which is what makes a `xml.path` written in
qualified names resolvable from the Document alone.

**Identity.** `schema_name` is set from the `SCHEMA_NAME` constant in
`src/document_fold.rs`, the upstream v2 document schema identifier this
plane stays compatible with. `origin.mimetype = "application/xml"` (the
archive dialects state the archive's own type), with
`origin.mimetype_evidence` naming the signal that resolved the dialect the
mimetype is derived from: `requested`, `root-namespace`, `public-id`,
`root-element` or `archive-magic`. `name`
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
and `xml.ordinal` when the event has them.

The element the item was read from has its own slots and is **not** among
those custom fields. `TextItemBase.source_element_name` is its qualified
name as written, and `source_namespace` its resolved namespace URI,
recorded **only when it differs from the root's**, which the body meta
already states. A `mml:math` inside a JATS article is the case worth
stating; repeating one URI on every item of a single-namespace document
would be noise. No item writes the identity in both places.

Two messages are the exception, and deliberately so: `CodeItem` and
`PictureItem` inline their own base-field set rather than wrapping a
`TextItemBase`, and the schema did not extend either with those slots. For
those two the identity stays in `xml.element_name` / `xml.namespace`
alongside the other locators, because the fold has nowhere typed to put it
and dropping it would be worse than the asymmetry.

`xml.from_cdata` stays a custom field on every item kind. It is a fact
about how the parser read the text rather than about the document, and it
is written only when true, so its absence means ordinary character data
rather than a false entry on every item.

**Inline runs** (`emit_inline_spans`). `TextItem.spans` folds onto
`TextItemBase.spans`: styles onto `Formatting` (including its `monospace`,
`small_caps` and `math` bits), an `ext-link` href onto
`InlineSpan.hyperlink`, and each `xref`/`claim-ref` identifier onto its own
`InlineSpan.target`, with `reference_kind` naming what it points at and
`InlineSpan.reference` keeping the source's key verbatim. The two reference
vocabularies match member for member, so every kind a dialect states lands;
only a source that stated no kind leaves `reference_kind` unset, which
reads as "not distinguished". The key is kept whether or not the target
resolves, so an unresolved reference is legible without taking the ref
string apart. Targets are
written as the source name (`#b1`) while the parse runs and are resolved
onto item refs when the stream ends, because a citation normally names an
entry that arrives after it; an identifier the document never defines keeps
its `#name` form. Every item that declared an identifier becomes a
`Document.anchors` entry, which is what makes that resolution possible in
both directions.

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

A cell is a capture like any other. Its markup flattens into the cell's text
and the dialect's inline vocabulary leaves a run over it, so `TableCell.spans`
folds onto `TableCell.spans` on the Document plane exactly as a paragraph's
runs fold onto `TextItemBase.spans`: styles onto `Formatting`, an `ext-link`
onto `hyperlink`, an `xref` onto a `target` with its `reference_kind` and its
key. A cross-reference inside a cell resolves in the same end-of-stream pass
a paragraph's does: a citation in a cell names the same reference list, and
both `grid` and `table_cells` are rewritten so a reader of either sees the
same graph. Runs are gated by `emit_inline_spans` in a cell as everywhere
else.

**Column geometry.** A CALS `colspec` and an XHTML `col` declare the column
rather than any cell in it, and both are children of the table element, so
they are read *after* `TableStart` has streamed. They ride on `TableEnd`
instead: holding the table back until the geometry is complete would trade
the live stream for one attribute. An XHTML `col span="3"` is expanded so
index N of the list is column N either way, under a bound (4096) that a real
table is orders of magnitude below and a hostile `span` attribute is not.
The fold lands them on `TableData.columns`, one `TableColumnSchema` per
declared column: the name when the source declares one (presence-tracked, so
an unnamed column says nothing rather than `""`), the width verbatim in
`width_raw` because a CALS `2*` is a share of the table and a `30%` a share
of something else again, and neither is the page unit `width` means, plus the
two alignment axes in their own fields. A cell's own `@align`/`@valign`
override the column's and land on the cell. `ALIGNMENT_CHAR`, which CALS
declares and the Document plane has no member for, stays on the typed wire
and leaves the projection's slot unset rather than becoming a different
alignment.

**XBRL facts.** One `TableItem` (`meta.custom_fields["xml.table"] = "facts"`)
created lazily on the first fact: header row `concept | context | period |
unit | value | decimals`, one row per fact. Concept is `prefix:localName`,
period is an instant, an ISO `start/end` interval or `forever`, unit is the
resolved measures (`numerator/denominator` for a divide unit) or the bare
`unitRef`. A large instance makes a large table: the row count is bounded
only by the input, and the request byte cap is what bounds both.

### 4.2 Deliberately not mapped

**`html_island` content.** An XHTML fragment is the HTML collector's job;
re-parsing it with an XML stack would produce a worse result than that
collector gets. The *content* is what is not mapped, not the fact of it: the
fold emits a placeholder `GroupItem` at the island's position, named
`html-island` in `name` and `label_raw` (no `GroupLabel` fits an island, and
claiming one would say what it is not) and carrying the same `xml.` locators
every other item carries. The group rather than a text item, because a
container is what a group is for, and its place among the body's children is
the reading-order fact: a reader can tell a paragraph that followed an
island from one that followed nothing.

Inside the group sits one `TextItem` holding what the island says.
`TextItemBase.raw` is the fragment as the source wrote it, and `text` is its
character data, which is the projection of that raw the schema documents the
field as pairing with. Pulling text nodes out of XHTML is the same
well-defined operation this parser performs on every other element, so an
island's words are no longer absent from the plane. Its **structure** still
is: which runs were headings, list items or cells is the HTML collector's
answer to give, which is what the attachment below is for. The text carries
one space at every element boundary, unconditionally, because this parser
does not know which XHTML elements are inline: separating always puts a
space where a renderer would not, joining always would invent words the
document does not contain, and the second is the worse failure and the
silent one.

Each island also becomes a `SubDocumentRef` in `Document.attachments`, which
the schema documents as a nested payload addressable for fan-out parsing,
which is precisely what an island is. `item_ref` points back at the
placeholder, so a coordinator that hands the fragment to the HTML collector
knows where the result belongs, and `integrity_errors` holds that pointer to
the same merge contract as every other ref. The count stays in
`body.meta.custom_fields["xml.html_islands"]`.

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

**Processing instructions.** An instruction is addressed to an application,
and this service is not that application: acting on one is exactly the
"resolve what the document asks for" the security policy refuses. It is
still content the source put there, so it is no longer dropped in silence:
`WARNING_CODE_UNMAPPED_ELEMENT` names its target on the trailer, in the
prolog as well as in the body. Only the target is named: an instruction's
data is arbitrarily long and the warning table aggregates by message, so
carrying the body would let one document mint a warning kind per
instruction. A comment stays silent, because a comment is an author's note
to another author rather than a construct with a mapping.

**XBRL facts.** One table, created when the first fact arrives. Eleven
columns — `concept`, `entity_scheme`, `entity`, `context`, `period`, `unit`,
`value`, `decimals`, `precision`, `sign`, `nil` — declared in
`TableData.columns` with the type each holds. The cells that have a machine
value carry it in `TableCell.value`: the fact's number with the source's
sign applied, `decimals` and `precision` as numbers (`INF` is the floating
point infinity, not a marker), and `sign` and `nil` as booleans. A fact's
segment and scenario axes become a `KeyValueItem` with real `GraphData`,
one KEY cell per axis linked to the VALUE cell holding its member, and the
fact row's `context` cell points at it. An XBRL footnote is narrative a
filer attached to a number, so it folds as a `FOOTNOTE` item; a label names
a concept in a schema this service never reads and stays on the wire.

### 4.3 A boundary, not a deferral: XBRL label linkbases

`Fact.label` is the concept's local name and always will be in this service.
Resolving a taxonomy label linkbase means reading the taxonomy: a set of
schema and linkbase documents that live outside the instance, are referenced
by URI, and in practice are fetched from the web or unpacked from a
distribution. This service is diskless and fetches nothing (see the security
posture in `architecture.md`), so it has nothing to resolve against, and a
`taxonomy` blob on the request is accepted only to be reported as unused via
`WARNING_CODE_TAXONOMY_IGNORED`.

That is a boundary of what this process owns, not work postponed. Label
resolution belongs to a stage that has the taxonomy: the wire carries the
concept namespace, prefix and local name on every fact, which is everything
such a stage needs to attach a label afterwards. Nothing here should grow a
taxonomy reader.

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
