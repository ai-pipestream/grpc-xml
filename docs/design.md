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

Follow Docling's backends, not a 1:1 XML clone:

| Dialect | Typical items |
|---|---|
| JATS | title, authors, abstract, sections, paragraphs, tables, refs |
| USPTO | title, inventors, abstract, claims (numbered), description, drawings as pictures if present as embedded images |
| XBRL | fact table(s): concept, context, unit, value; taxonomy labels when provided |
| DocLang | already-close-to-Document; mostly a typed decode |

Every item: `CollectorSource.collector = "xml"`, `model` = dialect
name. No fake page bboxes. If the XML carries named coordinates
(USPTO drawings), keep them with an explicit origin.

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
