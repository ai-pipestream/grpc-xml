# Node demo client

Two programs against the same contract: a CLI streamer and a live web viewer.
Stubs are loaded dynamically from [`../../proto`](../../proto) at run time, so
nothing generated is checked in.

```bash
npm install

# CLI
node cli.js ../sample-data/jats-article.xml

# Web viewer, then open http://127.0.0.1:8087
npm start
```

Both honour `XML_SERVER_ADDR` (default `127.0.0.1:50066`). The viewer also
takes `PORT` (default 8087).

### Serving under a base path

Set `UI_BASE` and the whole viewer moves under that prefix, for example behind
the demo shell's reverse proxy, which forwards `/ui/xml/*` unchanged:

```bash
UI_BASE=/ui/xml npm start   # page at http://127.0.0.1:8087/ui/xml/
```

The bridge strips the prefix before routing, so every endpoint lives at
`$UI_BASE/api/*`, and it injects a `<meta name="ui-base">` tag into the served
page, which the page reads to prefix its own `fetch()` calls. Unset, nothing
changes: the bridge answers at the root exactly as before.

## The web viewer

The viewer exists to make one property visible: **document events arrive
before the upload finishes**.

It is a single HTTP request. The browser POSTs the document and reads
Server-Sent Events off the *same* response, which is deliberately the same
shape as the gRPC call underneath it: bytes going one way while typed items
come back the other. Nothing buffers the document. Each upload slice is
written into the gRPC call as it lands, and each event is flushed to the page
as the Rust server emits it.

The page shows an upload bar with a green marker where the first content
event landed, and says so in words:

> First content event after **208 B of 2.3 KiB** (9% uploaded). The rest of
> the document had not been sent yet.

Controls, beyond the document picker:

- **Dialect** pins the mapping, or leaves the server to sniff it from the
  namespace, DOCTYPE or root element. The `info` event reports what it
  decided and on what evidence.
- **XHTML islands** hands XHTML subtrees to the HTML collector as opaque
  fragments instead of flattening them into item text. Compare
  `jats-xhtml-island.xml` with the option on and off.
- **Source attributes** attaches the attributes the mapper did not consume to
  every item.
- **Document fold** additionally emits the whole parse folded into one
  `ai.pipestream.document.v1.Document`, just before the trailer.
- **Upload throttle** sleeps between upload slices. It slows the *upload*
  only. The parser is never waiting on anything but bytes.

Worth trying:

| Document | What you see |
|---|---|
| `jats-article.xml` | title, authors, abstract, nested sections, a captioned table streaming a row at a time |
| `uspto-grant.xml` | a DOCTYPE sniffed by public identifier, numbered claims, an ignored external DTD warning |
| `xbrl-instance.xbrl` | facts arriving with their contexts and units resolved inline, one of them nil |
| `doclang-document.dclg` | a typed decode of label-named elements, ordinals on the list items |
| `jats-xhtml-island.xml` | with islands on, an XHTML fragment handed off rather than flattened |

The fixtures are the ones the integration tests pin, in
[`../../tests/common/mod.rs`](../../tests/common/mod.rs), copied verbatim so
the page and the test suite always agree about what a parse produces.

### A real document

The small fixtures each pin one dialect; none of them show what the service
is actually for. Drop any JATS, USPTO, XBRL or DocLang document worth
megabytes into [`../sample-data/large/`](../sample-data) and it appears in
the dropdown, or use the file picker for something on your disk.

The throughput the page reports is browser time: throttle, SSE bridge, JSON
parsing and rendering all included. It is not what the service does on its
own.

### Why SSE is parsed by hand

`EventSource` only does `GET`, and the whole point is that the upload and the
event stream are one request. So the page reads `response.body` as a stream
and splits frames itself. It is about fifteen lines and it is in `stream()`
in `public/index.html`.

## Things that bite

**Write the options frame before you read the response.** The first request
frame must carry `options` and every later one a `chunk`; the server sniffs
the dialect off the first bytes it is handed. `lib/xml.js` sends options
inside `openParse()` for exactly this reason, so the ordering cannot be got
wrong by a caller.

**Handle the oneof by name, not by guessing.** With `oneofs: true`,
proto-loader sets `message.event` to the name of the active arm. The bridge
forwards `message[message.event]` rather than sniffing which key is
populated, so an arm added to the contract later is passed through to the
page instead of being dropped silently.

**`bytes` fields arrive as Buffers.** The `html_island` payload would JSON
into a page of decimal arrays; the bridge re-encodes it as UTF-8 text before
framing it for the page.

**Backpressure is real and worth keeping.** `res.write()` returning false
means the browser is behind. The bridge pauses the gRPC call and resumes on
`drain`, which propagates through gRPC flow control back to the parser.
Without it, a large document queues the whole event stream in this process.
