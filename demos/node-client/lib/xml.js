// SPDX-License-Identifier: Apache-2.0
//
// Thin wrapper around the ai.pipestream.xml.v1 gRPC contract.
//
// The protos are loaded dynamically from ../../proto (the single source of
// truth in this repository) — no generated code is checked in.

import { fileURLToPath } from "node:url";
import path from "node:path";
import grpc from "@grpc/grpc-js";
import protoLoader from "@grpc/proto-loader";

const PROTO_ROOT = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  "..", "..", "..", "proto",
);

const packageDefinition = protoLoader.loadSync(
  path.join(PROTO_ROOT, "ai", "pipestream", "xml", "v1", "xml_service.proto"),
  {
    includeDirs: [PROTO_ROOT],
    keepCase: false,
    longs: Number,
    enums: String,
    defaults: true,
    oneofs: true,
  },
);

const { ai } = grpc.loadPackageDefinition(packageDefinition);

/** Upload chunk size. Any value gives the same events; this one is quick. */
export const CHUNK_BYTES = 64 * 1024;

/** A connected grpc-xml client. */
export class XmlClient {
  /** @param {string} address host:port of the grpc-xml server. */
  constructor(address = process.env.XML_SERVER_ADDR ?? "127.0.0.1:50066") {
    this.stub = new ai.pipestream.xml.v1.XmlParseService(
      address,
      grpc.credentials.createInsecure(),
    );
  }

  /**
   * Open a ParseXml call and send the options frame.
   *
   * The caller then writes `{ chunk }` frames as the document becomes
   * available and calls `.end()`. This is the shape to use when the document
   * is itself arriving from somewhere, since it never holds the whole thing:
   * `server.js` pipes an HTTP upload straight through it.
   *
   * Note the ordering: options go out before anything reads the response. The
   * server sniffs the dialect from the first bytes it is handed, so the
   * options frame must lead or the call makes no sense at all.
   *
   * @param {object} options a ParseOptions message.
   * @returns {object} the duplex call.
   */
  openParse(options) {
    const call = this.stub.parseXml();
    call.write({ options });
    return call;
  }

  /**
   * Stream a whole in-memory document and yield each event as it arrives.
   *
   * @param {Buffer} bytes the document.
   * @param {object} options a ParseOptions message.
   * @returns {AsyncGenerator<object>} ParseXmlResponse messages.
   */
  async *parse(bytes, options) {
    const call = this.openParse(options);

    for (let at = 0; at < bytes.length; at += CHUNK_BYTES) {
      call.write({ chunk: bytes.subarray(at, at + CHUNK_BYTES) });
    }
    call.end();

    // grpc-js hands events to callbacks; this turns the callback stream into
    // an async iterator without buffering the whole document's worth.
    const queue = [];
    let waiting = null;
    let done = false;
    let failure = null;

    const wake = () => {
      if (waiting) {
        const resolve = waiting;
        waiting = null;
        resolve();
      }
    };
    call.on("data", (event) => { queue.push(event); wake(); });
    call.on("end", () => { done = true; wake(); });
    call.on("error", (err) => { failure = err; done = true; wake(); });

    for (;;) {
      while (queue.length > 0) yield queue.shift();
      if (done) break;
      await new Promise((resolve) => { waiting = resolve; });
    }
    if (failure) throw failure;
  }

  /** The server's identity, limits and dialects. */
  getServiceInfo() {
    return new Promise((resolve, reject) => {
      this.stub.getServiceInfo({}, (err, response) => {
        if (err) reject(err); else resolve(response);
      });
    });
  }

  close() {
    grpc.closeClient(this.stub);
  }
}

/**
 * Render one event as a single stable line.
 *
 * @param {object} response a ParseXmlResponse.
 * @returns {string|null} the line, or null for an event with nothing to say.
 */
export function formatEvent(response) {
  const {
    info, textItem, tableStart, tableRow, tableEnd,
    fact, htmlIsland, document, status,
  } = response;

  if (info) {
    return [
      `info dialect=${info.dialect.replace("XML_DIALECT_", "")}`,
      `evidence=${info.evidence.replace("DIALECT_EVIDENCE_", "")}`,
      `root=${quote(info.rootLocalName)}`,
      info.title ? `title=${quote(info.title)}` : "",
      info.encoding ? `encoding=${info.encoding}` : "",
    ].filter(Boolean).join(" ");
  }
  if (textItem) {
    return [
      `text #${textItem.index}`,
      `label=${textItem.label.replace("XML_ITEM_LABEL_", "")}`,
      textItem.role ? `role=${textItem.role}` : "",
      textItem.level ? `level=${textItem.level}` : "",
      textItem.ordinal ? `ordinal=${textItem.ordinal}` : "",
      `path=${textItem.path}`,
      `value=${quote(clip(textItem.text))}`,
    ].filter(Boolean).join(" ");
  }
  if (tableStart) {
    return [
      `table-start #${tableStart.index} ref=${tableStart.tableRef}`,
      tableStart.caption ? `caption=${quote(tableStart.caption)}` : "",
      `path=${tableStart.path}`,
    ].filter(Boolean).join(" ");
  }
  if (tableRow) {
    const cells = tableRow.cells
      .map((c) => (c.isHeader ? `[${c.text}]` : c.text))
      .join(" | ");
    return `table-row ref=${tableRow.tableRef} row=${tableRow.rowIndex} cells=${quote(cells)}`;
  }
  if (tableEnd) {
    return `table-end ref=${tableEnd.tableRef} rows=${tableEnd.rowCount} cols=${tableEnd.columnCount}`;
  }
  if (fact) {
    const concept = fact.conceptPrefix
      ? `${fact.conceptPrefix}:${fact.conceptLocalName}`
      : fact.conceptLocalName;
    return [
      `fact #${fact.index} ${concept}=${quote(fact.value)}`,
      fact.isNil ? "nil" : "",
      fact.decimals ? `decimals=${fact.decimals}` : "",
      `context=${fact.contextRef}`,
      fact.unitRef ? `unit=${fact.unitRef}` : "",
      `path=${fact.path}`,
    ].filter(Boolean).join(" ");
  }
  if (htmlIsland) {
    const html = Buffer.isBuffer(htmlIsland.html)
      ? htmlIsland.html.toString("utf8")
      : String(htmlIsland.html);
    return `island #${htmlIsland.index} path=${htmlIsland.path} html=${quote(clip(html))}`;
  }
  if (document) {
    return `document name=${quote(document.name ?? "")} texts=${document.texts?.length ?? 0} tables=${document.tables?.length ?? 0}`;
  }
  if (status) {
    const c = status.counts;
    const counts = [
      `texts=${c.textItems}`, `tables=${c.tables}`, `rows=${c.tableRows}`,
      `facts=${c.facts}`, `islands=${c.htmlIslands}`,
    ].join(",");
    const warnings = status.warnings
      .map((w) => `${w.code.replace("WARNING_CODE_", "")}x${w.count}`)
      .join(",");
    return [
      `status dialect=${status.dialect.replace("XML_DIALECT_", "")}`,
      `bytes=${status.bytesConsumed} elapsed=${status.elapsedMillis}ms`,
      `counts=[${counts}]`,
      warnings ? `warnings=[${warnings}]` : "",
    ].filter(Boolean).join(" ");
  }
  // An event this client has no name for. The contract says to ignore those
  // rather than fail: the oneof is the extension point.
  return null;
}

function clip(value, n = 90) {
  return value.length > n ? `${value.slice(0, n)}…` : value;
}

/** Quote a string for a one-line rendering. */
function quote(value) {
  let out = '"';
  for (const ch of value) {
    if (ch === "\\") out += "\\\\";
    else if (ch === '"') out += '\\"';
    else if (ch === "\n") out += "\\n";
    else if (ch === "\r") out += "\\r";
    else if (ch === "\t") out += "\\t";
    else out += ch;
  }
  return `${out}"`;
}
