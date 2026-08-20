#!/usr/bin/env node
// SPDX-License-Identifier: Apache-2.0
//
// CLI demo: stream one XML document through grpc-xml and print each event as
// it arrives, one line per event.
//
//   node cli.js ../sample-data/jats-article.xml [DIALECT]
//
// DIALECT is an XmlDialect name without the XML_DIALECT_ prefix (JATS, USPTO,
// XBRL, DOCLANG, DCLX, METS_GBS); omitted means "sniff".
// Honours XML_SERVER_ADDR (default 127.0.0.1:50066).

import { readFile } from "node:fs/promises";
import { XmlClient, formatEvent } from "./lib/xml.js";

const [file, dialect] = process.argv.slice(2);
if (!file) {
  console.error("usage: node cli.js <document> [DIALECT]");
  process.exit(2);
}

const bytes = await readFile(file);
const client = new XmlClient();

const options = {
  dialect: dialect ? `XML_DIALECT_${dialect.toUpperCase()}` : "XML_DIALECT_UNSPECIFIED",
  maxDocumentMib: 0,
  emitHtmlIslands: process.env.ISLANDS === "1",
  includeAttributes: process.env.ATTRIBUTES === "1",
  emitDocument: process.env.DOCUMENT === "1",
};

try {
  for await (const event of client.parse(bytes, options)) {
    const line = formatEvent(event);
    if (line) console.log(line);
  }
} catch (err) {
  console.error(`error ${err.code ?? ""} ${err.details ?? err.message}`);
  process.exitCode = 1;
} finally {
  client.close();
}
