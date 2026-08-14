// SPDX-License-Identifier: Apache-2.0

//! A gRPC collector that maps JATS, USPTO, XBRL and `DocLang` XML into the
//! gRParse Document plane, streaming items as the parser yields them.
//!
//! Design rules, in the order they constrain everything else:
//!
//! - **Live stream is the product.** [`XmlInfo`](proto::v1::XmlInfo) goes out
//!   as soon as the root element is known, content events go out as each
//!   source element closes, and [`ParseStatus`](proto::v1::ParseStatus) is a
//!   trailer carrying counts. Nothing is buffered until the last closing tag.
//! - **Diskless.** Document bytes arrive over the request stream and are fed
//!   straight into the pull parser through an in-memory channel. Nothing is
//!   written, and no complete copy of the document is retained.
//! - **The parser resolves nothing.** No DTD is processed, no external
//!   entity, schema or `XInclude` is fetched, and no general entity is
//!   expanded. See [`security`] for the exact policy and why each half of it
//!   exists.
//!
//! The modules follow the data: [`security`] and [`sniff`] decide whether and
//! how to parse, [`dialect`] holds the per-family mapping rules, [`parse`]
//! is the streaming driver that turns XML events into protobuf events, and
//! [`service`] wires that to tonic. [`document_fold`] is the optional second
//! consumer of that same event stream: it folds it into one
//! `ai.pipestream.document.v1.Document` when a caller asks for one.

pub mod dialect;
pub mod document_fold;
pub mod metrics;
pub mod parse;
pub mod security;
pub mod service;
pub mod sniff;

/// Generated protobuf messages and gRPC stubs for `ai.pipestream.xml.v1`.
///
/// Produced from `proto/` by `build.rs` on every build, so they cannot drift
/// from the schema that `buf lint` gates. The stubs carry no documentation of
/// their own; the commented, linted source of truth is the `.proto` files.
#[allow(missing_docs, clippy::all, clippy::pedantic, clippy::nursery)]
pub mod proto {
    /// Encoded `FileDescriptorSet` for the package, served by gRPC
    /// reflection.
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("descriptor");

    /// Messages, client and server for the `ai.pipestream.xml.v1` package.
    pub mod v1 {
        tonic::include_proto!("ai.pipestream.xml.v1");
    }
}

/// Generated protobuf messages for `ai.pipestream.document.v1`, the Document
/// plane this collector projects into.
///
/// The schema is vendored byte-identical from the gRParse repository and is
/// never edited here; [`document_fold`] is the only thing in this crate that
/// builds one.
///
/// The module sits at the crate root rather than under [`proto`] because
/// prost resolves a cross-package reference by walking up from the
/// referring package's module: the `document` variant generated into
/// `proto::v1::parse_xml_response` names `super::super::super::document::v1`,
/// which is `crate::document::v1`. Moving this module changes nothing but
/// the compile error.
#[allow(missing_docs, clippy::all, clippy::pedantic, clippy::nursery)]
pub mod document {
    /// Messages for the `ai.pipestream.document.v1` package.
    pub mod v1 {
        tonic::include_proto!("ai.pipestream.document.v1");
    }
}

pub use service::XmlGrpc;

/// Version of this server, reported by `GetServiceInfo` and attached to
/// every item's `CollectorSource`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Name of the XML parser this build links, reported by `GetServiceInfo`.
pub const PARSER: &str = "quick-xml 0.41";

/// Value of `CollectorSource.collector` on every item this service produces.
pub const COLLECTOR: &str = "xml";
