use super::client::OpenSearchClient;
use super::config::{DEFAULT_PAGE_SIZE, OpenSearchConfig, PAGE_LIMIT};
use super::error::{Error, Result};
use crate::entry::ManagedEntry;
use crate::protocol::{
    AsyncCull, AsyncDestroyCollection, AsyncDestroyStore, AsyncEnumerateCollections,
    AsyncEnumerateKeys, AsyncKeyValue,
};
use crate::value::Value;
use async_trait::async_trait;
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use bytes::Bytes;
use opensearch::indices::{IndicesDeleteParts, IndicesGetParts};
use opensearch::params::{Conflicts, Refresh};
use opensearch::{
    BulkOperation, BulkParts, ClearScrollParts, DeleteByQueryParts, GetParts, MgetParts,
    ScrollParts, SearchParts,
};
use serde::de::IgnoredAny;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

const INDEX_MARKER: &str = "-okv1-";
const DOCUMENT_ID_MARKER: &str = "okv1-";
const MAX_INDEX_BYTES: usize = 255;
const MAX_DOCUMENT_ID_BYTES: usize = 512;

fn validate_index_prefix(prefix: &str) -> Result<()> {
    let Some(first) = prefix.bytes().next() else {
        return Err(Error::InvalidKey(
            "OpenSearch index prefix must not be empty".to_string(),
        ));
    };
    if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
        return Err(Error::InvalidKey(format!(
            "OpenSearch index prefix must start with a lowercase ASCII letter or digit: {prefix:?}"
        )));
    }
    if !prefix.bytes().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
    }) {
        return Err(Error::InvalidKey(format!(
            "OpenSearch index prefix contains unsupported characters: {prefix:?}"
        )));
    }
    if prefix.len() + INDEX_MARKER.len() > MAX_INDEX_BYTES {
        return Err(Error::InvalidKey(format!(
            "OpenSearch index prefix is too long: empty collection index would be {} bytes (max {MAX_INDEX_BYTES})",
            prefix.len() + INDEX_MARKER.len()
        )));
    }
    Ok(())
}

fn encode_index_name(prefix: &str, collection: &str) -> Result<String> {
    validate_index_prefix(prefix)?;
    let final_len = prefix.len() + INDEX_MARKER.len() + collection.len() * 2;
    if final_len > MAX_INDEX_BYTES {
        return Err(Error::InvalidKey(format!(
            "OpenSearch collection identity encodes to {final_len} index bytes (max {MAX_INDEX_BYTES})"
        )));
    }

    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut index = String::with_capacity(final_len);
    index.push_str(prefix);
    index.push_str(INDEX_MARKER);
    for byte in collection.as_bytes() {
        index.push(HEX[(byte >> 4) as usize] as char);
        index.push(HEX[(byte & 0x0f) as usize] as char);
    }
    Ok(index)
}

fn decode_index_name(prefix: &str, index: &str) -> Result<String> {
    validate_index_prefix(prefix)?;
    if index.len() > MAX_INDEX_BYTES {
        return Err(Error::InvalidKey(format!(
            "OpenSearch index name is {} bytes (max {MAX_INDEX_BYTES}): {index:?}",
            index.len()
        )));
    }

    let encoded_prefix = format!("{prefix}{INDEX_MARKER}");
    let encoded = index.strip_prefix(&encoded_prefix).ok_or_else(|| {
        Error::InvalidKey(format!(
            "OpenSearch index is not a canonical OpenKeyV collection identity: {index:?}"
        ))
    })?;
    if encoded.len() % 2 != 0 {
        return Err(Error::InvalidKey(format!(
            "OpenSearch index has odd-length collection hex: {index:?}"
        )));
    }

    let mut bytes = Vec::with_capacity(encoded.len() / 2);
    for pair in encoded.as_bytes().chunks_exact(2) {
        let high = match pair[0] {
            b'0'..=b'9' => pair[0] - b'0',
            b'a'..=b'f' => pair[0] - b'a' + 10,
            _ => {
                return Err(Error::InvalidKey(format!(
                    "OpenSearch index contains non-lowercase-hex collection data: {index:?}"
                )));
            }
        };
        let low = match pair[1] {
            b'0'..=b'9' => pair[1] - b'0',
            b'a'..=b'f' => pair[1] - b'a' + 10,
            _ => {
                return Err(Error::InvalidKey(format!(
                    "OpenSearch index contains non-lowercase-hex collection data: {index:?}"
                )));
            }
        };
        bytes.push((high << 4) | low);
    }

    let collection = String::from_utf8(bytes).map_err(|error| {
        Error::InvalidKey(format!(
            "OpenSearch index contains invalid UTF-8 collection data {index:?}: {error}"
        ))
    })?;
    if encode_index_name(prefix, &collection)? != index {
        return Err(Error::InvalidKey(format!(
            "OpenSearch index is not canonical: {index:?}"
        )));
    }
    Ok(collection)
}

fn index_pattern(prefix: &str) -> Result<String> {
    validate_index_prefix(prefix)?;
    Ok(format!("{prefix}-*"))
}

fn encode_document_id(key: &str) -> Result<String> {
    let encoded = URL_SAFE_NO_PAD.encode(key.as_bytes());
    let final_len = DOCUMENT_ID_MARKER.len() + encoded.len();
    if final_len > MAX_DOCUMENT_ID_BYTES {
        return Err(Error::InvalidKey(format!(
            "OpenSearch key identity encodes to {final_len} document ID bytes (max {MAX_DOCUMENT_ID_BYTES})"
        )));
    }
    Ok(format!("{DOCUMENT_ID_MARKER}{encoded}"))
}

fn decode_document_id(document_id: &str) -> Result<String> {
    if document_id.len() > MAX_DOCUMENT_ID_BYTES {
        return Err(Error::InvalidKey(format!(
            "OpenSearch document ID is {} bytes (max {MAX_DOCUMENT_ID_BYTES}): {document_id:?}",
            document_id.len()
        )));
    }
    let encoded = document_id
        .strip_prefix(DOCUMENT_ID_MARKER)
        .ok_or_else(|| {
            Error::InvalidKey(format!(
                "OpenSearch document ID is not a canonical OpenKeyV key identity: {document_id:?}"
            ))
        })?;
    let bytes = URL_SAFE_NO_PAD.decode(encoded).map_err(|error| {
        Error::InvalidKey(format!(
            "OpenSearch document ID contains invalid unpadded Base64URL {document_id:?}: {error}"
        ))
    })?;
    if URL_SAFE_NO_PAD.encode(&bytes) != encoded {
        return Err(Error::InvalidKey(format!(
            "OpenSearch document ID is not canonical unpadded Base64URL: {document_id:?}"
        )));
    }
    String::from_utf8(bytes).map_err(|error| {
        Error::InvalidKey(format!(
            "OpenSearch document ID contains invalid UTF-8 key data {document_id:?}: {error}"
        ))
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenSearchDoc {
    entry: String,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_millis"
    )]
    expires_at: Option<i64>,
}

fn deserialize_optional_millis<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    i64::deserialize(deserializer).map(Some)
}

#[derive(Debug, Deserialize)]
struct OpenSearchErrorResponse {
    error: OpenSearchErrorBody,
    status: u16,
}

#[derive(Debug, Deserialize)]
struct OpenSearchErrorBody {
    #[serde(rename = "type")]
    kind: String,
    reason: String,
}

impl OpenSearchErrorBody {
    fn is_index_not_found(&self) -> bool {
        self.kind == "index_not_found_exception"
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum GetBody {
    Document(GetResponse),
    Error(OpenSearchErrorResponse),
}

#[derive(Debug, Deserialize)]
struct GetResponse {
    #[serde(rename = "_id")]
    id: String,
    found: bool,
    #[serde(rename = "_source", default)]
    source: Option<OpenSearchDoc>,
}

#[derive(Serialize)]
struct MgetRequest<'a> {
    ids: Vec<&'a str>,
}

#[derive(Debug, Deserialize)]
struct MgetResponse {
    docs: Vec<MgetDocument>,
}

#[derive(Debug, Deserialize)]
struct MgetDocument {
    #[serde(rename = "_id")]
    id: String,
    #[serde(default)]
    found: Option<bool>,
    #[serde(rename = "_source", default)]
    source: Option<OpenSearchDoc>,
    #[serde(default)]
    error: Option<OpenSearchErrorBody>,
}

#[derive(Debug, Deserialize)]
struct ShardSummary {
    total: u64,
    successful: u64,
    #[serde(default)]
    skipped: u64,
    failed: u64,
}

#[derive(Debug, Deserialize)]
struct BulkResponse {
    errors: bool,
    items: Vec<BulkItem>,
}

#[derive(Debug, Deserialize)]
struct BulkItem {
    #[serde(default)]
    index: Option<BulkItemResult>,
    #[serde(default)]
    delete: Option<BulkItemResult>,
}

#[derive(Debug, Deserialize)]
struct BulkItemResult {
    #[serde(rename = "_id")]
    id: String,
    status: u16,
    #[serde(default)]
    result: Option<String>,
    #[serde(default)]
    error: Option<OpenSearchErrorBody>,
    #[serde(rename = "_shards", default)]
    shards: Option<ShardSummary>,
}

#[derive(Debug, Deserialize)]
struct SearchResponse {
    timed_out: bool,
    #[serde(rename = "_shards")]
    shards: ShardSummary,
    hits: SearchHits,
}

#[derive(Debug, Deserialize)]
struct ScrollSearchResponse {
    #[serde(rename = "_scroll_id")]
    scroll_id: String,
    timed_out: bool,
    #[serde(rename = "_shards")]
    shards: ShardSummary,
    hits: SearchHits,
}

#[derive(Debug, Deserialize)]
struct ClearScrollResponse {
    succeeded: bool,
    num_freed: u64,
}

#[derive(Debug, Deserialize)]
struct SearchHits {
    hits: Vec<SearchHit>,
}

#[derive(Debug, Deserialize)]
struct SearchHit {
    #[serde(rename = "_id")]
    id: String,
}

#[derive(Deserialize)]
struct DeleteByQueryResponse {
    timed_out: bool,
    total: u64,
    deleted: u64,
    version_conflicts: u64,
    noops: u64,
    failures: Vec<IgnoredAny>,
}

#[derive(Debug, Deserialize)]
struct AcknowledgedResponse {
    acknowledged: bool,
}

/// OpenSearch-backed key-value store.
///
/// Each collection maps to a lowercase, reversible OpenSearch index transport.
/// Each document stores one canonical Base64 `OKVE1` entry and optional
/// numeric expiration metadata used only for indexed expiration queries.
pub struct OpenSearchStore {
    client: OpenSearchClient,
    config: OpenSearchConfig,
}

impl OpenSearchStore {
    pub fn new(client: opensearch::OpenSearch, index_prefix: impl Into<String>) -> Self {
        Self::with_config(client, OpenSearchConfig::new(index_prefix))
    }

    pub async fn from_url(url: impl Into<String>, index_prefix: impl Into<String>) -> Result<Self> {
        let url = url.into();
        let transport =
            opensearch::http::transport::Transport::single_node(&url).map_err(|error| {
                Error::StoreSetup {
                    message: format!("failed to create OpenSearch transport for {url}: {error}"),
                }
            })?;
        Ok(Self::new(
            opensearch::OpenSearch::new(transport),
            index_prefix,
        ))
    }

    pub fn with_config(client: opensearch::OpenSearch, config: OpenSearchConfig) -> Self {
        Self {
            client: OpenSearchClient::new(client),
            config,
        }
    }

    fn collection_name<'a>(&'a self, collection: Option<&'a str>) -> &'a str {
        collection.unwrap_or(&self.config.default_collection)
    }

    fn index_name(&self, collection: &str) -> Result<String> {
        encode_index_name(&self.config.index_prefix, collection)
    }

    fn index_pattern(&self) -> Result<String> {
        index_pattern(&self.config.index_prefix)
    }

    fn os(&self) -> &opensearch::OpenSearch {
        self.client.client()
    }

    fn entry_to_doc(entry: &ManagedEntry) -> OpenSearchDoc {
        OpenSearchDoc {
            entry: STANDARD.encode(entry.encode()),
            expires_at: entry
                .expires_at
                .map(|expires_at| expires_at.timestamp_millis()),
        }
    }

    fn doc_to_entry(index: &str, key: &str, doc: OpenSearchDoc) -> Result<ManagedEntry> {
        let encoded = STANDARD.decode(doc.entry.as_bytes()).map_err(|error| {
            Error::Deserialization(format!(
                "failed to decode Base64 OpenSearch entry {index}/{key}: {error}"
            ))
        })?;
        if STANDARD.encode(&encoded) != doc.entry {
            return Err(Error::Deserialization(format!(
                "OpenSearch entry {index}/{key} is not canonical padded standard Base64"
            )));
        }

        let entry = ManagedEntry::decode(Bytes::from(encoded)).map_err(|error| {
            Error::Deserialization(format!(
                "failed to decode OpenKeyV entry in OpenSearch document {index}/{key}: {error}"
            ))
        })?;
        let embedded_expires_at = entry
            .expires_at
            .map(|expires_at| expires_at.timestamp_millis());
        if doc.expires_at != embedded_expires_at {
            return Err(Error::Deserialization(format!(
                "OpenSearch document {index}/{key} expiration metadata does not match its OpenKeyV entry"
            )));
        }
        Ok(entry)
    }

    async fn owned_indices(&self) -> Result<Vec<(String, String)>> {
        let pattern = self.index_pattern()?;
        let response = self
            .os()
            .indices()
            .get(IndicesGetParts::Index(&[&pattern]))
            .allow_no_indices(true)
            .ignore_unavailable(true)
            .send()
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to enumerate OpenSearch indices for {pattern}: {error}"),
            })?;
        let status = response.status_code();
        if !status.is_success() {
            let error = response
                .json::<OpenSearchErrorResponse>()
                .await
                .map_err(|decode_error| Error::Deserialization(format!(
                    "failed to decode OpenSearch index-enumeration error response for {pattern}: {decode_error}"
                )))?;
            if status.as_u16() == 404 && error.status == 404 && error.error.is_index_not_found() {
                return Ok(Vec::new());
            }
            return Err(Error::StoreConnection {
                message: format!(
                    "OpenSearch index enumeration for {pattern} failed with HTTP {} / body status {}: {}: {}",
                    status.as_u16(),
                    error.status,
                    error.error.kind,
                    error.error.reason
                ),
            });
        }

        let indices = response
            .json::<BTreeMap<String, IgnoredAny>>()
            .await
            .map_err(|error| {
                Error::Deserialization(format!(
                    "failed to decode OpenSearch index-enumeration response for {pattern}: {error}"
                ))
            })?;
        indices
            .into_keys()
            .map(|index| {
                let collection = decode_index_name(&self.config.index_prefix, &index)?;
                Ok((index, collection))
            })
            .collect()
    }

    async fn clear_scroll(&self, scroll_id: &str) -> Result<()> {
        let response = self
            .os()
            .clear_scroll(ClearScrollParts::None)
            .body(serde_json::json!({ "scroll_id": scroll_id }))
            .send()
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to clear OpenSearch scroll {scroll_id}: {error}"),
            })?;
        let status = response.status_code();
        if !status.is_success() {
            let error = response
                .json::<OpenSearchErrorResponse>()
                .await
                .map_err(|decode_error| Error::Deserialization(format!(
                    "failed to decode OpenSearch clear-scroll error response for {scroll_id}: {decode_error}"
                )))?;
            return Err(Error::StoreConnection {
                message: format!(
                    "OpenSearch clear-scroll for {scroll_id} failed with HTTP {} / body status {}: {}: {}",
                    status.as_u16(),
                    error.status,
                    error.error.kind,
                    error.error.reason
                ),
            });
        }

        let body = response
            .json::<ClearScrollResponse>()
            .await
            .map_err(|error| {
                Error::Deserialization(format!(
                    "failed to decode OpenSearch clear-scroll response for {scroll_id}: {error}"
                ))
            })?;
        if !body.succeeded {
            return Err(Error::StoreConnection {
                message: format!(
                    "OpenSearch did not clear scroll {scroll_id}; num_freed={}",
                    body.num_freed
                ),
            });
        }
        Ok(())
    }

    async fn validate_document_ids(&self, index: &str) -> Result<bool> {
        let response = self
            .os()
            .search(SearchParts::Index(&[index]))
            .scroll("1m")
            .body(serde_json::json!({
                "query": { "match_all": {} },
                "sort": ["_doc"],
                "size": PAGE_LIMIT,
                "_source": false
            }))
            .send()
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!(
                    "failed to start OpenSearch document-ID validation for {index}: {error}"
                ),
            })?;
        let status = response.status_code();
        if !status.is_success() {
            let error = response
                .json::<OpenSearchErrorResponse>()
                .await
                .map_err(|decode_error| Error::Deserialization(format!(
                    "failed to decode OpenSearch document-ID validation error response for {index}: {decode_error}"
                )))?;
            if status.as_u16() == 404 && error.status == 404 && error.error.is_index_not_found() {
                return Ok(false);
            }
            return Err(Error::StoreConnection {
                message: format!(
                    "OpenSearch document-ID validation for {index} failed with HTTP {} / body status {}: {}: {}",
                    status.as_u16(),
                    error.status,
                    error.error.kind,
                    error.error.reason
                ),
            });
        }

        let mut page = response
            .json::<ScrollSearchResponse>()
            .await
            .map_err(|error| {
                Error::Deserialization(format!(
                    "failed to decode OpenSearch document-ID validation response for {index}: {error}"
                ))
            })?;
        if page.scroll_id.is_empty() {
            return Err(Error::Deserialization(format!(
                "OpenSearch document-ID validation for {index} returned an empty scroll ID"
            )));
        }
        let mut scroll_id = page.scroll_id.clone();

        let scan_result = async {
            loop {
                if page.timed_out
                    || page.shards.failed != 0
                    || page.shards.successful + page.shards.skipped > page.shards.total
                {
                    return Err(Error::StoreConnection {
                        message: format!(
                            "OpenSearch document-ID validation for {index} was incomplete: timed_out={}, shards total={}, successful={}, skipped={}, failed={}",
                            page.timed_out,
                            page.shards.total,
                            page.shards.successful,
                            page.shards.skipped,
                            page.shards.failed
                        ),
                    });
                }
                for hit in &page.hits.hits {
                    decode_document_id(&hit.id)?;
                }
                if page.hits.hits.is_empty() {
                    break;
                }

                let response = self
                    .os()
                    .scroll(ScrollParts::None)
                    .body(serde_json::json!({
                        "scroll": "1m",
                        "scroll_id": scroll_id
                    }))
                    .send()
                    .await
                    .map_err(|error| Error::StoreConnection {
                        message: format!(
                            "failed to continue OpenSearch document-ID validation for {index}: {error}"
                        ),
                    })?;
                let status = response.status_code();
                if !status.is_success() {
                    let error = response
                        .json::<OpenSearchErrorResponse>()
                        .await
                        .map_err(|decode_error| Error::Deserialization(format!(
                            "failed to decode OpenSearch scroll error response for {index}: {decode_error}"
                        )))?;
                    return Err(Error::StoreConnection {
                        message: format!(
                            "OpenSearch scroll for {index} failed with HTTP {} / body status {}: {}: {}",
                            status.as_u16(),
                            error.status,
                            error.error.kind,
                            error.error.reason
                        ),
                    });
                }
                page = response
                    .json::<ScrollSearchResponse>()
                    .await
                    .map_err(|error| {
                        Error::Deserialization(format!(
                            "failed to decode OpenSearch scroll response for {index}: {error}"
                        ))
                    })?;
                if page.scroll_id.is_empty() {
                    return Err(Error::Deserialization(format!(
                        "OpenSearch document-ID validation for {index} returned an empty scroll ID"
                    )));
                }
                scroll_id = page.scroll_id.clone();
            }
            Ok(())
        }
        .await;

        let clear_result = self.clear_scroll(&scroll_id).await;
        match (scan_result, clear_result) {
            (Ok(()), Ok(())) => Ok(true),
            (Err(scan_error), Ok(())) => Err(scan_error),
            (Ok(()), Err(clear_error)) => Err(clear_error),
            (Err(scan_error), Err(clear_error)) => Err(Error::StoreConnection {
                message: format!(
                    "OpenSearch document-ID validation for {index} failed: {scan_error}; scroll cleanup also failed: {clear_error}"
                ),
            }),
        }
    }

    async fn read_entry(&self, index: &str, key: &str) -> Result<Option<ManagedEntry>> {
        let response = self
            .os()
            .get(GetParts::IndexId(index, key))
            .send()
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to read OpenSearch document {index}/{key}: {error}"),
            })?;
        let status = response.status_code();
        let body = response.json::<GetBody>().await.map_err(|error| {
            Error::Deserialization(format!(
                "failed to decode OpenSearch read response for {index}/{key}: {error}"
            ))
        })?;

        let source = match (status.is_success(), body) {
            (true, GetBody::Document(document)) => {
                if document.id != key {
                    return Err(Error::StoreConnection {
                        message: format!(
                            "OpenSearch read for {index}/{key} returned document id {}",
                            document.id
                        ),
                    });
                }
                if !document.found {
                    return Err(Error::StoreConnection {
                        message: format!(
                            "OpenSearch returned a successful response with found=false for {index}/{key}"
                        ),
                    });
                }
                document.source.ok_or_else(|| {
                    Error::Deserialization(format!(
                        "OpenSearch document {index}/{key} is missing _source"
                    ))
                })?
            }
            (false, GetBody::Document(document))
                if status.as_u16() == 404 && document.id == key && !document.found =>
            {
                if document.source.is_some() {
                    return Err(Error::Deserialization(format!(
                        "missing OpenSearch document {index}/{key} unexpectedly contained _source"
                    )));
                }
                return Ok(None);
            }
            (false, GetBody::Error(error))
                if status.as_u16() == 404
                    && error.status == 404
                    && error.error.is_index_not_found() =>
            {
                return Ok(None);
            }
            (_, GetBody::Error(error)) => {
                return Err(Error::StoreConnection {
                    message: format!(
                        "OpenSearch read for {index}/{key} failed with HTTP {} / body status {}: {}: {}",
                        status.as_u16(),
                        error.status,
                        error.error.kind,
                        error.error.reason
                    ),
                });
            }
            (_, GetBody::Document(document)) => {
                return Err(Error::StoreConnection {
                    message: format!(
                        "OpenSearch read for {index}/{key} returned unexpected HTTP {} and found={} for document {}",
                        status.as_u16(),
                        document.found,
                        document.id
                    ),
                });
            }
        };

        let entry = Self::doc_to_entry(index, key, source)?;
        if !entry.is_expired() {
            return Ok(Some(entry));
        }

        self.bulk_delete(index, &[key.to_string()]).await?;
        Ok(None)
    }

    async fn read_entries(
        &self,
        index: &str,
        keys: &[String],
    ) -> Result<HashMap<String, Option<ManagedEntry>>> {
        if keys.is_empty() {
            return Ok(HashMap::new());
        }

        let response = self
            .os()
            .mget(MgetParts::Index(index))
            .body(MgetRequest {
                ids: keys.iter().map(String::as_str).collect(),
            })
            .send()
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to batch-read OpenSearch index {index}: {error}"),
            })?;
        let status = response.status_code();
        if !status.is_success() {
            let error = response
                .json::<OpenSearchErrorResponse>()
                .await
                .map_err(|decode_error| Error::Deserialization(format!(
                    "failed to decode OpenSearch batch-read error response for {index}: {decode_error}"
                )))?;
            if status.as_u16() == 404 && error.status == 404 && error.error.is_index_not_found() {
                return Ok(keys.iter().cloned().map(|key| (key, None)).collect());
            }
            return Err(Error::StoreConnection {
                message: format!(
                    "OpenSearch batch read for {index} failed with HTTP {} / body status {}: {}: {}",
                    status.as_u16(),
                    error.status,
                    error.error.kind,
                    error.error.reason
                ),
            });
        }

        let body = response.json::<MgetResponse>().await.map_err(|error| {
            Error::Deserialization(format!(
                "failed to decode OpenSearch batch-read response for {index}: {error}"
            ))
        })?;
        if body.docs.len() != keys.len() {
            return Err(Error::StoreConnection {
                message: format!(
                    "OpenSearch batch read for {index} returned {} documents for {} keys",
                    body.docs.len(),
                    keys.len()
                ),
            });
        }

        let mut entries = HashMap::with_capacity(keys.len());
        let mut expired = Vec::new();
        for (expected_key, document) in keys.iter().zip(body.docs) {
            if document.id != *expected_key {
                return Err(Error::StoreConnection {
                    message: format!(
                        "OpenSearch batch read for {index}/{expected_key} returned document id {}",
                        document.id
                    ),
                });
            }

            let entry = if let Some(error) = document.error {
                if document.found.is_some() || document.source.is_some() {
                    return Err(Error::Deserialization(format!(
                        "OpenSearch batch error for {index}/{expected_key} also contained document data"
                    )));
                }
                if error.is_index_not_found() {
                    None
                } else {
                    return Err(Error::StoreConnection {
                        message: format!(
                            "OpenSearch batch read failed for {index}/{expected_key}: {}: {}",
                            error.kind, error.reason
                        ),
                    });
                }
            } else {
                match (document.found, document.source) {
                    (Some(false), None) => None,
                    (Some(true), Some(source)) => {
                        Some(Self::doc_to_entry(index, expected_key, source)?)
                    }
                    (Some(false), Some(_)) => {
                        return Err(Error::Deserialization(format!(
                            "missing OpenSearch document {index}/{expected_key} unexpectedly contained _source"
                        )));
                    }
                    (Some(true), None) => {
                        return Err(Error::Deserialization(format!(
                            "OpenSearch document {index}/{expected_key} is missing _source"
                        )));
                    }
                    (None, _) => {
                        return Err(Error::Deserialization(format!(
                            "OpenSearch batch response for {index}/{expected_key} is missing found and error"
                        )));
                    }
                }
            };

            if entry.as_ref().is_some_and(ManagedEntry::is_expired) {
                expired.push(expected_key.clone());
                entries.insert(expected_key.clone(), None);
            } else {
                entries.insert(expected_key.clone(), entry);
            }
        }

        if !expired.is_empty() {
            self.bulk_delete(index, &expired).await?;
        }
        Ok(entries)
    }

    async fn bulk_index(&self, index: &str, documents: Vec<(String, OpenSearchDoc)>) -> Result<()> {
        if documents.is_empty() {
            return Ok(());
        }

        let expected_keys: Vec<_> = documents.iter().map(|(key, _)| key.clone()).collect();
        let operations: Vec<BulkOperation<OpenSearchDoc>> = documents
            .into_iter()
            .map(|(key, document)| BulkOperation::index(document).id(key).into())
            .collect();
        let response = self
            .os()
            .bulk(BulkParts::Index(index))
            .refresh(Refresh::WaitFor)
            .body(operations)
            .send()
            .await
            .map_err(|error| {
                if error.is_json() {
                    Error::Serialization(format!(
                        "failed to encode OpenSearch bulk index request for {index}: {error}"
                    ))
                } else {
                    Error::StoreConnection {
                        message: format!(
                            "failed to send OpenSearch bulk index request for {index}: {error}"
                        ),
                    }
                }
            })?;
        let status = response.status_code();
        if !status.is_success() {
            let error = response
                .json::<OpenSearchErrorResponse>()
                .await
                .map_err(|decode_error| Error::Deserialization(format!(
                    "failed to decode OpenSearch bulk index error response for {index}: {decode_error}"
                )))?;
            return Err(Error::StoreConnection {
                message: format!(
                    "OpenSearch bulk index for {index} failed with HTTP {} / body status {}: {}: {}",
                    status.as_u16(),
                    error.status,
                    error.error.kind,
                    error.error.reason
                ),
            });
        }

        let body = response.json::<BulkResponse>().await.map_err(|error| {
            Error::Deserialization(format!(
                "failed to decode OpenSearch bulk index response for {index}: {error}"
            ))
        })?;
        if body.items.len() != expected_keys.len() {
            return Err(Error::StoreConnection {
                message: format!(
                    "OpenSearch bulk index for {index} returned {} items for {} documents",
                    body.items.len(),
                    expected_keys.len()
                ),
            });
        }

        for (expected_key, item) in expected_keys.iter().zip(body.items) {
            let Some(result) = item.index else {
                return Err(Error::Deserialization(format!(
                    "OpenSearch bulk index for {index}/{expected_key} returned a non-index item"
                )));
            };
            if item.delete.is_some() {
                return Err(Error::Deserialization(format!(
                    "OpenSearch bulk index for {index}/{expected_key} returned multiple item actions"
                )));
            }
            if result.id != *expected_key {
                return Err(Error::StoreConnection {
                    message: format!(
                        "OpenSearch bulk index for {index}/{expected_key} returned document id {}",
                        result.id
                    ),
                });
            }
            if let Some(error) = result.error {
                return Err(Error::StoreConnection {
                    message: format!(
                        "OpenSearch bulk index failed for {index}/{expected_key} with status {}: {}: {}",
                        result.status, error.kind, error.reason
                    ),
                });
            }
            let valid_result = matches!(result.result.as_deref(), Some("created" | "updated"));
            if !matches!(result.status, 200 | 201) || !valid_result {
                return Err(Error::StoreConnection {
                    message: format!(
                        "OpenSearch bulk index for {index}/{expected_key} returned status {} and result {:?}",
                        result.status, result.result
                    ),
                });
            }
            let shards = result.shards.ok_or_else(|| Error::Deserialization(format!(
                "OpenSearch bulk index result for {index}/{expected_key} is missing shard metadata"
            )))?;
            if shards.failed != 0 || shards.successful == 0 || shards.successful > shards.total {
                return Err(Error::StoreConnection {
                    message: format!(
                        "OpenSearch bulk index for {index}/{expected_key} reported shards total={}, successful={}, failed={}",
                        shards.total, shards.successful, shards.failed
                    ),
                });
            }
        }
        if body.errors {
            return Err(Error::StoreConnection {
                message: format!(
                    "OpenSearch bulk index for {index} reported errors despite successful items"
                ),
            });
        }
        Ok(())
    }

    async fn bulk_delete(&self, index: &str, keys: &[String]) -> Result<usize> {
        if keys.is_empty() {
            return Ok(0);
        }

        let operations: Vec<BulkOperation<OpenSearchDoc>> = keys
            .iter()
            .map(|key| BulkOperation::delete(key.clone()).into())
            .collect();
        let response = self
            .os()
            .bulk(BulkParts::Index(index))
            .refresh(Refresh::WaitFor)
            .body(operations)
            .send()
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to send OpenSearch bulk delete for {index}: {error}"),
            })?;
        let status = response.status_code();
        if !status.is_success() {
            let error = response
                .json::<OpenSearchErrorResponse>()
                .await
                .map_err(|decode_error| Error::Deserialization(format!(
                    "failed to decode OpenSearch bulk delete error response for {index}: {decode_error}"
                )))?;
            if status.as_u16() == 404 && error.status == 404 && error.error.is_index_not_found() {
                return Ok(0);
            }
            return Err(Error::StoreConnection {
                message: format!(
                    "OpenSearch bulk delete for {index} failed with HTTP {} / body status {}: {}: {}",
                    status.as_u16(),
                    error.status,
                    error.error.kind,
                    error.error.reason
                ),
            });
        }

        let body = response.json::<BulkResponse>().await.map_err(|error| {
            Error::Deserialization(format!(
                "failed to decode OpenSearch bulk delete response for {index}: {error}"
            ))
        })?;
        if body.items.len() != keys.len() {
            return Err(Error::StoreConnection {
                message: format!(
                    "OpenSearch bulk delete for {index} returned {} items for {} documents",
                    body.items.len(),
                    keys.len()
                ),
            });
        }

        let mut deleted = 0;
        let mut accepted_absence = false;
        for (expected_key, item) in keys.iter().zip(body.items) {
            let Some(result) = item.delete else {
                return Err(Error::Deserialization(format!(
                    "OpenSearch bulk delete for {index}/{expected_key} returned a non-delete item"
                )));
            };
            if item.index.is_some() {
                return Err(Error::Deserialization(format!(
                    "OpenSearch bulk delete for {index}/{expected_key} returned multiple item actions"
                )));
            }
            if result.id != *expected_key {
                return Err(Error::StoreConnection {
                    message: format!(
                        "OpenSearch bulk delete for {index}/{expected_key} returned document id {}",
                        result.id
                    ),
                });
            }

            if let Some(error) = result.error {
                if result.status == 404 && error.is_index_not_found() {
                    accepted_absence = true;
                    continue;
                }
                return Err(Error::StoreConnection {
                    message: format!(
                        "OpenSearch bulk delete failed for {index}/{expected_key} with status {}: {}: {}",
                        result.status, error.kind, error.reason
                    ),
                });
            }

            match (result.status, result.result.as_deref()) {
                (200, Some("deleted")) => {
                    let shards = result.shards.ok_or_else(|| Error::Deserialization(format!(
                        "OpenSearch bulk delete result for {index}/{expected_key} is missing shard metadata"
                    )))?;
                    if shards.failed != 0
                        || shards.successful == 0
                        || shards.successful > shards.total
                    {
                        return Err(Error::StoreConnection {
                            message: format!(
                                "OpenSearch bulk delete for {index}/{expected_key} reported shards total={}, successful={}, failed={}",
                                shards.total, shards.successful, shards.failed
                            ),
                        });
                    }
                    deleted += 1;
                }
                (404, Some("not_found")) => {
                    if let Some(shards) = result.shards
                        && (shards.failed != 0
                            || shards.successful == 0
                            || shards.successful > shards.total)
                    {
                        return Err(Error::StoreConnection {
                            message: format!(
                                "OpenSearch missing-document delete for {index}/{expected_key} reported shards total={}, successful={}, failed={}",
                                shards.total, shards.successful, shards.failed
                            ),
                        });
                    }
                    accepted_absence = true;
                }
                _ => {
                    return Err(Error::StoreConnection {
                        message: format!(
                            "OpenSearch bulk delete for {index}/{expected_key} returned status {} and result {:?}",
                            result.status, result.result
                        ),
                    });
                }
            }
        }
        if body.errors && !accepted_absence {
            return Err(Error::StoreConnection {
                message: format!(
                    "OpenSearch bulk delete for {index} reported errors despite successful items"
                ),
            });
        }
        Ok(deleted)
    }
}

#[async_trait]
impl AsyncKeyValue for OpenSearchStore {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        let index = self.index_name(self.collection_name(collection))?;
        let document_id = encode_document_id(key)?;
        Ok(self
            .read_entry(&index, &document_id)
            .await?
            .map(|entry| entry.value))
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(Value, Option<f64>)>> {
        let index = self.index_name(self.collection_name(collection))?;
        let document_id = encode_document_id(key)?;
        Ok(self.read_entry(&index, &document_id).await?.map(|entry| {
            let ttl = entry.ttl();
            (entry.value, ttl)
        }))
    }

    async fn put(
        &self,
        key: &str,
        value: Value,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        let index = self.index_name(self.collection_name(collection))?;
        let document_id = encode_document_id(key)?;
        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds)?,
            None => ManagedEntry::new(value),
        };
        self.bulk_index(&index, vec![(document_id, Self::entry_to_doc(&entry))])
            .await
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let index = self.index_name(self.collection_name(collection))?;
        let document_id = encode_document_id(key)?;
        Ok(self.bulk_delete(&index, &[document_id]).await? == 1)
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        let index = self.index_name(self.collection_name(collection))?;
        let document_ids = keys
            .iter()
            .map(|key| encode_document_id(key))
            .collect::<Result<Vec<_>>>()?;
        if document_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut seen = HashSet::with_capacity(document_ids.len());
        let mut unique_ids = Vec::with_capacity(document_ids.len());
        for document_id in &document_ids {
            if seen.insert(document_id.as_str()) {
                unique_ids.push(document_id.clone());
            }
        }

        let entries = self.read_entries(&index, &unique_ids).await?;
        Ok(document_ids
            .iter()
            .map(|document_id| {
                entries
                    .get(document_id)
                    .and_then(|entry| entry.as_ref().map(|entry| entry.value.clone()))
            })
            .collect())
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, Option<f64>)>>> {
        let index = self.index_name(self.collection_name(collection))?;
        let document_ids = keys
            .iter()
            .map(|key| encode_document_id(key))
            .collect::<Result<Vec<_>>>()?;
        if document_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut seen = HashSet::with_capacity(document_ids.len());
        let mut unique_ids = Vec::with_capacity(document_ids.len());
        for document_id in &document_ids {
            if seen.insert(document_id.as_str()) {
                unique_ids.push(document_id.clone());
            }
        }

        let entries = self.read_entries(&index, &unique_ids).await?;
        Ok(document_ids
            .iter()
            .map(|document_id| {
                entries.get(document_id).and_then(|entry| {
                    entry
                        .as_ref()
                        .map(|entry| (entry.value.clone(), entry.ttl()))
                })
            })
            .collect())
    }

    async fn put_many(
        &self,
        keys: &[String],
        values: &[Value],
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        if keys.len() != values.len() {
            return Err(Error::BatchSizeMismatch {
                keys: keys.len(),
                values: values.len(),
            });
        }

        let index = self.index_name(self.collection_name(collection))?;
        let document_ids = keys
            .iter()
            .map(|key| encode_document_id(key))
            .collect::<Result<Vec<_>>>()?;
        if let Some(seconds) = ttl {
            ManagedEntry::validate_ttl(seconds)?;
        }

        let entries = values
            .iter()
            .cloned()
            .map(|value| match ttl {
                Some(seconds) => ManagedEntry::with_ttl(value, seconds),
                None => Ok(ManagedEntry::new(value)),
            })
            .collect::<Result<Vec<_>>>()?;
        if document_ids.is_empty() {
            return Ok(());
        }

        let mut positions = HashMap::with_capacity(document_ids.len());
        let mut documents = Vec::with_capacity(document_ids.len());
        for (document_id, entry) in document_ids.into_iter().zip(entries) {
            let document = Self::entry_to_doc(&entry);
            if let Some(position) = positions.get(&document_id).copied() {
                documents[position] = (document_id, document);
            } else {
                positions.insert(document_id.clone(), documents.len());
                documents.push((document_id, document));
            }
        }

        self.bulk_index(&index, documents).await
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        let index = self.index_name(self.collection_name(collection))?;
        let document_ids = keys
            .iter()
            .map(|key| encode_document_id(key))
            .collect::<Result<Vec<_>>>()?;
        if document_ids.is_empty() {
            return Ok(0);
        }

        let mut seen = HashSet::with_capacity(document_ids.len());
        let mut unique_ids = Vec::with_capacity(document_ids.len());
        for document_id in document_ids {
            if seen.insert(document_id.clone()) {
                unique_ids.push(document_id);
            }
        }

        self.bulk_delete(&index, &unique_ids).await
    }
}

#[async_trait]
impl AsyncCull for OpenSearchStore {
    async fn cull(&self) -> Result<()> {
        let indices = self.owned_indices().await?;
        for (index, _) in &indices {
            self.validate_document_ids(index).await?;
        }
        if indices.is_empty() {
            return Ok(());
        }

        let index_names: Vec<_> = indices.iter().map(|(index, _)| index.as_str()).collect();
        let now = chrono::Utc::now().timestamp_millis();
        let response = self
            .os()
            .delete_by_query(DeleteByQueryParts::Index(&index_names))
            .allow_no_indices(true)
            .ignore_unavailable(true)
            .conflicts(Conflicts::Abort)
            .refresh(true)
            .body(serde_json::json!({
                "query": {
                    "range": {
                        "expires_at": { "lte": now }
                    }
                }
            }))
            .send()
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!(
                    "failed to cull expired OpenSearch documents from exact indices {index_names:?}: {error}"
                ),
            })?;
        let status = response.status_code();
        if !status.is_success() {
            let error = response
                .json::<OpenSearchErrorResponse>()
                .await
                .map_err(|decode_error| Error::Deserialization(format!(
                    "failed to decode OpenSearch cull error response for exact indices {index_names:?}: {decode_error}"
                )))?;
            if status.as_u16() == 404 && error.status == 404 && error.error.is_index_not_found() {
                return Ok(());
            }
            return Err(Error::StoreConnection {
                message: format!(
                    "OpenSearch cull for exact indices {index_names:?} failed with HTTP {} / body status {}: {}: {}",
                    status.as_u16(),
                    error.status,
                    error.error.kind,
                    error.error.reason
                ),
            });
        }

        let body = response
            .json::<DeleteByQueryResponse>()
            .await
            .map_err(|error| {
                Error::Deserialization(format!(
                    "failed to decode OpenSearch cull response for exact indices {index_names:?}: {error}"
                ))
            })?;
        if body.timed_out
            || body.version_conflicts != 0
            || body.noops != 0
            || !body.failures.is_empty()
            || body.deleted != body.total
        {
            return Err(Error::StoreConnection {
                message: format!(
                    "OpenSearch cull for exact indices {index_names:?} was incomplete: timed_out={}, total={}, deleted={}, version_conflicts={}, noops={}, failures={}",
                    body.timed_out,
                    body.total,
                    body.deleted,
                    body.version_conflicts,
                    body.noops,
                    body.failures.len()
                ),
            });
        }
        Ok(())
    }
}

#[async_trait]
impl AsyncEnumerateKeys for OpenSearchStore {
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        let index = self.index_name(self.collection_name(collection))?;
        if !self.validate_document_ids(&index).await? {
            return Ok(Vec::new());
        }

        let limit = limit.unwrap_or(DEFAULT_PAGE_SIZE).min(PAGE_LIMIT);
        if limit == 0 {
            return Ok(Vec::new());
        }

        let now = chrono::Utc::now().timestamp_millis();
        let response = self
            .os()
            .search(SearchParts::Index(&[&index]))
            .body(serde_json::json!({
                "query": {
                    "bool": {
                        "should": [
                            {
                                "bool": {
                                    "must_not": {
                                        "exists": { "field": "expires_at" }
                                    }
                                }
                            },
                            {
                                "range": {
                                    "expires_at": { "gt": now }
                                }
                            }
                        ],
                        "minimum_should_match": 1
                    }
                },
                "size": limit,
                "_source": false
            }))
            .send()
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to enumerate OpenSearch keys in {index}: {error}"),
            })?;
        let status = response.status_code();
        if !status.is_success() {
            let error = response
                .json::<OpenSearchErrorResponse>()
                .await
                .map_err(|decode_error| Error::Deserialization(format!(
                    "failed to decode OpenSearch key-enumeration error response for {index}: {decode_error}"
                )))?;
            if status.as_u16() == 404 && error.status == 404 && error.error.is_index_not_found() {
                return Ok(Vec::new());
            }
            return Err(Error::StoreConnection {
                message: format!(
                    "OpenSearch key enumeration for {index} failed with HTTP {} / body status {}: {}: {}",
                    status.as_u16(),
                    error.status,
                    error.error.kind,
                    error.error.reason
                ),
            });
        }

        let body = response.json::<SearchResponse>().await.map_err(|error| {
            Error::Deserialization(format!(
                "failed to decode OpenSearch key-enumeration response for {index}: {error}"
            ))
        })?;
        if body.timed_out
            || body.shards.failed != 0
            || body.shards.successful + body.shards.skipped > body.shards.total
        {
            return Err(Error::StoreConnection {
                message: format!(
                    "OpenSearch key enumeration for {index} was incomplete: timed_out={}, shards total={}, successful={}, skipped={}, failed={}",
                    body.timed_out,
                    body.shards.total,
                    body.shards.successful,
                    body.shards.skipped,
                    body.shards.failed
                ),
            });
        }
        body.hits
            .hits
            .into_iter()
            .map(|hit| decode_document_id(&hit.id))
            .collect()
    }
}

#[async_trait]
impl AsyncEnumerateCollections for OpenSearchStore {
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        let indices = self.owned_indices().await?;
        let limit = limit.unwrap_or(DEFAULT_PAGE_SIZE).min(PAGE_LIMIT);
        Ok(indices
            .into_iter()
            .take(limit)
            .map(|(_, collection)| collection)
            .collect())
    }
}

#[async_trait]
impl AsyncDestroyCollection for OpenSearchStore {
    async fn destroy_collection(&self, collection: &str) -> Result<bool> {
        let index = self.index_name(collection)?;
        if !self.validate_document_ids(&index).await? {
            return Ok(false);
        }

        let response = self
            .os()
            .indices()
            .delete(IndicesDeleteParts::Index(&[&index]))
            .send()
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to destroy OpenSearch collection {index}: {error}"),
            })?;
        let status = response.status_code();
        if status.is_success() {
            let body = response
                .json::<AcknowledgedResponse>()
                .await
                .map_err(|error| Error::Deserialization(format!(
                    "failed to decode OpenSearch collection-destroy response for {index}: {error}"
                )))?;
            if !body.acknowledged {
                return Err(Error::StoreConnection {
                    message: format!(
                        "OpenSearch did not acknowledge collection destruction for {index}"
                    ),
                });
            }
            return Ok(true);
        }

        let error = response
            .json::<OpenSearchErrorResponse>()
            .await
            .map_err(|decode_error| Error::Deserialization(format!(
                "failed to decode OpenSearch collection-destroy error response for {index}: {decode_error}"
            )))?;
        if status.as_u16() == 404 && error.status == 404 && error.error.is_index_not_found() {
            return Ok(false);
        }
        Err(Error::StoreConnection {
            message: format!(
                "OpenSearch collection destruction for {index} failed with HTTP {} / body status {}: {}: {}",
                status.as_u16(),
                error.status,
                error.error.kind,
                error.error.reason
            ),
        })
    }
}

#[async_trait]
impl AsyncDestroyStore for OpenSearchStore {
    async fn destroy(&self) -> Result<bool> {
        let indices = self.owned_indices().await?;
        for (index, _) in &indices {
            self.validate_document_ids(index).await?;
        }
        if indices.is_empty() {
            return Ok(true);
        }

        let index_names: Vec<_> = indices.iter().map(|(index, _)| index.as_str()).collect();
        let response = self
            .os()
            .indices()
            .delete(IndicesDeleteParts::Index(&index_names))
            .allow_no_indices(true)
            .ignore_unavailable(true)
            .send()
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!(
                    "failed to destroy exact OpenSearch store indices {index_names:?}: {error}"
                ),
            })?;
        let status = response.status_code();
        if !status.is_success() {
            let error = response
                .json::<OpenSearchErrorResponse>()
                .await
                .map_err(|decode_error| Error::Deserialization(format!(
                    "failed to decode OpenSearch store-destroy error response for exact indices {index_names:?}: {decode_error}"
                )))?;
            if status.as_u16() == 404 && error.status == 404 && error.error.is_index_not_found() {
                return Ok(true);
            }
            return Err(Error::StoreConnection {
                message: format!(
                    "OpenSearch store destruction for exact indices {index_names:?} failed with HTTP {} / body status {}: {}: {}",
                    status.as_u16(),
                    error.status,
                    error.error.kind,
                    error.error.reason
                ),
            });
        }

        let body = response
            .json::<AcknowledgedResponse>()
            .await
            .map_err(|error| {
                Error::Deserialization(format!(
                    "failed to decode OpenSearch store-destroy response for exact indices {index_names:?}: {error}"
                ))
            })?;
        if !body.acknowledged {
            return Err(Error::StoreConnection {
                message: format!(
                    "OpenSearch did not acknowledge destruction for exact indices {index_names:?}"
                ),
            });
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use opensearch::IndexParts;
    use opensearch::indices::IndicesCreateParts;

    #[derive(Deserialize)]
    struct RawGetResponse {
        #[serde(rename = "_source")]
        source: serde_json::Value,
    }

    #[test]
    fn opensearch_identity_transport_is_reversible_and_bounded() {
        let collections = ["", "Users", "users", "é", "e\u{301}", "a/b:*?[x]\\\0值"];
        for collection in collections {
            let index = encode_index_name("p", collection).unwrap();
            assert_eq!(decode_index_name("p", &index).unwrap(), collection);
            assert!(
                index
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            );
        }
        assert_ne!(
            encode_index_name("p", "Users").unwrap(),
            encode_index_name("p", "users").unwrap()
        );
        assert_ne!(
            encode_index_name("p", "é").unwrap(),
            encode_index_name("p", "e\u{301}").unwrap()
        );

        let max_collection = "a".repeat(124);
        let max_index = encode_index_name("p", &max_collection).unwrap();
        assert_eq!(max_index.len(), 255);
        assert_eq!(decode_index_name("p", &max_index).unwrap(), max_collection);
        assert!(matches!(
            encode_index_name("p", &"a".repeat(125)),
            Err(Error::InvalidKey(_))
        ));

        let keys = ["", "Key", "key", "é", "e\u{301}", "a/b:*?[x]\\\0值"];
        for key in keys {
            let document_id = encode_document_id(key).unwrap();
            assert_eq!(decode_document_id(&document_id).unwrap(), key);
            assert!(
                document_id
                    .bytes()
                    .all(|byte| { byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_' })
            );
        }
        assert_ne!(
            encode_document_id("Key").unwrap(),
            encode_document_id("key").unwrap()
        );
        assert_ne!(
            encode_document_id("é").unwrap(),
            encode_document_id("e\u{301}").unwrap()
        );

        let max_key = "a".repeat(380);
        let max_document_id = encode_document_id(&max_key).unwrap();
        assert_eq!(max_document_id.len(), 512);
        assert_eq!(decode_document_id(&max_document_id).unwrap(), max_key);
        assert!(matches!(
            encode_document_id(&"a".repeat(381)),
            Err(Error::InvalidKey(_))
        ));
    }

    #[test]
    fn opensearch_identity_transport_rejects_noncanonical_names() {
        for prefix in [
            "", "Upper", "-prefix", "_prefix", "+prefix", "prefix*", "prefix?", "prefix ",
            "prefix:", "前缀",
        ] {
            assert!(
                matches!(
                    encode_index_name(prefix, "collection"),
                    Err(Error::InvalidKey(_))
                ),
                "accepted invalid prefix {prefix:?}"
            );
        }
        assert!(matches!(
            encode_index_name(&"a".repeat(250), ""),
            Err(Error::InvalidKey(_))
        ));

        for index in [
            "p-users",
            "p-okv2-",
            "p-okv1-0",
            "p-okv1-4A",
            "p-okv1-gg",
            "p-okv1-ff",
            "q-okv1-61",
        ] {
            assert!(
                matches!(decode_index_name("p", index), Err(Error::InvalidKey(_))),
                "accepted invalid index {index:?}"
            );
        }
        let oversized_index = format!("p-okv1-{}", "61".repeat(125));
        assert!(matches!(
            decode_index_name("p", &oversized_index),
            Err(Error::InvalidKey(_))
        ));

        for document_id in [
            "raw-key",
            "okv2-",
            "okv1-YQ==",
            "okv1-*",
            "okv1-YR",
            "okv1-_w",
        ] {
            assert!(
                matches!(decode_document_id(document_id), Err(Error::InvalidKey(_))),
                "accepted invalid document id {document_id:?}"
            );
        }
        let oversized_document_id = format!("okv1-{}", "a".repeat(508));
        assert_eq!(oversized_document_id.len(), 513);
        assert!(matches!(
            decode_document_id(&oversized_document_id),
            Err(Error::InvalidKey(_))
        ));
    }

    #[tokio::test]
    #[ignore = "requires OPENKEYV_OPENSEARCH_URL"]
    async fn opensearch_stores_exact_canonical_okve1_documents() {
        let url = std::env::var("OPENKEYV_OPENSEARCH_URL")
            .expect("OPENKEYV_OPENSEARCH_URL must point to OpenSearch");
        let prefix = format!(
            "openkeyv-native-{}-{}",
            std::process::id(),
            Utc::now().timestamp_millis()
        );
        let store = OpenSearchStore::from_url(url, prefix).await.unwrap();

        store
            .put("plain", Value::utf8("plain"), None, None)
            .await
            .unwrap();
        store
            .put("ttl", Value::utf8("ttl"), None, Some(60.0))
            .await
            .unwrap();

        for (key, expected_fields) in [("plain", 1), ("ttl", 2)] {
            let index = store.index_name(store.collection_name(None)).unwrap();
            let document_id = encode_document_id(key).unwrap();
            let response = store
                .os()
                .get(GetParts::IndexId(&index, &document_id))
                .send()
                .await
                .unwrap();
            assert!(response.status_code().is_success());
            let raw = response.json::<RawGetResponse>().await.unwrap();
            let source = raw.source.as_object().unwrap();
            assert_eq!(source.len(), expected_fields);
            let encoded = source.get("entry").unwrap().as_str().unwrap();
            let bytes = STANDARD.decode(encoded).unwrap();
            assert_eq!(STANDARD.encode(&bytes), encoded);
            assert!(bytes.starts_with(b"OKVE1"));
            let decoded = ManagedEntry::decode(Bytes::from(bytes)).unwrap();
            assert_eq!(
                decoded
                    .expires_at
                    .map(|expires_at| expires_at.timestamp_millis()),
                source
                    .get("expires_at")
                    .map(|value| value.as_i64().unwrap())
            );
        }

        assert_eq!(
            store.get("plain", None).await.unwrap(),
            Some(Value::utf8("plain"))
        );
        let ttl = store.ttl("ttl", None).await.unwrap().unwrap();
        assert_eq!(ttl.0, Value::utf8("ttl"));
        let ttl = ttl.1.unwrap();
        assert!(ttl > 0.0 && ttl <= 60.0);
        assert!(store.destroy().await.unwrap());
    }

    #[tokio::test]
    #[ignore = "requires OPENKEYV_OPENSEARCH_URL"]
    async fn opensearch_batches_preserve_duplicates_ttl_and_delete_counts() {
        let url = std::env::var("OPENKEYV_OPENSEARCH_URL")
            .expect("OPENKEYV_OPENSEARCH_URL must point to OpenSearch");
        let prefix = format!(
            "openkeyv-batch-{}-{}",
            std::process::id(),
            Utc::now().timestamp_millis()
        );
        let store = OpenSearchStore::from_url(url, prefix).await.unwrap();
        let keys = vec!["a".to_string(), "b".to_string(), "a".to_string()];
        let values = vec![
            Value::utf8("first-a"),
            Value::utf8("b"),
            Value::utf8("last-a"),
        ];
        store
            .put_many(&keys, &values, None, Some(60.0))
            .await
            .unwrap();

        let read_keys = vec![
            "a".to_string(),
            "missing".to_string(),
            "b".to_string(),
            "a".to_string(),
        ];
        assert_eq!(
            store.get_many(&read_keys, None).await.unwrap(),
            vec![
                Some(Value::utf8("last-a")),
                None,
                Some(Value::utf8("b")),
                Some(Value::utf8("last-a")),
            ]
        );
        let ttl_values = store.ttl_many(&read_keys, None).await.unwrap();
        assert_eq!(ttl_values.len(), read_keys.len());
        assert_eq!(
            ttl_values[0].as_ref().map(|(value, _)| value),
            Some(&Value::utf8("last-a"))
        );
        assert!(ttl_values[0].as_ref().unwrap().1.unwrap() > 0.0);
        assert!(ttl_values[1].is_none());
        assert_eq!(
            ttl_values[2].as_ref().map(|(value, _)| value),
            Some(&Value::utf8("b"))
        );
        assert_eq!(
            ttl_values[3].as_ref().map(|(value, _)| value),
            Some(&Value::utf8("last-a"))
        );

        let mut expired = ManagedEntry::new(Value::utf8("expired"));
        expired.expires_at = Some(Utc::now() - chrono::TimeDelta::seconds(1));
        let index = store.index_name(store.collection_name(None)).unwrap();
        store
            .bulk_index(
                &index,
                vec![(
                    encode_document_id("expired-read").unwrap(),
                    OpenSearchStore::entry_to_doc(&expired),
                )],
            )
            .await
            .unwrap();
        assert!(
            !store
                .keys(None, None)
                .await
                .unwrap()
                .contains(&"expired-read".to_string())
        );
        assert_eq!(store.get("expired-read", None).await.unwrap(), None);

        store
            .bulk_index(
                &index,
                vec![(
                    encode_document_id("expired-cull").unwrap(),
                    OpenSearchStore::entry_to_doc(&expired),
                )],
            )
            .await
            .unwrap();
        store.cull().await.unwrap();
        assert_eq!(store.get("expired-cull", None).await.unwrap(), None);

        let delete_keys = vec![
            "a".to_string(),
            "a".to_string(),
            "missing".to_string(),
            "b".to_string(),
        ];
        assert_eq!(store.delete_many(&delete_keys, None).await.unwrap(), 2);
        assert_eq!(store.delete_many(&delete_keys, None).await.unwrap(), 0);
        assert!(store.destroy().await.unwrap());
    }

    #[tokio::test]
    #[ignore = "requires OPENKEYV_OPENSEARCH_URL"]
    async fn opensearch_collections_include_empty_indices_and_destroy_strictly() {
        let url = std::env::var("OPENKEYV_OPENSEARCH_URL")
            .expect("OPENKEYV_OPENSEARCH_URL must point to OpenSearch");
        let prefix = format!(
            "openkeyv-collections-{}-{}",
            std::process::id(),
            Utc::now().timestamp_millis()
        );
        let store = OpenSearchStore::from_url(url, prefix).await.unwrap();
        let empty_index = store.index_name("empty").unwrap();
        let response = store
            .os()
            .indices()
            .create(IndicesCreateParts::Index(&empty_index))
            .send()
            .await
            .unwrap();
        assert!(response.status_code().is_success());
        store
            .put("key", Value::utf8("value"), Some("full"), None)
            .await
            .unwrap();

        assert_eq!(
            store.collections(None).await.unwrap(),
            vec!["empty".to_string(), "full".to_string()]
        );
        assert!(store.keys(Some("missing"), None).await.unwrap().is_empty());
        assert_eq!(store.get("missing", Some("missing")).await.unwrap(), None);
        assert!(!store.delete("missing", Some("missing")).await.unwrap());
        assert!(store.destroy_collection("empty").await.unwrap());
        assert!(!store.destroy_collection("empty").await.unwrap());
        assert!(store.destroy().await.unwrap());
        assert!(store.destroy().await.unwrap());
        assert!(store.collections(None).await.unwrap().is_empty());
    }

    #[tokio::test]
    #[ignore = "requires OPENKEYV_OPENSEARCH_URL"]
    async fn opensearch_roundtrips_logical_identities_and_prevalidates_batches() {
        let url = std::env::var("OPENKEYV_OPENSEARCH_URL")
            .expect("OPENKEYV_OPENSEARCH_URL must point to OpenSearch");
        let prefix = format!(
            "openkeyv-identity-{}-{}",
            std::process::id(),
            Utc::now().timestamp_millis()
        );
        let store = OpenSearchStore::from_url(url, prefix).await.unwrap();
        let identities = [
            ("", ""),
            ("Users", "Key"),
            ("users", "key"),
            ("é", "é"),
            ("e\u{301}", "e\u{301}"),
            ("a/b:*?[x]\\\0值", "a/b:*?[x]\\\0值"),
        ];

        for (position, (collection, key)) in identities.iter().enumerate() {
            store
                .put(
                    key,
                    Value::unsigned_integer(position as u64),
                    Some(collection),
                    None,
                )
                .await
                .unwrap();
            assert_eq!(
                store.get(key, Some(collection)).await.unwrap(),
                Some(Value::unsigned_integer(position as u64))
            );
            assert_eq!(
                store.keys(Some(collection), None).await.unwrap(),
                vec![(*key).to_string()]
            );
        }

        let collections: HashSet<_> = store.collections(None).await.unwrap().into_iter().collect();
        assert_eq!(
            collections,
            identities
                .iter()
                .map(|(collection, _)| (*collection).to_string())
                .collect()
        );

        let max_key = "k".repeat(380);
        store
            .put(&max_key, Value::utf8("max-key"), Some("boundary"), None)
            .await
            .unwrap();
        assert_eq!(
            store.get(&max_key, Some("boundary")).await.unwrap(),
            Some(Value::utf8("max-key"))
        );

        let first = "first".to_string();
        let too_long = "k".repeat(381);
        let error = store
            .put_many(
                &[first.clone(), too_long],
                &[Value::utf8("first"), Value::utf8("too-long")],
                Some("batch"),
                None,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, Error::InvalidKey(_)));
        assert_eq!(store.get(&first, Some("batch")).await.unwrap(), None);

        assert!(store.destroy().await.unwrap());
    }

    #[tokio::test]
    #[ignore = "requires OPENKEYV_OPENSEARCH_URL"]
    async fn opensearch_enumeration_rejects_all_malformed_physical_identities() {
        let url = std::env::var("OPENKEYV_OPENSEARCH_URL")
            .expect("OPENKEYV_OPENSEARCH_URL must point to OpenSearch");
        let prefix = format!(
            "openkeyv-enumeration-{}-{}",
            std::process::id(),
            Utc::now().timestamp_millis()
        );
        let store = OpenSearchStore::from_url(url, prefix.clone())
            .await
            .unwrap();
        store
            .put("valid", Value::utf8("value"), Some("valid"), None)
            .await
            .unwrap();

        let malformed_index = format!("{prefix}-zz-invalid");
        let response = store
            .os()
            .indices()
            .create(IndicesCreateParts::Index(&malformed_index))
            .send()
            .await
            .unwrap();
        assert!(response.status_code().is_success());
        assert!(matches!(
            store.collections(Some(1)).await,
            Err(Error::InvalidKey(_))
        ));
        assert!(matches!(
            store.collections(Some(0)).await,
            Err(Error::InvalidKey(_))
        ));

        let response = store
            .os()
            .indices()
            .delete(IndicesDeleteParts::Index(&[&malformed_index]))
            .send()
            .await
            .unwrap();
        assert!(response.status_code().is_success());

        let index = store.index_name("valid").unwrap();
        let response = store
            .os()
            .index(IndexParts::IndexId(&index, "raw-key"))
            .refresh(Refresh::WaitFor)
            .body(OpenSearchStore::entry_to_doc(&ManagedEntry::new(
                Value::utf8("malformed-id"),
            )))
            .send()
            .await
            .unwrap();
        assert!(response.status_code().is_success());
        assert!(matches!(
            store.keys(Some("valid"), Some(1)).await,
            Err(Error::InvalidKey(_))
        ));
        assert!(matches!(
            store.keys(Some("valid"), Some(0)).await,
            Err(Error::InvalidKey(_))
        ));

        let response = store
            .os()
            .indices()
            .delete(IndicesDeleteParts::Index(&[&index]))
            .send()
            .await
            .unwrap();
        assert!(response.status_code().is_success());
        assert!(store.destroy().await.unwrap());
    }

    #[tokio::test]
    #[ignore = "requires OPENKEYV_OPENSEARCH_URL"]
    async fn opensearch_mutations_reject_malformed_identities_without_partial_changes() {
        let url = std::env::var("OPENKEYV_OPENSEARCH_URL")
            .expect("OPENKEYV_OPENSEARCH_URL must point to OpenSearch");
        let prefix = format!(
            "openkeyv-mutation-{}-{}",
            std::process::id(),
            Utc::now().timestamp_millis()
        );
        let store = OpenSearchStore::from_url(url, prefix.clone())
            .await
            .unwrap();

        let cull_index = store.index_name("cull").unwrap();
        let expired_id = encode_document_id("expired-valid").unwrap();
        let mut expired = ManagedEntry::new(Value::utf8("expired"));
        expired.expires_at = Some(Utc::now() - chrono::TimeDelta::seconds(1));
        store
            .bulk_index(
                &cull_index,
                vec![(expired_id.clone(), OpenSearchStore::entry_to_doc(&expired))],
            )
            .await
            .unwrap();
        let response = store
            .os()
            .index(IndexParts::IndexId(&cull_index, "raw-key"))
            .refresh(Refresh::WaitFor)
            .body(OpenSearchStore::entry_to_doc(&ManagedEntry::new(
                Value::utf8("malformed-id"),
            )))
            .send()
            .await
            .unwrap();
        assert!(response.status_code().is_success());
        assert!(matches!(store.cull().await, Err(Error::InvalidKey(_))));
        let response = store
            .os()
            .get(GetParts::IndexId(&cull_index, &expired_id))
            .send()
            .await
            .unwrap();
        assert!(response.status_code().is_success());
        let response = store
            .os()
            .indices()
            .delete(IndicesDeleteParts::Index(&[&cull_index]))
            .send()
            .await
            .unwrap();
        assert!(response.status_code().is_success());

        store
            .put(
                "valid",
                Value::utf8("value"),
                Some("destroy-collection"),
                None,
            )
            .await
            .unwrap();
        let collection_index = store.index_name("destroy-collection").unwrap();
        let valid_id = encode_document_id("valid").unwrap();
        let response = store
            .os()
            .index(IndexParts::IndexId(&collection_index, "raw-key"))
            .refresh(Refresh::WaitFor)
            .body(OpenSearchStore::entry_to_doc(&ManagedEntry::new(
                Value::utf8("malformed-id"),
            )))
            .send()
            .await
            .unwrap();
        assert!(response.status_code().is_success());
        assert!(matches!(
            store.destroy_collection("destroy-collection").await,
            Err(Error::InvalidKey(_))
        ));
        let response = store
            .os()
            .get(GetParts::IndexId(&collection_index, &valid_id))
            .send()
            .await
            .unwrap();
        assert!(response.status_code().is_success());
        let response = store
            .os()
            .indices()
            .delete(IndicesDeleteParts::Index(&[&collection_index]))
            .send()
            .await
            .unwrap();
        assert!(response.status_code().is_success());

        store
            .put("valid", Value::utf8("keep"), Some("keep-index"), None)
            .await
            .unwrap();
        let keep_index = store.index_name("keep-index").unwrap();
        let keep_id = encode_document_id("valid").unwrap();
        let malformed_index = format!("{prefix}-zz-invalid");
        let response = store
            .os()
            .indices()
            .create(IndicesCreateParts::Index(&malformed_index))
            .send()
            .await
            .unwrap();
        assert!(response.status_code().is_success());
        assert!(matches!(store.destroy().await, Err(Error::InvalidKey(_))));
        let response = store
            .os()
            .get(GetParts::IndexId(&keep_index, &keep_id))
            .send()
            .await
            .unwrap();
        assert!(response.status_code().is_success());
        let response = store
            .os()
            .indices()
            .delete(IndicesDeleteParts::Index(&[&malformed_index]))
            .send()
            .await
            .unwrap();
        assert!(response.status_code().is_success());
        assert!(store.destroy().await.unwrap());

        store
            .put("valid", Value::utf8("bad"), Some("bad-document"), None)
            .await
            .unwrap();
        store
            .put("valid", Value::utf8("keep"), Some("keep-document"), None)
            .await
            .unwrap();
        let bad_index = store.index_name("bad-document").unwrap();
        let keep_index = store.index_name("keep-document").unwrap();
        let keep_id = encode_document_id("valid").unwrap();
        let response = store
            .os()
            .index(IndexParts::IndexId(&bad_index, "raw-key"))
            .refresh(Refresh::WaitFor)
            .body(OpenSearchStore::entry_to_doc(&ManagedEntry::new(
                Value::utf8("malformed-id"),
            )))
            .send()
            .await
            .unwrap();
        assert!(response.status_code().is_success());
        assert!(matches!(store.destroy().await, Err(Error::InvalidKey(_))));
        let response = store
            .os()
            .get(GetParts::IndexId(&keep_index, &keep_id))
            .send()
            .await
            .unwrap();
        assert!(response.status_code().is_success());
        let response = store
            .os()
            .indices()
            .delete(IndicesDeleteParts::Index(&[&bad_index]))
            .send()
            .await
            .unwrap();
        assert!(response.status_code().is_success());
        assert!(store.destroy().await.unwrap());
    }

    #[tokio::test]
    #[ignore = "requires OPENKEYV_OPENSEARCH_URL"]
    async fn opensearch_rejects_noncanonical_and_corrupt_documents() {
        let url = std::env::var("OPENKEYV_OPENSEARCH_URL")
            .expect("OPENKEYV_OPENSEARCH_URL must point to OpenSearch");
        let prefix = format!(
            "openkeyv-invalid-{}-{}",
            std::process::id(),
            Utc::now().timestamp_millis()
        );
        let store = OpenSearchStore::from_url(url, prefix).await.unwrap();
        let index = store.index_name(store.collection_name(None)).unwrap();
        let valid_entry = ManagedEntry::with_ttl(Value::utf8("value"), 60.0).unwrap();
        let valid_encoded = STANDARD.encode(valid_entry.encode());
        let unpadded = valid_encoded.trim_end_matches('=').to_string();
        let expires_at = valid_entry.expires_at.unwrap().timestamp_millis();

        let invalid_documents = [
            (
                "old-json",
                serde_json::json!({ "value": "{\"value\":null}" }),
            ),
            (
                "unknown-field",
                serde_json::json!({ "entry": valid_encoded, "extra": true }),
            ),
            (
                "invalid-base64",
                serde_json::json!({ "entry": "not base64" }),
            ),
            ("unpadded-base64", serde_json::json!({ "entry": unpadded })),
            (
                "invalid-entry",
                serde_json::json!({ "entry": STANDARD.encode(br#"{"value":null}"#) }),
            ),
            (
                "mismatched-expiration",
                serde_json::json!({
                    "entry": STANDARD.encode(valid_entry.encode()),
                    "expires_at": expires_at + 1
                }),
            ),
            (
                "null-expiration",
                serde_json::json!({
                    "entry": STANDARD.encode(ManagedEntry::new(Value::utf8("value")).encode()),
                    "expires_at": null
                }),
            ),
        ];

        for (key, document) in invalid_documents {
            let response = store
                .os()
                .index(IndexParts::IndexId(
                    &index,
                    &encode_document_id(key).unwrap(),
                ))
                .refresh(Refresh::WaitFor)
                .body(document)
                .send()
                .await
                .unwrap();
            assert!(response.status_code().is_success());
            let error = store.get(key, None).await.unwrap_err();
            assert!(matches!(error, Error::Deserialization(_)), "{key}: {error}");
        }

        assert!(store.destroy().await.unwrap());
    }
}
