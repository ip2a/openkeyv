use super::client::OpenSearchClient;
use super::config::{DEFAULT_PAGE_SIZE, OpenSearchConfig, PAGE_LIMIT};
use super::error::{Error, Result, map_os_err};
use crate::entry::ManagedEntry;
use crate::protocol::{
    AsyncCull, AsyncDestroyCollection, AsyncDestroyStore, AsyncEnumerateCollections,
    AsyncEnumerateKeys, AsyncKeyValue,
};
use crate::value::Value;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct OpenSearchDoc {
    value: String,
    created_at: Option<String>,
    expires_at: Option<String>,
    collection: String,
}

/// OpenSearch-backed key-value store.
///
/// Each collection maps to an index named `{index_prefix}-{collection}`.
/// Each key maps to a document ID. Values are JSON-serialized `ManagedEntry`
/// strings with metadata fields.
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
        let transport = opensearch::http::transport::Transport::single_node(&url).map_err(|e| {
            Error::StoreSetup {
                message: format!("failed to create transport: {}", e),
            }
        })?;
        let client = opensearch::OpenSearch::new(transport);
        Ok(Self::new(client, index_prefix))
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

    fn index_name(&self, collection: &str) -> String {
        format!("{}-{}", self.config.index_prefix, collection)
    }

    fn os(&self) -> &opensearch::OpenSearch {
        self.client.client()
    }

    fn entry_to_doc(entry: &ManagedEntry, collection: &str) -> Result<OpenSearchDoc> {
        let value =
            serde_json::to_string(&entry.value).map_err(|e| Error::Serialization(e.to_string()))?;
        Ok(OpenSearchDoc {
            value,
            created_at: entry.created_at.map(|dt| dt.to_rfc3339()),
            expires_at: entry.expires_at.map(|dt| dt.to_rfc3339()),
            collection: collection.to_string(),
        })
    }

    fn doc_to_entry(doc: &OpenSearchDoc) -> Result<ManagedEntry> {
        let value: Value =
            serde_json::from_str(&doc.value).map_err(|e| Error::Deserialization(e.to_string()))?;
        let created_at = doc
            .created_at
            .as_ref()
            .and_then(|s| s.parse::<chrono::DateTime<chrono::Utc>>().ok());
        let expires_at = doc
            .expires_at
            .as_ref()
            .and_then(|s| s.parse::<chrono::DateTime<chrono::Utc>>().ok());
        Ok(ManagedEntry {
            value,
            created_at,
            expires_at,
        })
    }
}

#[async_trait]
impl AsyncKeyValue for OpenSearchStore {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        let cname = self.collection_name(collection);
        let index = self.index_name(cname);
        let response = self
            .os()
            .get(opensearch::GetParts::IndexId(&index, key))
            .send()
            .await
            .map_err(map_os_err)?;

        let body = response
            .json::<serde_json::Value>()
            .await
            .map_err(map_os_err)?;

        if body.get("found").and_then(|v| v.as_bool()).unwrap_or(false) {
            if let Some(source) = body.get("_source") {
                let doc: OpenSearchDoc = serde_json::from_value(source.clone())
                    .map_err(|e| Error::Deserialization(e.to_string()))?;
                let entry = Self::doc_to_entry(&doc)?;
                if entry.is_expired() {
                    let _ = self
                        .os()
                        .delete(opensearch::DeleteParts::IndexId(&index, key))
                        .send()
                        .await;
                    Ok(None)
                } else {
                    Ok(Some(entry.value))
                }
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    async fn ttl(&self, key: &str, collection: Option<&str>) -> Result<Option<(Value, f64)>> {
        let cname = self.collection_name(collection);
        let index = self.index_name(cname);
        let response = self
            .os()
            .get(opensearch::GetParts::IndexId(&index, key))
            .send()
            .await
            .map_err(map_os_err)?;

        let body = response
            .json::<serde_json::Value>()
            .await
            .map_err(map_os_err)?;

        if body.get("found").and_then(|v| v.as_bool()).unwrap_or(false) {
            if let Some(source) = body.get("_source") {
                let doc: OpenSearchDoc = serde_json::from_value(source.clone())
                    .map_err(|e| Error::Deserialization(e.to_string()))?;
                let entry = Self::doc_to_entry(&doc)?;
                if entry.is_expired() {
                    let _ = self
                        .os()
                        .delete(opensearch::DeleteParts::IndexId(&index, key))
                        .send()
                        .await;
                    Ok(None)
                } else {
                    let ttl = entry.ttl().unwrap_or(0.0);
                    Ok(Some((entry.value, ttl)))
                }
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    async fn put(
        &self,
        key: &str,
        value: Value,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        let cname = self.collection_name(collection);
        let index = self.index_name(cname);
        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds),
            None => ManagedEntry::new(value),
        };
        let doc = Self::entry_to_doc(&entry, cname)?;
        self.os()
            .index(opensearch::IndexParts::IndexId(&index, key))
            .body(doc)
            .send()
            .await
            .map_err(map_os_err)?;
        Ok(())
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let cname = self.collection_name(collection);
        let index = self.index_name(cname);
        let response = self
            .os()
            .delete(opensearch::DeleteParts::IndexId(&index, key))
            .send()
            .await
            .map_err(map_os_err)?;

        let body = response
            .json::<serde_json::Value>()
            .await
            .map_err(map_os_err)?;

        match body.get("result").and_then(|v| v.as_str()) {
            Some("deleted") => Ok(true),
            _ => Ok(false),
        }
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        let cname = self.collection_name(collection);
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.get(key, Some(cname)).await?);
        }
        Ok(results)
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, f64)>>> {
        let cname = self.collection_name(collection);
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.ttl(key, Some(cname)).await?);
        }
        Ok(results)
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
        let cname = self.collection_name(collection);
        for (key, value) in keys.iter().zip(values.iter()) {
            let entry = match ttl {
                Some(seconds) => ManagedEntry::with_ttl(value.clone(), seconds),
                None => ManagedEntry::new(value.clone()),
            };
            let doc = Self::entry_to_doc(&entry, cname)?;
            self.os()
                .index(opensearch::IndexParts::IndexId(
                    &self.index_name(cname),
                    key,
                ))
                .body(doc)
                .send()
                .await
                .map_err(map_os_err)?;
        }
        Ok(())
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        let cname = self.collection_name(collection);
        let mut count = 0;
        for key in keys {
            if self.delete(key, Some(cname)).await? {
                count += 1;
            }
        }
        Ok(count)
    }
}

#[async_trait]
impl AsyncCull for OpenSearchStore {
    async fn cull(&self) -> Result<()> {
        let pattern = format!("{}-*", self.config.index_prefix);
        let now = chrono::Utc::now().timestamp_millis();
        self.os()
            .delete_by_query(opensearch::DeleteByQueryParts::Index(&[&pattern]))
            .body(serde_json::json!({
                "query": {
                    "range": {
                        "expires_at": { "lt": now }
                    }
                }
            }))
            .send()
            .await
            .map_err(map_os_err)?;
        Ok(())
    }
}

#[async_trait]
impl AsyncEnumerateKeys for OpenSearchStore {
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        let cname = self.collection_name(collection);
        let index = self.index_name(cname);
        let limit = limit.unwrap_or(DEFAULT_PAGE_SIZE).min(PAGE_LIMIT);
        let response = self
            .os()
            .search(opensearch::SearchParts::Index(&[&index]))
            .body(serde_json::json!({
                "query": { "match_all": {} },
                "size": limit,
                "_source": false
            }))
            .send()
            .await
            .map_err(map_os_err)?;

        let body = response
            .json::<serde_json::Value>()
            .await
            .map_err(map_os_err)?;

        let mut keys = Vec::new();
        if let Some(hits) = body
            .get("hits")
            .and_then(|h| h.get("hits"))
            .and_then(|h| h.as_array())
        {
            for hit in hits {
                if let Some(id) = hit.get("_id").and_then(|v| v.as_str()) {
                    keys.push(id.to_string());
                }
            }
        }
        Ok(keys)
    }
}

#[async_trait]
impl AsyncEnumerateCollections for OpenSearchStore {
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        let pattern = format!("{}-*", self.config.index_prefix);
        let limit = limit.unwrap_or(DEFAULT_PAGE_SIZE).min(PAGE_LIMIT);
        let response = self
            .os()
            .search(opensearch::SearchParts::Index(&[&pattern]))
            .body(serde_json::json!({
                "query": { "match_all": {} },
                "size": 0,
                "aggs": {
                    "collections": {
                        "terms": {
                            "field": "collection",
                            "size": limit
                        }
                    }
                }
            }))
            .send()
            .await
            .map_err(map_os_err)?;

        let body = response
            .json::<serde_json::Value>()
            .await
            .map_err(map_os_err)?;

        let mut collections = Vec::new();
        if let Some(buckets) = body
            .get("aggregations")
            .and_then(|a| a.get("collections"))
            .and_then(|c| c.get("buckets"))
            .and_then(|b| b.as_array())
        {
            for bucket in buckets {
                if let Some(key) = bucket.get("key").and_then(|v| v.as_str()) {
                    collections.push(key.to_string());
                }
            }
        }
        Ok(collections)
    }
}

#[async_trait]
impl AsyncDestroyCollection for OpenSearchStore {
    async fn destroy_collection(&self, collection: &str) -> Result<bool> {
        let index = self.index_name(collection);
        let response = self
            .os()
            .indices()
            .delete(opensearch::indices::IndicesDeleteParts::Index(&[&index]))
            .send()
            .await;

        match response {
            Ok(_) => Ok(true),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("404") || msg.contains("NotFound") || msg.contains("not_found") {
                    Ok(false)
                } else {
                    Err(map_os_err(e))
                }
            }
        }
    }
}

#[async_trait]
impl AsyncDestroyStore for OpenSearchStore {
    async fn destroy(&self) -> Result<bool> {
        let pattern = format!("{}-*", self.config.index_prefix);
        let response = self
            .os()
            .indices()
            .delete(opensearch::indices::IndicesDeleteParts::Index(&[&pattern]))
            .send()
            .await;

        match response {
            Ok(_) => Ok(true),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("404") || msg.contains("NotFound") || msg.contains("not_found") {
                    Ok(true)
                } else {
                    Err(map_os_err(e))
                }
            }
        }
    }
}
