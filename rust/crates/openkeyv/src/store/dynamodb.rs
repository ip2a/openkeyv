use crate::entry::ManagedEntry;
use crate::error::{Error, Result};
use crate::protocol::{
    AsyncCull, AsyncDestroyCollection, AsyncDestroyStore, AsyncEnumerateCollections,
    AsyncEnumerateKeys, AsyncKeyValue,
};
use async_trait::async_trait;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType,
};
use serde_json::Value;
use std::collections::HashMap;

const DEFAULT_COLLECTION: &str = "default_collection";

fn build_err(e: aws_sdk_dynamodb::error::BuildError) -> Error {
    Error::StoreSetup {
        message: e.to_string(),
    }
}

/// DynamoDB-backed key-value store.
///
/// Uses a single table with `collection` (HASH) and `key` (RANGE) as the composite primary key.
/// Values are stored as JSON strings in a `value` attribute. TTL is supported via a `ttl` attribute
/// (Unix epoch seconds) with DynamoDB native TTL.
pub struct DynamoDBStore {
    client: aws_sdk_dynamodb::Client,
    table_name: String,
    default_collection: String,
}

impl DynamoDBStore {
    pub async fn new(table_name: impl Into<String>) -> Result<Self> {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = aws_sdk_dynamodb::Client::new(&config);
        let store = Self {
            client,
            table_name: table_name.into(),
            default_collection: DEFAULT_COLLECTION.to_string(),
        };
        store.ensure_table().await?;
        Ok(store)
    }

    pub fn from_client(client: aws_sdk_dynamodb::Client, table_name: impl Into<String>) -> Self {
        Self {
            client,
            table_name: table_name.into(),
            default_collection: DEFAULT_COLLECTION.to_string(),
        }
    }

    fn collection_name<'a>(&'a self, collection: Option<&'a str>) -> &'a str {
        collection.unwrap_or(&self.default_collection)
    }

    async fn ensure_table(&self) -> Result<()> {
        let exists = self
            .client
            .describe_table()
            .table_name(&self.table_name)
            .send()
            .await
            .is_ok();

        if !exists {
            self.client
                .create_table()
                .table_name(&self.table_name)
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("collection")
                        .key_type(KeyType::Hash)
                        .build()
                        .map_err(build_err)?,
                )
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("key")
                        .key_type(KeyType::Range)
                        .build()
                        .map_err(build_err)?,
                )
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name("collection")
                        .attribute_type(ScalarAttributeType::S)
                        .build()
                        .map_err(build_err)?,
                )
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name("key")
                        .attribute_type(ScalarAttributeType::S)
                        .build()
                        .map_err(build_err)?,
                )
                .billing_mode(BillingMode::PayPerRequest)
                .send()
                .await
                .map_err(|e| Error::StoreConnection {
                    message: format!("{}", e),
                })?;
        }
        Ok(())
    }

    fn item_to_entry(item: &HashMap<String, AttributeValue>) -> Result<Option<ManagedEntry>> {
        let value_json = item
            .get("value")
            .and_then(|v| v.as_s().ok())
            .ok_or_else(|| Error::Deserialization("missing value".to_string()))?;
        let value: HashMap<String, Value> =
            serde_json::from_str(value_json).map_err(|e| Error::Deserialization(e.to_string()))?;

        let created_at = item
            .get("created_at")
            .and_then(|v| v.as_s().ok())
            .and_then(|s| s.parse::<chrono::DateTime<chrono::Utc>>().ok());

        let expires_at = item
            .get("ttl")
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<i64>().ok())
            .and_then(|ts| chrono::DateTime::from_timestamp(ts, 0));

        let entry = ManagedEntry {
            value,
            created_at,
            expires_at,
        };
        if entry.is_expired() {
            Ok(None)
        } else {
            Ok(Some(entry))
        }
    }

    fn entry_to_item(
        key: &str,
        collection: &str,
        entry: &ManagedEntry,
    ) -> Result<HashMap<String, AttributeValue>> {
        let json =
            serde_json::to_string(&entry.value).map_err(|e| Error::Serialization(e.to_string()))?;
        let mut item = HashMap::new();
        item.insert(
            "collection".to_string(),
            AttributeValue::S(collection.to_string()),
        );
        item.insert("key".to_string(), AttributeValue::S(key.to_string()));
        item.insert("value".to_string(), AttributeValue::S(json));
        if let Some(dt) = entry.created_at {
            item.insert("created_at".to_string(), AttributeValue::S(dt.to_rfc3339()));
        }
        if let Some(dt) = entry.expires_at {
            item.insert(
                "ttl".to_string(),
                AttributeValue::N(dt.timestamp().to_string()),
            );
        }
        Ok(item)
    }
}

#[async_trait]
impl AsyncKeyValue for DynamoDBStore {
    async fn get(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<HashMap<String, Value>>> {
        let cname = self.collection_name(collection);
        let res = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key("collection", AttributeValue::S(cname.to_string()))
            .key("key", AttributeValue::S(key.to_string()))
            .send()
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("{}", e),
            })?;
        match res.item {
            Some(item) => Ok(Self::item_to_entry(&item)?.map(|e| e.value)),
            None => Ok(None),
        }
    }

    async fn ttl(
        &self,
        key: &str,
        collection: Option<&str>,
    ) -> Result<Option<(HashMap<String, Value>, f64)>> {
        let cname = self.collection_name(collection);
        let res = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key("collection", AttributeValue::S(cname.to_string()))
            .key("key", AttributeValue::S(key.to_string()))
            .send()
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("{}", e),
            })?;
        match res.item {
            Some(item) => match Self::item_to_entry(&item)? {
                Some(entry) => {
                    let ttl = entry.ttl().unwrap_or(0.0);
                    Ok(Some((entry.value, ttl)))
                }
                None => Ok(None),
            },
            None => Ok(None),
        }
    }

    async fn put(
        &self,
        key: &str,
        value: HashMap<String, Value>,
        collection: Option<&str>,
        ttl: Option<f64>,
    ) -> Result<()> {
        let cname = self.collection_name(collection);
        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds),
            None => ManagedEntry::new(value),
        };
        let item = Self::entry_to_item(key, cname, &entry)?;
        self.client
            .put_item()
            .table_name(&self.table_name)
            .set_item(Some(item))
            .send()
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("{}", e),
            })?;
        Ok(())
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let cname = self.collection_name(collection);
        let res = self
            .client
            .delete_item()
            .table_name(&self.table_name)
            .key("collection", AttributeValue::S(cname.to_string()))
            .key("key", AttributeValue::S(key.to_string()))
            .return_values(aws_sdk_dynamodb::types::ReturnValue::AllOld)
            .send()
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("{}", e),
            })?;
        Ok(res.attributes.is_some())
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<HashMap<String, Value>>>> {
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
    ) -> Result<Vec<Option<(HashMap<String, Value>, f64)>>> {
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
        values: &[HashMap<String, Value>],
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
            let item = Self::entry_to_item(key, cname, &entry)?;
            self.client
                .put_item()
                .table_name(&self.table_name)
                .set_item(Some(item))
                .send()
                .await
                .map_err(|e| Error::StoreConnection {
                    message: format!("{}", e),
                })?;
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
impl AsyncCull for DynamoDBStore {
    async fn cull(&self) -> Result<()> {
        // DynamoDB native TTL handles expiration automatically.
        Ok(())
    }
}

#[async_trait]
impl AsyncEnumerateKeys for DynamoDBStore {
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        let cname = self.collection_name(collection);
        let limit = limit.unwrap_or(10_000).min(10_000) as i32;
        let mut keys = Vec::new();
        let mut last_key: Option<HashMap<String, AttributeValue>> = None;
        loop {
            let mut req = self
                .client
                .query()
                .table_name(&self.table_name)
                .key_condition_expression("collection = :c")
                .expression_attribute_values(":c", AttributeValue::S(cname.to_string()))
                .projection_expression("key")
                .limit(limit);
            if let Some(ref k) = last_key {
                req = req.set_exclusive_start_key(Some(k.clone()));
            }
            let res = req.send().await.map_err(|e| Error::StoreConnection {
                message: format!("{}", e),
            })?;
            if let Some(items) = res.items {
                for item in items {
                    if let Some(key) = item.get("key").and_then(|v| v.as_s().ok()) {
                        keys.push(key.to_string());
                    }
                }
            }
            if res.last_evaluated_key.is_none() || keys.len() >= limit as usize {
                break;
            }
            last_key = res.last_evaluated_key;
        }
        Ok(keys)
    }
}

#[async_trait]
impl AsyncEnumerateCollections for DynamoDBStore {
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        let limit = limit.unwrap_or(10_000).min(10_000) as i32;
        let mut collections = std::collections::HashSet::new();
        let mut last_key: Option<HashMap<String, AttributeValue>> = None;
        loop {
            let mut req = self
                .client
                .scan()
                .table_name(&self.table_name)
                .projection_expression("collection")
                .limit(limit);
            if let Some(ref k) = last_key {
                req = req.set_exclusive_start_key(Some(k.clone()));
            }
            let res = req.send().await.map_err(|e| Error::StoreConnection {
                message: format!("{}", e),
            })?;
            if let Some(items) = res.items {
                for item in items {
                    if let Some(c) = item.get("collection").and_then(|v| v.as_s().ok()) {
                        collections.insert(c.to_string());
                    }
                }
            }
            if res.last_evaluated_key.is_none() || collections.len() >= limit as usize {
                break;
            }
            last_key = res.last_evaluated_key;
        }
        Ok(collections.into_iter().collect())
    }
}

#[async_trait]
impl AsyncDestroyCollection for DynamoDBStore {
    async fn destroy_collection(&self, collection: &str) -> Result<bool> {
        let keys = self.keys(Some(collection), None).await?;
        if keys.is_empty() {
            return Ok(false);
        }
        for key in keys {
            self.delete(&key, Some(collection)).await?;
        }
        Ok(true)
    }
}

#[async_trait]
impl AsyncDestroyStore for DynamoDBStore {
    async fn destroy(&self) -> Result<bool> {
        self.client
            .delete_table()
            .table_name(&self.table_name)
            .send()
            .await
            .map_err(|e| Error::StoreConnection {
                message: format!("{}", e),
            })?;
        Ok(true)
    }
}
