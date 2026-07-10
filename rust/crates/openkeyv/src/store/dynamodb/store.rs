use super::client::DynamoDBClient;
use super::config::DynamoDBConfig;
use super::error::{Error, Result};
use crate::entry::ManagedEntry;
use crate::protocol::{
    AsyncCull, AsyncDestroyCollection, AsyncDestroyStore, AsyncEnumerateCollections,
    AsyncEnumerateKeys, AsyncKeyValue,
};
use crate::value::Value;
use async_trait::async_trait;
use aws_sdk_dynamodb::client::Waiters;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, DeleteRequest, KeySchemaElement, KeyType,
    KeysAndAttributes, PutRequest, ReturnValue, ScalarAttributeType, TableDescription, TableStatus,
    TimeToLiveSpecification, TimeToLiveStatus, WriteRequest,
};
use bytes::Bytes;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;

const COLLECTION_ATTR: &str = "collection";
const KEY_ATTR: &str = "key";
const ENTRY_ATTR: &str = "entry";
const TTL_ATTR: &str = "ttl";
const MAX_BATCH_GET: usize = 100;
const MAX_BATCH_WRITE: usize = 25;
const MAX_ENUMERATION: usize = 10_000;

type Item = HashMap<String, AttributeValue>;

struct DecodedItem {
    collection: String,
    key: String,
    entry: ManagedEntry,
    encoded: Bytes,
    ttl: Option<i64>,
}

/// DynamoDB-backed key-value store.
///
/// Each item contains the composite string key (`collection`, `key`), one binary `entry`
/// containing the complete OpenKeyV `OKVE1` payload, and an optional numeric `ttl` used only
/// by DynamoDB native expiration.
pub struct DynamoDBStore {
    client: DynamoDBClient,
    config: DynamoDBConfig,
}

impl DynamoDBStore {
    pub async fn new(table_name: impl Into<String>) -> Result<Self> {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = aws_sdk_dynamodb::Client::new(&config);
        let store = Self::with_config(client, DynamoDBConfig::new(table_name, None));
        store.ensure_table().await?;
        Ok(store)
    }

    pub fn from_client(client: aws_sdk_dynamodb::Client, table_name: impl Into<String>) -> Self {
        Self::with_config(client, DynamoDBConfig::new(table_name, None))
    }

    pub fn with_config(client: aws_sdk_dynamodb::Client, config: DynamoDBConfig) -> Self {
        Self {
            client: DynamoDBClient::new(client),
            config,
        }
    }

    fn collection_name<'a>(&'a self, collection: Option<&'a str>) -> &'a str {
        collection.unwrap_or(&self.config.default_collection)
    }

    fn table_name(&self) -> &str {
        &self.config.table_name
    }

    fn client(&self) -> &aws_sdk_dynamodb::Client {
        self.client.client()
    }

    fn primary_key(collection: &str, key: &str) -> Item {
        HashMap::from([
            (
                COLLECTION_ATTR.to_string(),
                AttributeValue::S(collection.to_string()),
            ),
            (KEY_ATTR.to_string(), AttributeValue::S(key.to_string())),
        ])
    }

    fn native_ttl_seconds(expires_at_millis: i64) -> i64 {
        expires_at_millis.div_euclid(1000) + i64::from(expires_at_millis.rem_euclid(1000) != 0)
    }

    fn encode_item(collection: &str, key: &str, entry: &ManagedEntry) -> Item {
        let mut item = Self::primary_key(collection, key);
        item.insert(
            ENTRY_ATTR.to_string(),
            AttributeValue::B(Blob::new(entry.encode())),
        );
        if let Some(expires_at) = entry.expires_at {
            item.insert(
                TTL_ATTR.to_string(),
                AttributeValue::N(
                    Self::native_ttl_seconds(expires_at.timestamp_millis()).to_string(),
                ),
            );
        }
        item
    }

    fn decode_item(mut item: Item) -> Result<DecodedItem> {
        let mut names: Vec<&str> = item.keys().map(String::as_str).collect();
        names.sort_unstable();
        let valid_shape = matches!(item.len(), 3 | 4)
            && names
                .iter()
                .all(|name| matches!(*name, COLLECTION_ATTR | KEY_ATTR | ENTRY_ATTR | TTL_ATTR))
            && item.contains_key(COLLECTION_ATTR)
            && item.contains_key(KEY_ATTR)
            && item.contains_key(ENTRY_ATTR);
        if !valid_shape {
            return Err(Error::Deserialization(format!(
                "DynamoDB item must contain exactly collection, key, entry, and optional ttl; found [{}]",
                names.join(", ")
            )));
        }

        let collection = match item.remove(COLLECTION_ATTR) {
            Some(AttributeValue::S(value)) => value,
            Some(_) => {
                return Err(Error::Deserialization(
                    "DynamoDB item collection must be a string".to_string(),
                ));
            }
            None => {
                return Err(Error::Deserialization(
                    "DynamoDB item is missing collection".to_string(),
                ));
            }
        };
        let key = match item.remove(KEY_ATTR) {
            Some(AttributeValue::S(value)) => value,
            Some(_) => {
                return Err(Error::Deserialization(
                    "DynamoDB item key must be a string".to_string(),
                ));
            }
            None => {
                return Err(Error::Deserialization(
                    "DynamoDB item is missing key".to_string(),
                ));
            }
        };
        let encoded = match item.remove(ENTRY_ATTR) {
            Some(AttributeValue::B(value)) => Bytes::from(value.into_inner()),
            Some(_) => {
                return Err(Error::Deserialization(
                    "DynamoDB item entry must be binary".to_string(),
                ));
            }
            None => {
                return Err(Error::Deserialization(
                    "DynamoDB item is missing entry".to_string(),
                ));
            }
        };
        let ttl = match item.remove(TTL_ATTR) {
            Some(AttributeValue::N(value)) => Some(value.parse::<i64>().map_err(|_| {
                Error::Deserialization(format!(
                    "DynamoDB item ttl must be an integer, found {value}"
                ))
            })?),
            Some(_) => {
                return Err(Error::Deserialization(
                    "DynamoDB item ttl must be a number".to_string(),
                ));
            }
            None => None,
        };

        let entry = ManagedEntry::decode(encoded.clone()).map_err(|error| {
            Error::Deserialization(format!(
                "failed to decode OpenKeyV entry in DynamoDB item {collection}/{key}: {error}"
            ))
        })?;
        let expected_ttl = entry
            .expires_at
            .map(|expires_at| Self::native_ttl_seconds(expires_at.timestamp_millis()));
        if ttl != expected_ttl {
            return Err(Error::Deserialization(format!(
                "DynamoDB item {collection}/{key} ttl {ttl:?} does not match embedded expiration {expected_ttl:?}"
            )));
        }

        Ok(DecodedItem {
            collection,
            key,
            entry,
            encoded,
            ttl,
        })
    }

    fn decode_physical_key(mut item: Item) -> Result<(String, String)> {
        let mut names: Vec<&str> = item.keys().map(String::as_str).collect();
        names.sort_unstable();
        if names != [COLLECTION_ATTR, KEY_ATTR] {
            return Err(Error::Deserialization(format!(
                "DynamoDB key projection must contain exactly collection and key; found [{}]",
                names.join(", ")
            )));
        }

        let collection = match item.remove(COLLECTION_ATTR) {
            Some(AttributeValue::S(value)) => value,
            Some(_) => {
                return Err(Error::Deserialization(
                    "DynamoDB projected collection must be a string".to_string(),
                ));
            }
            None => {
                return Err(Error::Deserialization(
                    "DynamoDB key projection is missing collection".to_string(),
                ));
            }
        };
        let key = match item.remove(KEY_ATTR) {
            Some(AttributeValue::S(value)) => value,
            Some(_) => {
                return Err(Error::Deserialization(
                    "DynamoDB projected key must be a string".to_string(),
                ));
            }
            None => {
                return Err(Error::Deserialization(
                    "DynamoDB key projection is missing key".to_string(),
                ));
            }
        };
        Ok((collection, key))
    }

    async fn ensure_table(&self) -> Result<()> {
        match self
            .client()
            .describe_table()
            .table_name(self.table_name())
            .send()
            .await
        {
            Ok(_) => {}
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(|service| service.is_resource_not_found_exception()) =>
            {
                let collection_schema = KeySchemaElement::builder()
                    .attribute_name(COLLECTION_ATTR)
                    .key_type(KeyType::Hash)
                    .build()
                    .map_err(|error| Error::StoreSetup {
                        message: format!(
                            "failed to build DynamoDB collection key schema for {}: {error}",
                            self.table_name()
                        ),
                    })?;
                let key_schema = KeySchemaElement::builder()
                    .attribute_name(KEY_ATTR)
                    .key_type(KeyType::Range)
                    .build()
                    .map_err(|error| Error::StoreSetup {
                        message: format!(
                            "failed to build DynamoDB range key schema for {}: {error}",
                            self.table_name()
                        ),
                    })?;
                let collection_definition = AttributeDefinition::builder()
                    .attribute_name(COLLECTION_ATTR)
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .map_err(|error| Error::StoreSetup {
                        message: format!(
                            "failed to build DynamoDB collection attribute for {}: {error}",
                            self.table_name()
                        ),
                    })?;
                let key_definition = AttributeDefinition::builder()
                    .attribute_name(KEY_ATTR)
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .map_err(|error| Error::StoreSetup {
                        message: format!(
                            "failed to build DynamoDB key attribute for {}: {error}",
                            self.table_name()
                        ),
                    })?;

                self.client()
                    .create_table()
                    .table_name(self.table_name())
                    .key_schema(collection_schema)
                    .key_schema(key_schema)
                    .attribute_definitions(collection_definition)
                    .attribute_definitions(key_definition)
                    .billing_mode(BillingMode::PayPerRequest)
                    .send()
                    .await
                    .map_err(|error| Error::StoreSetup {
                        message: format!(
                            "failed to create DynamoDB table {}: {error}",
                            self.table_name()
                        ),
                    })?;
            }
            Err(error) => {
                return Err(Error::StoreSetup {
                    message: format!(
                        "failed to describe DynamoDB table {}: {error}",
                        self.table_name()
                    ),
                });
            }
        }

        self.client()
            .wait_until_table_exists()
            .table_name(self.table_name())
            .wait(Duration::from_secs(300))
            .await
            .map_err(|error| Error::StoreSetup {
                message: format!(
                    "DynamoDB table {} did not become ACTIVE: {error}",
                    self.table_name()
                ),
            })?;

        let output = self
            .client()
            .describe_table()
            .table_name(self.table_name())
            .send()
            .await
            .map_err(|error| Error::StoreSetup {
                message: format!(
                    "failed to validate DynamoDB table {} after waiting: {error}",
                    self.table_name()
                ),
            })?;
        let table = output.table().ok_or_else(|| Error::StoreSetup {
            message: format!(
                "DynamoDB describe_table returned no table description for {}",
                self.table_name()
            ),
        })?;
        self.validate_table(table)?;
        self.ensure_ttl().await
    }

    fn validate_table(&self, table: &TableDescription) -> Result<()> {
        if table.table_status() != Some(&TableStatus::Active) {
            return Err(Error::StoreSetup {
                message: format!(
                    "DynamoDB table {} is not ACTIVE; status is {:?}",
                    self.table_name(),
                    table.table_status().map(TableStatus::as_str)
                ),
            });
        }

        let schema = table.key_schema();
        let valid_schema = schema.len() == 2
            && schema.iter().any(|element| {
                element.attribute_name() == COLLECTION_ATTR && element.key_type() == &KeyType::Hash
            })
            && schema.iter().any(|element| {
                element.attribute_name() == KEY_ATTR && element.key_type() == &KeyType::Range
            });
        if !valid_schema {
            return Err(Error::StoreSetup {
                message: format!(
                    "DynamoDB table {} must use collection as HASH and key as RANGE",
                    self.table_name()
                ),
            });
        }

        let definitions = table.attribute_definitions();
        let valid_definitions = definitions.len() == 2
            && definitions.iter().any(|definition| {
                definition.attribute_name() == COLLECTION_ATTR
                    && definition.attribute_type() == &ScalarAttributeType::S
            })
            && definitions.iter().any(|definition| {
                definition.attribute_name() == KEY_ATTR
                    && definition.attribute_type() == &ScalarAttributeType::S
            });
        if !valid_definitions {
            return Err(Error::StoreSetup {
                message: format!(
                    "DynamoDB table {} must define collection and key as the only string key attributes",
                    self.table_name()
                ),
            });
        }
        Ok(())
    }

    async fn ensure_ttl(&self) -> Result<()> {
        let output = self
            .client()
            .describe_time_to_live()
            .table_name(self.table_name())
            .send()
            .await
            .map_err(|error| Error::StoreSetup {
                message: format!(
                    "failed to describe DynamoDB TTL for {}: {error}",
                    self.table_name()
                ),
            })?;
        let description = output
            .time_to_live_description()
            .ok_or_else(|| Error::StoreSetup {
                message: format!(
                    "DynamoDB TTL description is missing for {}",
                    self.table_name()
                ),
            })?;
        let status = description
            .time_to_live_status()
            .ok_or_else(|| Error::StoreSetup {
                message: format!("DynamoDB TTL status is missing for {}", self.table_name()),
            })?;
        if description
            .attribute_name()
            .is_some_and(|attribute| attribute != TTL_ATTR)
        {
            return Err(Error::StoreSetup {
                message: format!(
                    "DynamoDB table {} uses TTL attribute {:?}, expected {}",
                    self.table_name(),
                    description.attribute_name(),
                    TTL_ATTR
                ),
            });
        }

        match status {
            TimeToLiveStatus::Enabled | TimeToLiveStatus::Enabling => {
                if description.attribute_name() != Some(TTL_ATTR) {
                    return Err(Error::StoreSetup {
                        message: format!(
                            "DynamoDB table {} has TTL status {} without attribute {}",
                            self.table_name(),
                            status.as_str(),
                            TTL_ATTR
                        ),
                    });
                }
                Ok(())
            }
            TimeToLiveStatus::Disabled => {
                let specification = TimeToLiveSpecification::builder()
                    .attribute_name(TTL_ATTR)
                    .enabled(true)
                    .build()
                    .map_err(|error| Error::StoreSetup {
                        message: format!(
                            "failed to build DynamoDB TTL specification for {}: {error}",
                            self.table_name()
                        ),
                    })?;
                self.client()
                    .update_time_to_live()
                    .table_name(self.table_name())
                    .time_to_live_specification(specification)
                    .send()
                    .await
                    .map_err(|error| Error::StoreSetup {
                        message: format!(
                            "failed to enable DynamoDB TTL for {}: {error}",
                            self.table_name()
                        ),
                    })?;

                let verified = self
                    .client()
                    .describe_time_to_live()
                    .table_name(self.table_name())
                    .send()
                    .await
                    .map_err(|error| Error::StoreSetup {
                        message: format!(
                            "failed to verify DynamoDB TTL for {}: {error}",
                            self.table_name()
                        ),
                    })?;
                let verified =
                    verified
                        .time_to_live_description()
                        .ok_or_else(|| Error::StoreSetup {
                            message: format!(
                                "DynamoDB TTL description is missing after enabling {}",
                                self.table_name()
                            ),
                        })?;
                let verified_status =
                    verified
                        .time_to_live_status()
                        .ok_or_else(|| Error::StoreSetup {
                            message: format!(
                                "DynamoDB TTL status is missing after enabling {}",
                                self.table_name()
                            ),
                        })?;
                if !matches!(
                    verified_status,
                    TimeToLiveStatus::Enabled | TimeToLiveStatus::Enabling
                ) || verified.attribute_name() != Some(TTL_ATTR)
                {
                    return Err(Error::StoreSetup {
                        message: format!(
                            "DynamoDB TTL for {} was not enabled on {}: status={}, attribute={:?}",
                            self.table_name(),
                            TTL_ATTR,
                            verified_status.as_str(),
                            verified.attribute_name()
                        ),
                    });
                }
                Ok(())
            }
            TimeToLiveStatus::Disabling => Err(Error::StoreSetup {
                message: format!("DynamoDB TTL for {} is DISABLING", self.table_name()),
            }),
            other => Err(Error::StoreSetup {
                message: format!(
                    "DynamoDB TTL for {} has unsupported status {}",
                    self.table_name(),
                    other.as_str()
                ),
            }),
        }
    }

    async fn delete_observed_expired(&self, item: &DecodedItem) -> Result<()> {
        let ttl = item.ttl.ok_or_else(|| {
            Error::Deserialization(format!(
                "expired DynamoDB item {}/{} has no native ttl",
                item.collection, item.key
            ))
        })?;
        let result = self
            .client()
            .delete_item()
            .table_name(self.table_name())
            .set_key(Some(Self::primary_key(&item.collection, &item.key)))
            .condition_expression("#entry = :entry AND #ttl = :ttl")
            .expression_attribute_names("#entry", ENTRY_ATTR)
            .expression_attribute_names("#ttl", TTL_ATTR)
            .expression_attribute_values(
                ":entry",
                AttributeValue::B(Blob::new(item.encoded.to_vec())),
            )
            .expression_attribute_values(":ttl", AttributeValue::N(ttl.to_string()))
            .send()
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(|service| service.is_conditional_check_failed_exception()) =>
            {
                Ok(())
            }
            Err(error) => Err(Error::StoreConnection {
                message: format!(
                    "failed to conditionally delete expired DynamoDB item {}/{}: {error}",
                    item.collection, item.key
                ),
            }),
        }
    }

    async fn read_entry(&self, collection: &str, key: &str) -> Result<Option<ManagedEntry>> {
        let output = self
            .client()
            .get_item()
            .table_name(self.table_name())
            .set_key(Some(Self::primary_key(collection, key)))
            .consistent_read(true)
            .send()
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to read DynamoDB item {collection}/{key}: {error}"),
            })?;
        let Some(item) = output.item else {
            return Ok(None);
        };
        let decoded = Self::decode_item(item)?;
        if decoded.collection != collection || decoded.key != key {
            return Err(Error::Deserialization(format!(
                "DynamoDB get_item for {collection}/{key} returned {}/{}",
                decoded.collection, decoded.key
            )));
        }
        if decoded.entry.is_expired() {
            self.delete_observed_expired(&decoded).await?;
            return Ok(None);
        }
        Ok(Some(decoded.entry))
    }

    async fn read_entries(
        &self,
        collection: &str,
        keys: &[String],
    ) -> Result<HashMap<String, ManagedEntry>> {
        let mut seen = HashSet::with_capacity(keys.len());
        let mut unique_keys = Vec::with_capacity(keys.len());
        for key in keys {
            if seen.insert(key.as_str()) {
                unique_keys.push(key);
            }
        }

        let mut decoded_items = HashMap::with_capacity(unique_keys.len());
        for chunk in unique_keys.chunks(MAX_BATCH_GET) {
            let request_keys: Vec<Item> = chunk
                .iter()
                .map(|key| Self::primary_key(collection, key))
                .collect();
            let requested: HashSet<&str> = chunk.iter().map(|key| key.as_str()).collect();
            let request = KeysAndAttributes::builder()
                .set_keys(Some(request_keys))
                .consistent_read(true)
                .build()
                .map_err(|error| Error::StoreConnection {
                    message: format!(
                        "failed to build DynamoDB batch read for {collection}: {error}"
                    ),
                })?;
            let output = self
                .client()
                .batch_get_item()
                .request_items(self.table_name(), request)
                .send()
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!(
                        "failed to batch read DynamoDB items in {collection}: {error}"
                    ),
                })?;
            if output
                .unprocessed_keys()
                .is_some_and(|unprocessed| !unprocessed.is_empty())
            {
                return Err(Error::StoreConnection {
                    message: format!(
                        "DynamoDB batch read in {collection} returned unprocessed keys"
                    ),
                });
            }

            for (table, items) in output.responses.unwrap_or_default() {
                if table != self.table_name() {
                    return Err(Error::StoreConnection {
                        message: format!(
                            "DynamoDB batch read for {} returned unrelated table {table}",
                            self.table_name()
                        ),
                    });
                }
                for item in items {
                    let decoded = Self::decode_item(item)?;
                    if decoded.collection != collection || !requested.contains(decoded.key.as_str())
                    {
                        return Err(Error::Deserialization(format!(
                            "DynamoDB batch read in {collection} returned unrelated item {}/{}",
                            decoded.collection, decoded.key
                        )));
                    }
                    let item_key = decoded.key.clone();
                    if decoded_items.insert(item_key.clone(), decoded).is_some() {
                        return Err(Error::Deserialization(format!(
                            "DynamoDB batch read returned {collection}/{item_key} more than once"
                        )));
                    }
                }
            }
        }

        let expired: Vec<String> = decoded_items
            .iter()
            .filter(|(_, item)| item.entry.is_expired())
            .map(|(key, _)| key.clone())
            .collect();
        for key in expired {
            let item = decoded_items
                .remove(&key)
                .expect("expired key came from decoded items");
            self.delete_observed_expired(&item).await?;
        }

        Ok(decoded_items
            .into_iter()
            .map(|(key, item)| (key, item.entry))
            .collect())
    }
}

#[async_trait]
impl AsyncKeyValue for DynamoDBStore {
    async fn get(&self, key: &str, collection: Option<&str>) -> Result<Option<Value>> {
        let collection = self.collection_name(collection);
        Ok(self
            .read_entry(collection, key)
            .await?
            .map(|entry| entry.value))
    }

    async fn ttl(&self, key: &str, collection: Option<&str>) -> Result<Option<(Value, f64)>> {
        let collection = self.collection_name(collection);
        Ok(self.read_entry(collection, key).await?.map(|entry| {
            let ttl = entry.ttl().unwrap_or(0.0);
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
        let collection = self.collection_name(collection);
        let entry = match ttl {
            Some(seconds) => ManagedEntry::with_ttl(value, seconds),
            None => ManagedEntry::new(value),
        };
        self.client()
            .put_item()
            .table_name(self.table_name())
            .set_item(Some(Self::encode_item(collection, key, &entry)))
            .send()
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to write DynamoDB item {collection}/{key}: {error}"),
            })?;
        Ok(())
    }

    async fn delete(&self, key: &str, collection: Option<&str>) -> Result<bool> {
        let collection = self.collection_name(collection);
        let output = self
            .client()
            .delete_item()
            .table_name(self.table_name())
            .set_key(Some(Self::primary_key(collection, key)))
            .return_values(ReturnValue::AllOld)
            .send()
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!("failed to delete DynamoDB item {collection}/{key}: {error}"),
            })?;
        Ok(output.attributes.is_some())
    }

    async fn get_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<Value>>> {
        let collection = self.collection_name(collection);
        let entries = self.read_entries(collection, keys).await?;
        Ok(keys
            .iter()
            .map(|key| entries.get(key).map(|entry| entry.value.clone()))
            .collect())
    }

    async fn ttl_many(
        &self,
        keys: &[String],
        collection: Option<&str>,
    ) -> Result<Vec<Option<(Value, f64)>>> {
        let collection = self.collection_name(collection);
        let entries = self.read_entries(collection, keys).await?;
        Ok(keys
            .iter()
            .map(|key| {
                entries
                    .get(key)
                    .map(|entry| (entry.value.clone(), entry.ttl().unwrap_or(0.0)))
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

        let collection = self.collection_name(collection);
        let mut seen = HashSet::with_capacity(keys.len());
        let mut selected = Vec::with_capacity(keys.len());
        for index in (0..keys.len()).rev() {
            if seen.insert(keys[index].as_str()) {
                selected.push(index);
            }
        }
        selected.reverse();

        for chunk in selected.chunks(MAX_BATCH_WRITE) {
            let mut requests = Vec::with_capacity(chunk.len());
            for &index in chunk {
                let entry = match ttl {
                    Some(seconds) => ManagedEntry::with_ttl(values[index].clone(), seconds),
                    None => ManagedEntry::new(values[index].clone()),
                };
                let put = PutRequest::builder()
                    .set_item(Some(Self::encode_item(collection, &keys[index], &entry)))
                    .build()
                    .map_err(|error| Error::StoreConnection {
                        message: format!(
                            "failed to build DynamoDB batch write for {collection}/{}: {error}",
                            keys[index]
                        ),
                    })?;
                requests.push(WriteRequest::builder().put_request(put).build());
            }

            let output = self
                .client()
                .batch_write_item()
                .request_items(self.table_name(), requests)
                .send()
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!(
                        "failed to batch write DynamoDB items in {collection}: {error}"
                    ),
                })?;
            if output
                .unprocessed_items()
                .is_some_and(|unprocessed| !unprocessed.is_empty())
            {
                return Err(Error::StoreConnection {
                    message: format!(
                        "DynamoDB batch write in {collection} returned unprocessed items"
                    ),
                });
            }
        }
        Ok(())
    }

    async fn delete_many(&self, keys: &[String], collection: Option<&str>) -> Result<usize> {
        let collection = self.collection_name(collection);
        let mut seen = HashSet::with_capacity(keys.len());
        let mut unique_keys = Vec::with_capacity(keys.len());
        for key in keys {
            if seen.insert(key.as_str()) {
                unique_keys.push(key);
            }
        }

        let mut existing = HashSet::with_capacity(unique_keys.len());
        for chunk in unique_keys.chunks(MAX_BATCH_GET) {
            let request_keys: Vec<Item> = chunk
                .iter()
                .map(|key| Self::primary_key(collection, key))
                .collect();
            let requested: HashSet<&str> = chunk.iter().map(|key| key.as_str()).collect();
            let request = KeysAndAttributes::builder()
                .set_keys(Some(request_keys))
                .consistent_read(true)
                .projection_expression("#collection, #key")
                .expression_attribute_names("#collection", COLLECTION_ATTR)
                .expression_attribute_names("#key", KEY_ATTR)
                .build()
                .map_err(|error| Error::StoreConnection {
                    message: format!(
                        "failed to build DynamoDB delete existence read for {collection}: {error}"
                    ),
                })?;
            let output = self
                .client()
                .batch_get_item()
                .request_items(self.table_name(), request)
                .send()
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!(
                        "failed to read DynamoDB delete candidates in {collection}: {error}"
                    ),
                })?;
            if output
                .unprocessed_keys()
                .is_some_and(|unprocessed| !unprocessed.is_empty())
            {
                return Err(Error::StoreConnection {
                    message: format!(
                        "DynamoDB delete existence read in {collection} returned unprocessed keys"
                    ),
                });
            }

            for (table, items) in output.responses.unwrap_or_default() {
                if table != self.table_name() {
                    return Err(Error::StoreConnection {
                        message: format!(
                            "DynamoDB delete existence read for {} returned unrelated table {table}",
                            self.table_name()
                        ),
                    });
                }
                for item in items {
                    let (item_collection, key) = Self::decode_physical_key(item)?;
                    if item_collection != collection || !requested.contains(key.as_str()) {
                        return Err(Error::Deserialization(format!(
                            "DynamoDB delete existence read in {collection} returned unrelated item {item_collection}/{key}"
                        )));
                    }
                    if !existing.insert(key.clone()) {
                        return Err(Error::Deserialization(format!(
                            "DynamoDB delete existence read returned {collection}/{key} more than once"
                        )));
                    }
                }
            }
        }

        let existing_keys: Vec<&String> = unique_keys
            .into_iter()
            .filter(|key| existing.contains(key.as_str()))
            .collect();
        for chunk in existing_keys.chunks(MAX_BATCH_WRITE) {
            let mut requests = Vec::with_capacity(chunk.len());
            for key in chunk {
                let delete = DeleteRequest::builder()
                    .set_key(Some(Self::primary_key(collection, key)))
                    .build()
                    .map_err(|error| Error::StoreConnection {
                        message: format!(
                            "failed to build DynamoDB batch delete for {collection}/{key}: {error}"
                        ),
                    })?;
                requests.push(WriteRequest::builder().delete_request(delete).build());
            }
            let output = self
                .client()
                .batch_write_item()
                .request_items(self.table_name(), requests)
                .send()
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!(
                        "failed to batch delete DynamoDB items in {collection}: {error}"
                    ),
                })?;
            if output
                .unprocessed_items()
                .is_some_and(|unprocessed| !unprocessed.is_empty())
            {
                return Err(Error::StoreConnection {
                    message: format!(
                        "DynamoDB batch delete in {collection} returned unprocessed items"
                    ),
                });
            }
        }
        Ok(existing.len())
    }
}

#[async_trait]
impl AsyncCull for DynamoDBStore {
    async fn cull(&self) -> Result<()> {
        let mut last_key: Option<Item> = None;
        loop {
            let mut request = self.client().scan().table_name(self.table_name());
            if let Some(key) = last_key {
                request = request.set_exclusive_start_key(Some(key));
            }
            let output = request
                .send()
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!(
                        "failed to scan DynamoDB table {} while culling: {error}",
                        self.table_name()
                    ),
                })?;
            let next_key = output.last_evaluated_key;
            for item in output.items.unwrap_or_default() {
                let decoded = Self::decode_item(item)?;
                if decoded.entry.is_expired() {
                    self.delete_observed_expired(&decoded).await?;
                }
            }
            match next_key {
                Some(key) if !key.is_empty() => last_key = Some(key),
                _ => break,
            }
        }
        Ok(())
    }
}

#[async_trait]
impl AsyncEnumerateKeys for DynamoDBStore {
    async fn keys(&self, collection: Option<&str>, limit: Option<usize>) -> Result<Vec<String>> {
        let collection = self.collection_name(collection);
        let target = limit.unwrap_or(MAX_ENUMERATION).min(MAX_ENUMERATION);
        if target == 0 {
            return Ok(Vec::new());
        }

        let mut keys = Vec::with_capacity(target);
        let mut seen = HashSet::with_capacity(target);
        let mut last_key: Option<Item> = None;
        while keys.len() < target {
            let remaining = target - keys.len();
            let request_limit = i32::try_from(remaining).unwrap_or(i32::MAX);
            let mut request = self
                .client()
                .query()
                .table_name(self.table_name())
                .key_condition_expression("#collection = :collection")
                .expression_attribute_names("#collection", COLLECTION_ATTR)
                .expression_attribute_values(
                    ":collection",
                    AttributeValue::S(collection.to_string()),
                )
                .consistent_read(true)
                .limit(request_limit);
            if let Some(key) = last_key {
                request = request.set_exclusive_start_key(Some(key));
            }
            let output = request
                .send()
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to enumerate DynamoDB keys in {collection}: {error}"),
                })?;
            let next_key = output.last_evaluated_key;
            for item in output.items.unwrap_or_default() {
                let decoded = Self::decode_item(item)?;
                if decoded.collection != collection {
                    return Err(Error::Deserialization(format!(
                        "DynamoDB key query for {collection} returned {}/{}",
                        decoded.collection, decoded.key
                    )));
                }
                if !seen.insert(decoded.key.clone()) {
                    return Err(Error::Deserialization(format!(
                        "DynamoDB key query returned {collection}/{} more than once",
                        decoded.key
                    )));
                }
                if decoded.entry.is_expired() {
                    self.delete_observed_expired(&decoded).await?;
                } else {
                    keys.push(decoded.key);
                    if keys.len() == target {
                        break;
                    }
                }
            }
            match next_key {
                Some(key) if !key.is_empty() => last_key = Some(key),
                _ => break,
            }
        }
        Ok(keys)
    }
}

#[async_trait]
impl AsyncEnumerateCollections for DynamoDBStore {
    async fn collections(&self, limit: Option<usize>) -> Result<Vec<String>> {
        let target = limit.unwrap_or(MAX_ENUMERATION).min(MAX_ENUMERATION);
        if target == 0 {
            return Ok(Vec::new());
        }

        let mut collections = BTreeSet::new();
        let mut seen_items = HashSet::new();
        let mut last_key: Option<Item> = None;
        while collections.len() < target {
            let mut request = self.client().scan().table_name(self.table_name());
            if let Some(key) = last_key {
                request = request.set_exclusive_start_key(Some(key));
            }
            let output = request
                .send()
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!(
                        "failed to enumerate DynamoDB collections in {}: {error}",
                        self.table_name()
                    ),
                })?;
            let next_key = output.last_evaluated_key;
            for item in output.items.unwrap_or_default() {
                let decoded = Self::decode_item(item)?;
                if !seen_items.insert((decoded.collection.clone(), decoded.key.clone())) {
                    return Err(Error::Deserialization(format!(
                        "DynamoDB collection scan returned {}/{} more than once",
                        decoded.collection, decoded.key
                    )));
                }
                if decoded.entry.is_expired() {
                    self.delete_observed_expired(&decoded).await?;
                } else {
                    collections.insert(decoded.collection);
                    if collections.len() == target {
                        break;
                    }
                }
            }
            match next_key {
                Some(key) if !key.is_empty() => last_key = Some(key),
                _ => break,
            }
        }
        Ok(collections.into_iter().collect())
    }
}

#[async_trait]
impl AsyncDestroyCollection for DynamoDBStore {
    async fn destroy_collection(&self, collection: &str) -> Result<bool> {
        let mut deleted_any = false;
        loop {
            let output = self
                .client()
                .query()
                .table_name(self.table_name())
                .key_condition_expression("#collection = :collection")
                .expression_attribute_names("#collection", COLLECTION_ATTR)
                .expression_attribute_names("#key", KEY_ATTR)
                .expression_attribute_values(
                    ":collection",
                    AttributeValue::S(collection.to_string()),
                )
                .projection_expression("#collection, #key")
                .consistent_read(true)
                .limit(MAX_BATCH_WRITE as i32)
                .send()
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!(
                        "failed to list physical DynamoDB items in collection {collection}: {error}"
                    ),
                })?;
            let mut keys = Vec::new();
            for item in output.items.unwrap_or_default() {
                let (item_collection, key) = Self::decode_physical_key(item)?;
                if item_collection != collection {
                    return Err(Error::Deserialization(format!(
                        "DynamoDB collection destruction for {collection} returned {item_collection}/{key}"
                    )));
                }
                keys.push(key);
            }
            if keys.is_empty() {
                return Ok(deleted_any);
            }

            let mut requests = Vec::with_capacity(keys.len());
            for key in &keys {
                let delete = DeleteRequest::builder()
                    .set_key(Some(Self::primary_key(collection, key)))
                    .build()
                    .map_err(|error| Error::StoreConnection {
                        message: format!(
                            "failed to build DynamoDB collection delete for {collection}/{key}: {error}"
                        ),
                    })?;
                requests.push(WriteRequest::builder().delete_request(delete).build());
            }
            let output = self
                .client()
                .batch_write_item()
                .request_items(self.table_name(), requests)
                .send()
                .await
                .map_err(|error| Error::StoreConnection {
                    message: format!("failed to delete DynamoDB collection {collection}: {error}"),
                })?;
            if output
                .unprocessed_items()
                .is_some_and(|unprocessed| !unprocessed.is_empty())
            {
                return Err(Error::StoreConnection {
                    message: format!(
                        "DynamoDB collection deletion for {collection} returned unprocessed items"
                    ),
                });
            }
            deleted_any = true;
        }
    }
}

#[async_trait]
impl AsyncDestroyStore for DynamoDBStore {
    async fn destroy(&self) -> Result<bool> {
        match self
            .client()
            .delete_table()
            .table_name(self.table_name())
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(|service| service.is_resource_not_found_exception()) =>
            {
                Ok(false)
            }
            Err(error) => Err(Error::StoreConnection {
                message: format!(
                    "failed to delete DynamoDB table {}: {error}",
                    self.table_name()
                ),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::config::{Credentials, Region};
    use chrono::{TimeZone, Utc};

    fn fixed_entry(expires_at_millis: Option<i64>) -> ManagedEntry {
        ManagedEntry {
            value: Value::utf8("value"),
            created_at: Some(Utc.timestamp_millis_opt(1_000).single().unwrap()),
            expires_at: expires_at_millis
                .map(|millis| Utc.timestamp_millis_opt(millis).single().unwrap()),
        }
    }

    #[test]
    fn dynamodb_item_uses_exact_binary_shape() {
        let entry = fixed_entry(None);
        let item = DynamoDBStore::encode_item("collection", "key", &entry);

        assert_eq!(item.len(), 3);
        assert_eq!(
            item.get(COLLECTION_ATTR),
            Some(&AttributeValue::S("collection".to_string()))
        );
        assert_eq!(
            item.get(KEY_ATTR),
            Some(&AttributeValue::S("key".to_string()))
        );
        let AttributeValue::B(encoded) = item.get(ENTRY_ATTR).unwrap() else {
            panic!("entry must be binary");
        };
        assert!(encoded.as_ref().starts_with(b"OKVE1"));
        assert!(!item.contains_key(TTL_ATTR));

        let decoded = DynamoDBStore::decode_item(item).unwrap();
        assert_eq!(decoded.collection, "collection");
        assert_eq!(decoded.key, "key");
        assert_eq!(decoded.entry, entry);
    }

    #[test]
    fn dynamodb_native_ttl_uses_ceiling_seconds() {
        assert_eq!(DynamoDBStore::native_ttl_seconds(1_000), 1);
        assert_eq!(DynamoDBStore::native_ttl_seconds(1_001), 2);
        assert_eq!(DynamoDBStore::native_ttl_seconds(-1), 0);
        assert_eq!(DynamoDBStore::native_ttl_seconds(-1_001), -1);

        let entry = fixed_entry(Some(1_001));
        let item = DynamoDBStore::encode_item("collection", "key", &entry);
        assert_eq!(
            item.get(TTL_ATTR),
            Some(&AttributeValue::N("2".to_string()))
        );
    }

    #[test]
    fn dynamodb_item_rejects_unknown_missing_and_wrong_type_fields() {
        let entry = fixed_entry(None);

        let mut unknown = DynamoDBStore::encode_item("collection", "key", &entry);
        unknown.insert("value".to_string(), AttributeValue::S("old".to_string()));
        assert!(
            DynamoDBStore::decode_item(unknown)
                .err()
                .expect("unknown field must fail")
                .to_string()
                .contains("must contain exactly")
        );

        let mut missing = DynamoDBStore::encode_item("collection", "key", &entry);
        missing.remove(ENTRY_ATTR);
        assert!(
            DynamoDBStore::decode_item(missing)
                .err()
                .expect("missing entry must fail")
                .to_string()
                .contains("must contain exactly")
        );

        let mut wrong_type = DynamoDBStore::encode_item("collection", "key", &entry);
        wrong_type.insert(
            ENTRY_ATTR.to_string(),
            AttributeValue::S("not-binary".to_string()),
        );
        assert!(
            DynamoDBStore::decode_item(wrong_type)
                .err()
                .expect("wrong entry type must fail")
                .to_string()
                .contains("entry must be binary")
        );
    }

    #[test]
    fn dynamodb_item_rejects_old_json_and_ttl_mismatches() {
        let old_json = HashMap::from([
            (
                COLLECTION_ATTR.to_string(),
                AttributeValue::S("collection".to_string()),
            ),
            (KEY_ATTR.to_string(), AttributeValue::S("key".to_string())),
            (
                "value".to_string(),
                AttributeValue::S(r#"{"value":null}"#.to_string()),
            ),
        ]);
        assert!(DynamoDBStore::decode_item(old_json).is_err());

        let expiring = fixed_entry(Some(1_001));
        let mut missing_ttl = DynamoDBStore::encode_item("collection", "key", &expiring);
        missing_ttl.remove(TTL_ATTR);
        assert!(
            DynamoDBStore::decode_item(missing_ttl)
                .err()
                .expect("missing ttl must fail")
                .to_string()
                .contains("does not match embedded expiration")
        );

        let mut wrong_ttl = DynamoDBStore::encode_item("collection", "key", &expiring);
        wrong_ttl.insert(TTL_ATTR.to_string(), AttributeValue::N("1".to_string()));
        assert!(
            DynamoDBStore::decode_item(wrong_ttl)
                .err()
                .expect("wrong ttl must fail")
                .to_string()
                .contains("does not match embedded expiration")
        );

        let mut unexpected_ttl =
            DynamoDBStore::encode_item("collection", "key", &fixed_entry(None));
        unexpected_ttl.insert(TTL_ATTR.to_string(), AttributeValue::N("2".to_string()));
        assert!(
            DynamoDBStore::decode_item(unexpected_ttl)
                .err()
                .expect("unexpected ttl must fail")
                .to_string()
                .contains("does not match embedded expiration")
        );
    }

    #[tokio::test]
    #[ignore = "requires OPENKEYV_DYNAMODB_ENDPOINT"]
    async fn dynamodb_stores_exact_binary_items_and_configures_ttl() {
        let endpoint = std::env::var("OPENKEYV_DYNAMODB_ENDPOINT")
            .expect("OPENKEYV_DYNAMODB_ENDPOINT must point to DynamoDB Local");
        let table_name = format!(
            "openkeyv-native-{}-{}",
            std::process::id(),
            Utc::now().timestamp_millis()
        );
        let config = aws_sdk_dynamodb::Config::builder()
            .behavior_version_latest()
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                "openkeyv",
                "openkeyv",
                None,
                None,
                "openkeyv-tests",
            ))
            .endpoint_url(endpoint)
            .build();
        let client = aws_sdk_dynamodb::Client::from_conf(config);
        let store = DynamoDBStore::from_client(client.clone(), &table_name);
        store.ensure_table().await.unwrap();

        let ttl_description = client
            .describe_time_to_live()
            .table_name(&table_name)
            .send()
            .await
            .unwrap()
            .time_to_live_description
            .expect("TTL description must exist");
        assert_eq!(ttl_description.attribute_name(), Some(TTL_ATTR));
        assert!(ttl_description.time_to_live_status().is_some_and(|status| {
            matches!(
                status,
                TimeToLiveStatus::Enabled | TimeToLiveStatus::Enabling
            )
        }));

        store
            .put("plain", Value::utf8("plain"), None, None)
            .await
            .unwrap();
        store
            .put("expiring", Value::utf8("expiring"), None, Some(60.0))
            .await
            .unwrap();

        for (key, expected_fields) in [("plain", 3), ("expiring", 4)] {
            let raw = client
                .get_item()
                .table_name(&table_name)
                .set_key(Some(DynamoDBStore::primary_key(
                    store.collection_name(None),
                    key,
                )))
                .consistent_read(true)
                .send()
                .await
                .unwrap()
                .item
                .expect("stored item must exist");
            assert_eq!(raw.len(), expected_fields);
            assert!(raw.keys().all(|name| {
                matches!(
                    name.as_str(),
                    COLLECTION_ATTR | KEY_ATTR | ENTRY_ATTR | TTL_ATTR
                )
            }));
            let AttributeValue::B(encoded) = raw.get(ENTRY_ATTR).unwrap() else {
                panic!("entry must use DynamoDB binary");
            };
            assert!(encoded.as_ref().starts_with(b"OKVE1"));
            let decoded = DynamoDBStore::decode_item(raw).unwrap();
            assert_eq!(decoded.collection, store.collection_name(None));
            assert_eq!(decoded.key, key);
            assert_eq!(
                decoded.ttl,
                decoded
                    .entry
                    .expires_at
                    .map(|expires_at| DynamoDBStore::native_ttl_seconds(
                        expires_at.timestamp_millis()
                    ))
            );
        }

        assert_eq!(
            store.get("plain", None).await.unwrap(),
            Some(Value::utf8("plain"))
        );
        let ttl = store.ttl("expiring", None).await.unwrap().unwrap();
        assert_eq!(ttl.0, Value::utf8("expiring"));
        assert!(ttl.1 > 0.0 && ttl.1 <= 60.0);

        assert!(store.destroy().await.unwrap());
        let missing = DynamoDBStore::from_client(
            client,
            format!(
                "openkeyv-missing-{}-{}",
                std::process::id(),
                Utc::now().timestamp_millis()
            ),
        );
        assert!(!missing.destroy().await.unwrap());
    }

    #[tokio::test]
    #[ignore = "requires OPENKEYV_DYNAMODB_ENDPOINT"]
    async fn dynamodb_batches_cross_service_limits_and_preserve_semantics() {
        let endpoint = std::env::var("OPENKEYV_DYNAMODB_ENDPOINT")
            .expect("OPENKEYV_DYNAMODB_ENDPOINT must point to DynamoDB Local");
        let table_name = format!(
            "openkeyv-batch-{}-{}",
            std::process::id(),
            Utc::now().timestamp_millis()
        );
        let config = aws_sdk_dynamodb::Config::builder()
            .behavior_version_latest()
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                "openkeyv",
                "openkeyv",
                None,
                None,
                "openkeyv-tests",
            ))
            .endpoint_url(endpoint)
            .build();
        let client = aws_sdk_dynamodb::Client::from_conf(config);
        let store = DynamoDBStore::from_client(client, &table_name);
        store.ensure_table().await.unwrap();

        let mut keys: Vec<String> = (0..130).map(|index| format!("key-{index:03}")).collect();
        let mut values: Vec<Value> = (0..130)
            .map(|index| Value::utf8(format!("value-{index:03}")))
            .collect();
        keys.push("key-005".to_string());
        values.push(Value::utf8("last-key-005"));
        store
            .put_many(&keys, &values, None, Some(60.0))
            .await
            .unwrap();

        let mut read_keys: Vec<String> = (0..130)
            .rev()
            .map(|index| format!("key-{index:03}"))
            .collect();
        read_keys.push("missing".to_string());
        read_keys.push("key-005".to_string());
        let read_values = store.get_many(&read_keys, None).await.unwrap();
        assert_eq!(read_values.len(), read_keys.len());
        for (key, value) in read_keys.iter().zip(read_values) {
            let expected = match key.as_str() {
                "missing" => None,
                "key-005" => Some(Value::utf8("last-key-005")),
                _ => Some(Value::utf8(key.replacen("key-", "value-", 1))),
            };
            assert_eq!(value, expected, "{key}");
        }

        let ttl_values = store
            .ttl_many(
                &[
                    "key-005".to_string(),
                    "missing".to_string(),
                    "key-129".to_string(),
                    "key-005".to_string(),
                ],
                None,
            )
            .await
            .unwrap();
        assert_eq!(ttl_values.len(), 4);
        assert_eq!(
            ttl_values[0].as_ref().map(|(value, _)| value),
            Some(&Value::utf8("last-key-005"))
        );
        assert!(ttl_values[0].as_ref().unwrap().1 > 0.0);
        assert!(ttl_values[1].is_none());
        assert_eq!(
            ttl_values[2].as_ref().map(|(value, _)| value),
            Some(&Value::utf8("value-129"))
        );
        assert_eq!(
            ttl_values[3].as_ref().map(|(value, _)| value),
            Some(&Value::utf8("last-key-005"))
        );

        let mut delete_keys: Vec<String> =
            (0..130).map(|index| format!("key-{index:03}")).collect();
        delete_keys.push("key-005".to_string());
        delete_keys.push("missing".to_string());
        assert_eq!(store.delete_many(&delete_keys, None).await.unwrap(), 130);
        assert_eq!(store.delete_many(&delete_keys, None).await.unwrap(), 0);
        assert!(store.destroy().await.unwrap());
    }

    #[tokio::test]
    #[ignore = "requires OPENKEYV_DYNAMODB_ENDPOINT"]
    async fn dynamodb_ttl_enumeration_cull_and_conditional_cleanup_are_strict() {
        let endpoint = std::env::var("OPENKEYV_DYNAMODB_ENDPOINT")
            .expect("OPENKEYV_DYNAMODB_ENDPOINT must point to DynamoDB Local");
        let table_name = format!(
            "openkeyv-ttl-{}-{}",
            std::process::id(),
            Utc::now().timestamp_millis()
        );
        let config = aws_sdk_dynamodb::Config::builder()
            .behavior_version_latest()
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                "openkeyv",
                "openkeyv",
                None,
                None,
                "openkeyv-tests",
            ))
            .endpoint_url(endpoint)
            .build();
        let client = aws_sdk_dynamodb::Client::from_conf(config);
        let store = DynamoDBStore::from_client(client.clone(), &table_name);
        store.ensure_table().await.unwrap();

        store
            .put("alpha-key", Value::utf8("alpha"), Some("alpha"), None)
            .await
            .unwrap();
        store
            .put("beta-key", Value::utf8("beta"), Some("beta"), None)
            .await
            .unwrap();
        store
            .put(
                "expired-only",
                Value::utf8("expired"),
                Some("expired"),
                Some(-1.0),
            )
            .await
            .unwrap();
        assert_eq!(
            store.keys(Some("alpha"), None).await.unwrap(),
            vec!["alpha-key".to_string()]
        );
        assert_eq!(
            store.collections(None).await.unwrap(),
            vec!["alpha".to_string(), "beta".to_string()]
        );

        store
            .put(
                "subsecond",
                Value::utf8("subsecond"),
                Some("ttl"),
                Some(0.5),
            )
            .await
            .unwrap();
        let raw = client
            .get_item()
            .table_name(&table_name)
            .set_key(Some(DynamoDBStore::primary_key("ttl", "subsecond")))
            .consistent_read(true)
            .send()
            .await
            .unwrap()
            .item
            .unwrap();
        let decoded = DynamoDBStore::decode_item(raw).unwrap();
        assert_eq!(
            decoded.ttl,
            decoded
                .entry
                .expires_at
                .map(|expires_at| DynamoDBStore::native_ttl_seconds(expires_at.timestamp_millis()))
        );
        tokio::time::sleep(Duration::from_millis(650)).await;
        assert_eq!(store.get("subsecond", Some("ttl")).await.unwrap(), None);
        assert!(
            client
                .get_item()
                .table_name(&table_name)
                .set_key(Some(DynamoDBStore::primary_key("ttl", "subsecond")))
                .consistent_read(true)
                .send()
                .await
                .unwrap()
                .item
                .is_none()
        );

        store
            .put("race", Value::utf8("expired"), Some("race"), Some(-1.0))
            .await
            .unwrap();
        let observed = client
            .get_item()
            .table_name(&table_name)
            .set_key(Some(DynamoDBStore::primary_key("race", "race")))
            .consistent_read(true)
            .send()
            .await
            .unwrap()
            .item
            .map(DynamoDBStore::decode_item)
            .transpose()
            .unwrap()
            .expect("expired item must exist before cleanup");
        store
            .put("race", Value::utf8("replacement"), Some("race"), None)
            .await
            .unwrap();
        store.delete_observed_expired(&observed).await.unwrap();
        assert_eq!(
            store.get("race", Some("race")).await.unwrap(),
            Some(Value::utf8("replacement"))
        );

        store
            .put("cull", Value::utf8("expired"), Some("cull"), Some(-1.0))
            .await
            .unwrap();
        store.cull().await.unwrap();
        assert!(
            client
                .get_item()
                .table_name(&table_name)
                .set_key(Some(DynamoDBStore::primary_key("cull", "cull")))
                .consistent_read(true)
                .send()
                .await
                .unwrap()
                .item
                .is_none()
        );
        assert!(store.destroy().await.unwrap());
    }

    #[tokio::test]
    #[ignore = "requires OPENKEYV_DYNAMODB_ENDPOINT"]
    async fn dynamodb_rejects_malformed_items_and_destroys_corrupt_collections() {
        let endpoint = std::env::var("OPENKEYV_DYNAMODB_ENDPOINT")
            .expect("OPENKEYV_DYNAMODB_ENDPOINT must point to DynamoDB Local");
        let table_name = format!(
            "openkeyv-invalid-{}-{}",
            std::process::id(),
            Utc::now().timestamp_millis()
        );
        let config = aws_sdk_dynamodb::Config::builder()
            .behavior_version_latest()
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                "openkeyv",
                "openkeyv",
                None,
                None,
                "openkeyv-tests",
            ))
            .endpoint_url(endpoint)
            .build();
        let client = aws_sdk_dynamodb::Client::from_conf(config);
        let store = DynamoDBStore::from_client(client.clone(), &table_name);
        store.ensure_table().await.unwrap();

        let invalid_items = [
            (
                "old-json",
                HashMap::from([
                    (
                        COLLECTION_ATTR.to_string(),
                        AttributeValue::S("invalid".to_string()),
                    ),
                    (
                        KEY_ATTR.to_string(),
                        AttributeValue::S("old-json".to_string()),
                    ),
                    (
                        "value".to_string(),
                        AttributeValue::S(r#"{"value":null}"#.to_string()),
                    ),
                ]),
            ),
            (
                "wrong-entry-type",
                HashMap::from([
                    (
                        COLLECTION_ATTR.to_string(),
                        AttributeValue::S("invalid".to_string()),
                    ),
                    (
                        KEY_ATTR.to_string(),
                        AttributeValue::S("wrong-entry-type".to_string()),
                    ),
                    (
                        ENTRY_ATTR.to_string(),
                        AttributeValue::S("not-binary".to_string()),
                    ),
                ]),
            ),
            (
                "corrupt-entry",
                HashMap::from([
                    (
                        COLLECTION_ATTR.to_string(),
                        AttributeValue::S("invalid".to_string()),
                    ),
                    (
                        KEY_ATTR.to_string(),
                        AttributeValue::S("corrupt-entry".to_string()),
                    ),
                    (
                        ENTRY_ATTR.to_string(),
                        AttributeValue::B(Blob::new(br#"{"value":null}"#.to_vec())),
                    ),
                ]),
            ),
        ];
        for (key, item) in invalid_items {
            client
                .put_item()
                .table_name(&table_name)
                .set_item(Some(item))
                .send()
                .await
                .unwrap();
            let error = store.get(key, Some("invalid")).await.unwrap_err();
            assert!(matches!(error, Error::Deserialization(_)), "{key}: {error}");
        }

        client
            .put_item()
            .table_name(&table_name)
            .set_item(Some(HashMap::from([
                (
                    COLLECTION_ATTR.to_string(),
                    AttributeValue::S("corrupt-collection".to_string()),
                ),
                (
                    KEY_ATTR.to_string(),
                    AttributeValue::S("corrupt".to_string()),
                ),
                (
                    ENTRY_ATTR.to_string(),
                    AttributeValue::B(Blob::new(br#"{"value":null}"#.to_vec())),
                ),
            ])))
            .send()
            .await
            .unwrap();
        assert!(
            store
                .destroy_collection("corrupt-collection")
                .await
                .unwrap()
        );
        assert!(
            !store
                .destroy_collection("corrupt-collection")
                .await
                .unwrap()
        );
        assert!(store.destroy().await.unwrap());
    }

    #[tokio::test]
    #[ignore = "requires OPENKEYV_DYNAMODB_ENDPOINT"]
    async fn dynamodb_setup_rejects_wrong_schema_and_ttl_attribute() {
        let endpoint = std::env::var("OPENKEYV_DYNAMODB_ENDPOINT")
            .expect("OPENKEYV_DYNAMODB_ENDPOINT must point to DynamoDB Local");
        let config = aws_sdk_dynamodb::Config::builder()
            .behavior_version_latest()
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                "openkeyv",
                "openkeyv",
                None,
                None,
                "openkeyv-tests",
            ))
            .endpoint_url(endpoint)
            .build();
        let client = aws_sdk_dynamodb::Client::from_conf(config);

        let wrong_schema_table = format!(
            "openkeyv-wrong-schema-{}-{}",
            std::process::id(),
            Utc::now().timestamp_millis()
        );
        client
            .create_table()
            .table_name(&wrong_schema_table)
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("pk")
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("pk")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await
            .unwrap();
        let wrong_schema = DynamoDBStore::from_client(client.clone(), &wrong_schema_table);
        let error = wrong_schema.ensure_table().await.unwrap_err();
        assert!(matches!(error, Error::StoreSetup { .. }));
        assert!(
            error
                .to_string()
                .contains("collection as HASH and key as RANGE")
        );
        client
            .delete_table()
            .table_name(&wrong_schema_table)
            .send()
            .await
            .unwrap();

        let wrong_ttl_table = format!(
            "openkeyv-wrong-ttl-{}-{}",
            std::process::id(),
            Utc::now().timestamp_millis()
        );
        client
            .create_table()
            .table_name(&wrong_ttl_table)
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(COLLECTION_ATTR)
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(KEY_ATTR)
                    .key_type(KeyType::Range)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(COLLECTION_ATTR)
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(KEY_ATTR)
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await
            .unwrap();
        client
            .update_time_to_live()
            .table_name(&wrong_ttl_table)
            .time_to_live_specification(
                TimeToLiveSpecification::builder()
                    .attribute_name("expires")
                    .enabled(true)
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();
        let wrong_ttl = DynamoDBStore::from_client(client.clone(), &wrong_ttl_table);
        let error = wrong_ttl.ensure_table().await.unwrap_err();
        assert!(matches!(error, Error::StoreSetup { .. }));
        assert!(error.to_string().contains("expected ttl"));
        client
            .delete_table()
            .table_name(&wrong_ttl_table)
            .send()
            .await
            .unwrap();
    }
}
