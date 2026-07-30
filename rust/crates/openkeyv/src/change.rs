use crate::error::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::fmt;

/// Opaque position in a store's ordered change history.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ChangeCursor(String);

impl ChangeCursor {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ChangeCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Mutation that produced a store change.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ChangeOperation {
    Put,
    Delete,
}

/// Durable technical record describing one successful KV mutation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreChange {
    pub cursor: ChangeCursor,
    pub revision: u64,
    pub collection: String,
    pub key: String,
    pub operation: ChangeOperation,
    pub occurred_at: DateTime<Utc>,
}

/// Backend position from which a subscription starts.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum ChangeStart {
    /// Replay all changes still retained by the backend.
    Beginning,
    /// Replay changes strictly after this cursor, then continue live.
    After(ChangeCursor),
    /// Receive only changes committed after subscription succeeds.
    #[default]
    Latest,
}

/// Optional server-side filtering for a change subscription.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChangeFilter {
    pub collections: Vec<String>,
    pub operations: Vec<ChangeOperation>,
}

impl ChangeFilter {
    pub fn collection(collection: impl Into<String>) -> Self {
        Self {
            collections: vec![collection.into()],
            operations: Vec::new(),
        }
    }

    pub fn matches(&self, change: &StoreChange) -> bool {
        (self.collections.is_empty() || self.collections.contains(&change.collection))
            && (self.operations.is_empty() || self.operations.contains(&change.operation))
    }
}

/// Parameters for opening a durable change stream.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChangeFeedRequest {
    pub start: ChangeStart,
    pub filter: ChangeFilter,
}

/// Backend-specific source used by [`ChangeSubscription`].
#[async_trait]
pub trait ChangeStream: Send {
    async fn recv(&mut self) -> Result<Option<StoreChange>>;
}

/// Owned change stream. Type alias retained so call sites and migration
/// helpers can keep a stable name; trait implementations return this directly.
pub type ChangeSubscription = Box<dyn ChangeStream + Send>;
