use std::sync::Arc;

use crate::protocol::{
    AsyncChangeFeed, AsyncCompareAndSwap, AsyncEnumerateCollections, AsyncEnumerateKeys, BaseStore,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StoreCapabilities {
    pub enumerate_keys: bool,
    pub enumerate_collections: bool,
    pub compare_and_swap: bool,
    pub change_feed: bool,
}

pub struct StoreHandle {
    pub base: Arc<dyn BaseStore>,
    pub capabilities: StoreCapabilities,
    pub enumerate_keys: Option<Arc<dyn AsyncEnumerateKeys>>,
    pub enumerate_collections: Option<Arc<dyn AsyncEnumerateCollections>>,
    pub compare_and_swap: Option<Arc<dyn AsyncCompareAndSwap>>,
    pub change_feed: Option<Arc<dyn AsyncChangeFeed>>,
}

impl StoreHandle {
    pub fn basic<T>(store: Arc<T>) -> Self
    where
        T: BaseStore + 'static,
    {
        Self {
            base: store,
            capabilities: StoreCapabilities::default(),
            enumerate_keys: None,
            enumerate_collections: None,
            compare_and_swap: None,
            change_feed: None,
        }
    }

    pub fn with_capabilities<T>(
        store: Arc<T>,
        enumerate_keys: Option<Arc<dyn AsyncEnumerateKeys>>,
        enumerate_collections: Option<Arc<dyn AsyncEnumerateCollections>>,
        compare_and_swap: Option<Arc<dyn AsyncCompareAndSwap>>,
        change_feed: Option<Arc<dyn AsyncChangeFeed>>,
    ) -> Self
    where
        T: BaseStore + 'static,
    {
        Self {
            base: store,
            capabilities: StoreCapabilities {
                enumerate_keys: enumerate_keys.is_some(),
                enumerate_collections: enumerate_collections.is_some(),
                compare_and_swap: compare_and_swap.is_some(),
                change_feed: change_feed.is_some(),
            },
            enumerate_keys,
            enumerate_collections,
            compare_and_swap,
            change_feed,
        }
    }
}
