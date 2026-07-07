use super::error::{Error, Result};

const DEFAULT_COLLECTION: &str = "default_collection";
const DEFAULT_TABLE: &str = "kv_store";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostgresConfig {
    pub table_name: String,
    pub default_collection: String,
}

impl PostgresConfig {
    pub fn new(table_name: Option<&str>) -> Result<Self> {
        let table_name = table_name.unwrap_or(DEFAULT_TABLE).to_string();
        validate_table_name(&table_name)?;
        Ok(Self {
            table_name,
            default_collection: DEFAULT_COLLECTION.to_string(),
        })
    }
}

fn validate_table_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::StoreSetup {
            message: "table name cannot be empty".to_string(),
        });
    }
    if name.len() > 63 {
        return Err(Error::StoreSetup {
            message: format!("table name too long (>63): {name}"),
        });
    }
    if name.chars().next().unwrap().is_ascii_digit() {
        return Err(Error::StoreSetup {
            message: format!("table name must not start with a digit: {name}"),
        });
    }
    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(Error::StoreSetup {
            message: format!("table name must be alphanumeric (with underscores): {name}"),
        });
    }
    Ok(())
}
