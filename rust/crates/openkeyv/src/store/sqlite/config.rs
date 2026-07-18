use super::error::{Error, Result};

const DEFAULT_COLLECTION: &str = "default_collection";
const DEFAULT_TABLE: &str = "kv_entries";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqliteConfig {
    pub table_name: String,
    pub default_collection: String,
}

impl SqliteConfig {
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
    if !name
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
    {
        return Err(Error::StoreSetup {
            message: format!("table name must start with an ASCII letter or underscore: {name}"),
        });
    }
    if !name
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return Err(Error::StoreSetup {
            message: format!(
                "table name must contain only ASCII letters, digits, and underscores: {name}"
            ),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rusqlite_table_name_validation_is_ascii_and_sql_safe() {
        assert!(SqliteConfig::new(Some("kv_entries_2")).is_ok());
        assert!(SqliteConfig::new(Some("_entries")).is_ok());
        assert!(SqliteConfig::new(Some("2entries")).is_err());
        assert!(SqliteConfig::new(Some("entries-name")).is_err());
        assert!(SqliteConfig::new(Some("数据")).is_err());
    }
}
