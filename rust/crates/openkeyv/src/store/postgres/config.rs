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
    if name.as_bytes()[0].is_ascii_digit() {
        return Err(Error::StoreSetup {
            message: format!("table name must not start with a digit: {name}"),
        });
    }
    if !name
        .as_bytes()
        .iter()
        .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
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
    fn postgres_table_names_are_strict_ascii_identifiers() {
        for valid in ["kv_store", "_private", "Table42"] {
            assert!(PostgresConfig::new(Some(valid)).is_ok());
        }

        for invalid in ["", "1table", "has-dash", "table name", "数据"] {
            assert!(PostgresConfig::new(Some(invalid)).is_err());
        }

        assert!(PostgresConfig::new(Some(&"a".repeat(64))).is_err());
    }
}
