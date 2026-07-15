const PHYSICAL_PREFIX: &str = "okv1-";
pub(crate) const MAX_USERNAME_BYTES: usize = 513;
pub(crate) const MAX_TARGET_BYTES: usize = 32_767;
pub(crate) const MAX_SECRET_BYTES: usize = 2_560;

use crate::utils::compound::compound_key;

#[derive(Clone)]
pub struct KeyringClient {
    service_name: String,
}

impl KeyringClient {
    pub fn new(service_name: String) -> Self {
        Self { service_name }
    }

    fn physical_username(collection: &str, key: &str) -> keyring::Result<String> {
        let identity = compound_key(collection, key);
        let encoded_len = PHYSICAL_PREFIX
            .len()
            .checked_add(identity.len().checked_mul(2).ok_or_else(|| {
                keyring::Error::TooLong("user".to_string(), MAX_USERNAME_BYTES as u32)
            })?)
            .ok_or_else(|| {
                keyring::Error::TooLong("user".to_string(), MAX_USERNAME_BYTES as u32)
            })?;
        if encoded_len > MAX_USERNAME_BYTES {
            return Err(keyring::Error::TooLong(
                "user".to_string(),
                MAX_USERNAME_BYTES as u32,
            ));
        }

        let mut username = String::with_capacity(encoded_len);
        username.push_str(PHYSICAL_PREFIX);
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for byte in identity.as_bytes() {
            username.push(HEX[(byte >> 4) as usize] as char);
            username.push(HEX[(byte & 0x0f) as usize] as char);
        }
        Ok(username)
    }

    pub(crate) fn entry(&self, collection: &str, key: &str) -> keyring::Result<keyring::Entry> {
        let username = Self::physical_username(collection, key)?;

        if self.service_name.is_empty() {
            return Err(keyring::Error::Invalid(
                "service".to_string(),
                "cannot be empty".to_string(),
            ));
        }
        if self.service_name.contains('\0') {
            return Err(keyring::Error::Invalid(
                "service".to_string(),
                "cannot contain NUL".to_string(),
            ));
        }
        let target_len = username
            .len()
            .checked_add(1)
            .and_then(|len| len.checked_add(self.service_name.len()))
            .ok_or_else(|| {
                keyring::Error::TooLong("target".to_string(), MAX_TARGET_BYTES as u32)
            })?;
        if target_len > MAX_TARGET_BYTES {
            return Err(keyring::Error::TooLong(
                "target".to_string(),
                MAX_TARGET_BYTES as u32,
            ));
        }

        keyring::Entry::new(&self.service_name, &username)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::compound::decompound_key;

    fn decode_username(username: &str) -> (String, String) {
        let encoded = username.strip_prefix(PHYSICAL_PREFIX).unwrap();
        let bytes: Vec<u8> = encoded
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let digit = |byte: u8| match byte {
                    b'0'..=b'9' => byte - b'0',
                    b'a'..=b'f' => byte - b'a' + 10,
                    _ => panic!("noncanonical hex"),
                };
                (digit(pair[0]) << 4) | digit(pair[1])
            })
            .collect();
        let identity = String::from_utf8(bytes).unwrap();
        let (collection, key) = decompound_key(&identity).unwrap();
        (collection.to_string(), key.to_string())
    }

    #[test]
    fn physical_username_is_lowercase_nul_safe_and_reversible() {
        for (collection, key) in [
            ("", ""),
            ("Users", "Key"),
            ("users", "Key"),
            ("é", "e\u{301}"),
            ("/", "."),
            ("..", "__name__"),
            ("control\u{0001}", "nul\0key"),
            ("a:b", "c"),
            ("a", "b:c"),
        ] {
            let username = KeyringClient::physical_username(collection, key).unwrap();
            assert!(username.starts_with(PHYSICAL_PREFIX));
            assert!(
                username
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            );
            assert!(!username.contains('\0'));
            assert_eq!(
                decode_username(&username),
                (collection.to_string(), key.to_string())
            );
        }

        let left = KeyringClient::physical_username("a:b", "c").unwrap();
        let right = KeyringClient::physical_username("a", "b:c").unwrap();
        assert_ne!(left, right);
    }

    #[test]
    fn physical_username_has_a_nonempty_canonical_empty_identity() {
        let identity = compound_key("", "");
        assert_eq!(identity, "0:");
        assert_eq!(PHYSICAL_PREFIX.len() + identity.len() * 2, 9);
    }

    #[test]
    fn physical_username_enforces_the_portable_windows_limit() {
        let key = "x".repeat(252);
        let entry = KeyringClient::new("service".to_string()).entry("", &key);
        assert!(entry.is_ok());

        let key = "x".repeat(253);
        assert!(matches!(
            KeyringClient::new("service".to_string()).entry("", &key),
            Err(keyring::Error::TooLong(name, max)) if name == "user" && max as usize == MAX_USERNAME_BYTES
        ));
    }

    #[test]
    fn service_is_portably_nonempty_nul_free_and_target_bounded() {
        assert!(matches!(
            KeyringClient::new(String::new()).entry("", ""),
            Err(keyring::Error::Invalid(name, _)) if name == "service"
        ));
        assert!(matches!(
            KeyringClient::new("bad\0service".to_string()).entry("", ""),
            Err(keyring::Error::Invalid(name, _)) if name == "service"
        ));

        let username_len = PHYSICAL_PREFIX.len() + compound_key("", "").len() * 2;
        assert!(
            KeyringClient::new("service".to_string())
                .entry("", "")
                .is_ok()
        );
        let service = "s".repeat(MAX_TARGET_BYTES - username_len + 1);
        assert!(matches!(
            KeyringClient::new(service).entry("", ""),
            Err(keyring::Error::TooLong(name, max)) if name == "target" && max as usize == MAX_TARGET_BYTES
        ));
    }
}
