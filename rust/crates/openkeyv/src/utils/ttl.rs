use chrono::{DateTime, TimeDelta, Utc};

/// Returns the current UTC time.
pub fn now() -> DateTime<Utc> {
    Utc::now()
}

/// Prepare timestamps for a new entry.
/// Returns `(created_at, ttl_seconds, expires_at)`.
pub fn prepare_entry_timestamps(
    ttl: Option<f64>,
) -> (DateTime<Utc>, Option<f64>, Option<DateTime<Utc>>) {
    let created_at = now();
    match ttl {
        Some(seconds) => {
            let expires_at = Some(created_at + TimeDelta::milliseconds((seconds * 1000.0) as i64));
            (created_at, Some(seconds), expires_at)
        }
        None => (created_at, None, None),
    }
}

/// Seconds from now until the given datetime.
pub fn seconds_to(datetime: DateTime<Utc>) -> f64 {
    let now = Utc::now();
    if datetime <= now {
        0.0
    } else {
        (datetime - now).num_milliseconds() as f64 / 1000.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prepare_entry_timestamps_no_ttl() {
        let (created, ttl, expires) = prepare_entry_timestamps(None);
        assert!(ttl.is_none());
        assert!(expires.is_none());
        let now = Utc::now();
        assert!((now - created).num_seconds() < 1);
    }

    #[test]
    fn test_prepare_entry_timestamps_with_ttl() {
        let (created, ttl, expires) = prepare_entry_timestamps(Some(10.0));
        assert_eq!(ttl, Some(10.0));
        assert!(expires.is_some());
        let diff = expires.unwrap() - created;
        assert!((diff.num_milliseconds() - 10_000).abs() < 10);
    }
}
