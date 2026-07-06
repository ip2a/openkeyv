use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;

/// Retry an async operation with exponential backoff.
///
/// # Arguments
/// * `operation` - The async operation to retry.
/// * `max_attempts` - Maximum number of attempts (including the first).
/// * `base_delay_ms` - Initial delay between retries in milliseconds.
/// * `max_delay_ms` - Maximum delay between retries in milliseconds.
/// * `retry_if` - A predicate that returns true if the error should trigger a retry.
pub async fn retry_operation<F, Fut, T, E>(
    mut operation: F,
    max_attempts: usize,
    base_delay_ms: u64,
    max_delay_ms: u64,
    retry_if: impl Fn(&E) -> bool,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut attempt = 1;
    loop {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(err) if attempt >= max_attempts || !retry_if(&err) => return Err(err),
            Err(_) => {
                let delay = base_delay_ms * 2_u64.pow((attempt - 1) as u32);
                let delay = delay.min(max_delay_ms);
                sleep(Duration::from_millis(delay)).await;
                attempt += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn test_retry_success_on_second_attempt() {
        let counter = AtomicUsize::new(0);
        let result = retry_operation(
            || async {
                let count = counter.fetch_add(1, Ordering::SeqCst);
                if count == 0 {
                    Err("fail")
                } else {
                    Ok("success")
                }
            },
            3,
            10,
            100,
            |_e| true,
        )
        .await;

        assert_eq!(result, Ok("success"));
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_retry_gives_up() {
        let counter = AtomicUsize::new(0);
        let result: Result<&str, &str> = retry_operation(
            || async {
                counter.fetch_add(1, Ordering::SeqCst);
                Err("always fails")
            },
            2,
            1,
            10,
            |_e| true,
        )
        .await;

        assert_eq!(result, Err("always fails"));
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }
}
