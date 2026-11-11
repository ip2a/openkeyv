//! Cross-process change-feed wakeup via named FIFOs.
//!
//! Every subscribing process registers one header [`NotifySlot`] and holds the
//! read end of the FIFO `<store>.notify-<slot>`; the writer opens the write
//! end per wakeup and writes one byte. `poll()` with a timeout is the only
//! portable timed blocking wait for a named cross-process primitive (macOS has
//! no `sem_timedwait`), so the same transport serves every unix target.
//!
//! Wakeup is a latency optimization ONLY. Correctness never depends on it:
//! every stream re-reads the ring after every bounded `poll`, so a lost
//! wakeup costs 50 ms, never data. Registration failures are loud errors — a
//! subscriber that cannot register is an error, never a silent degraded
//! poller.

use super::layout::{NOTIFY_SLOTS, SharedMap};
use crate::error::{Error, Result};use std::ffi::CString;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

/// How long a stream blocks on the FIFO before re-polling the ring.
pub(crate) const IDLE_WAIT: Duration = Duration::from_millis(50);

fn notify_path(store: &Path, slot: usize) -> PathBuf {
    let mut name = store.as_os_str().to_owned();
    name.push(format!(".notify-{slot}"));
    PathBuf::from(name)
}

/// Read end of the registration's FIFO, opened non-blocking so the open
/// succeeds even before any writer exists. Polling uses the raw fd; the
/// `File` closes the descriptor when the last `Arc` drops — an in-flight
/// wait task (which holds a clone) can never race the close.
struct WakeupPipe {
    file: File,
}

// SAFETY: the descriptor is process-global; the kernel keeps the pipe object
// alive until the last reference closes.
unsafe impl Send for WakeupPipe {}
unsafe impl Sync for WakeupPipe {}

impl WakeupPipe {
    fn open(store: &Path, slot: usize) -> Result<Self> {
        let path = notify_path(store, slot);
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(&path)
            .map_err(|error| Error::StoreConnection {
                message: format!("cannot open notify pipe {}: {error}", path.display()),
            })?;
        Ok(Self { file })
    }

    /// Drain bytes left by a previous owner so the first wait actually blocks.
    fn drain(&self) {
        let mut byte = [0u8; 1];
        loop {
            // SAFETY: fd is live; reads are non-blocking so this never parks.
            let read = unsafe { libc::read(self.file.as_raw_fd(), byte.as_mut_ptr().cast(), 1) };
            if read != 1 {
                break; // EAGAIN (drained) or an error; both end the drain.
            }
        }
    }

    /// Blocking wait with a deadline. Runs on a blocking thread, never on an
    /// async runtime worker.
    fn poll_wait(&self, timeout: Duration) -> bool {
        let mut poll_fd = libc::pollfd {
            fd: self.file.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: fd is live for the whole call; one-element array.
        unsafe { libc::poll(&mut poll_fd, 1, timeout.as_millis() as libc::c_int) > 0 }
    }
}

/// RAII registration of one wakeup slot for a single stream. Registered on
/// the stream's first idle wait, unregistered on drop.
///
/// The raw map pointer is safe because the stream owns both this registration
/// and an `Arc<ShmClient>` — and declares the registration BEFORE the client
/// field, so registration drop runs while the map is still alive.
pub(crate) struct NotifyRegistration {
    map: *const SharedMap,
    pipe: Arc<WakeupPipe>,
    slot: usize,
    generation: u64,
}

// SAFETY: the shared map outlives the registration (see struct docs); all
// header writes go through atomics.
unsafe impl Send for NotifyRegistration {}

impl NotifyRegistration {
    /// Claim a free notify slot and open its FIFO's read end. Loud on every
    /// failure path.
    pub(crate) fn register(client: &super::client::ShmClient) -> Result<Self> {
        let header = client.map.header();
        let pid = u64::from(std::process::id());

        // 16 slots serve 16 concurrent streams across all processes; mcpstore
        // runs a handful per process. Exhaustion is reported, not absorbed.
        let mut chosen: Option<usize> = None;
        for slot in 0..NOTIFY_SLOTS {
            let notify = &header.notify[slot];
            if notify.pid.load(Ordering::Acquire) == 0
                && notify
                    .pid
                    .compare_exchange(0, pid, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                chosen = Some(slot);
                break;
            }
        }
        let Some(slot) = chosen else {
            return Err(Error::StoreConnection {
                message: format!(
                    "no free notify slot ({NOTIFY_SLOTS} change streams max across \
                     processes); reduce concurrent subscribers"
                ),
            });
        };

        let pipe = match WakeupPipe::open(client.path(), slot) {
            Ok(pipe) => Arc::new(pipe),
            Err(error) => {
                header.notify[slot].pid.store(0, Ordering::Release);
                return Err(error);
            }
        };
        pipe.drain();
        // Bump before recording: the drop-CAS below must observe exactly this
        // value to know the slot still belongs to us (a reset zeroes the slot
        // and a new registration bumps again — either makes the CAS fail).
        let generation = header.notify[slot].generation.fetch_add(1, Ordering::AcqRel) + 1;
        Ok(Self {
            map: &client.map,
            pipe,
            slot,
            generation,
        })
    }

    /// Block until a wakeup byte arrives or [`IDLE_WAIT`] elapses, on a
    /// blocking thread. Returns on either outcome — the caller polls the ring
    /// regardless.
    pub(crate) async fn wait(&mut self) -> Result<()> {
        let pipe = Arc::clone(&self.pipe);
        tokio::task::spawn_blocking(move || pipe.poll_wait(IDLE_WAIT))
            .await
            .map_err(|error| Error::StoreConnection {
                message: format!("notify wait task failed: {error}"),
            })?;
        Ok(())
    }
}

impl Drop for NotifyRegistration {
    fn drop(&mut self) {
        // SAFETY: the stream declares this field before the client Arc, so
        // the map is alive for the whole registration lifetime.
        let map = unsafe { &*self.map };
        let notify = &map.header().notify[self.slot];
        // Clear the pid only if the slot is still ours. A reset (or a fresh
        // registration after one) has already re-purposed it; touching that
        // registration would silence a live waiter until its 50 ms backstop.
        if notify
            .generation
            .compare_exchange(
                self.generation,
                self.generation + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            notify.pid.store(0, Ordering::Release);
        }
        // The descriptor closes when the last Arc drops — possibly deferred
        // past this drop by an in-flight wait task, by design.
    }
}

/// Writer-side wakeup: write one byte to every registered slot's FIFO. Called
/// once per writer-lock section (see `ShmStore::mutate`) and after recovery's
/// re-append. Subscribers drain the whole ring on wake, so a batch costs one
/// post sweep, not one per event.
///
/// Signaling errors are swallowed deliberately: a lost post costs one idle
/// wait cycle (50 ms), never data. ENXIO means the subscriber is gone; EAGAIN
/// means its pipe buffer is full and its next poll re-checks the ring anyway.
pub(crate) fn notify_all(client: &super::client::ShmClient) {
    let header = client.map.header();
    for slot in 0..NOTIFY_SLOTS {
        if header.notify[slot].pid.load(Ordering::Acquire) == 0 {
            continue;
        }
        let path = notify_path(client.path(), slot);
        // Open non-blocking: ENXIO when no reader holds the pipe (the slot's
        // owner died between registering and now) — posting is pointless.
        let writer = OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(&path);
        if let Ok(mut writer) = writer {
            use std::io::Write;
            let _ = writer.write_all(&[1]);
        }
    }
}

/// Recreate the 16 FIFOs for a store. Runs during reset under the exclusive
/// attach lock: every previous registration dies with the reset (generations
/// zero below makes each live Drop-CAS fail), so unlink-and-remake gives
/// fresh registrations clean pipes instead of stale bytes.
pub(crate) fn reset_pipes(store: &Path) -> Result<()> {
    for slot in 0..NOTIFY_SLOTS {
        let path = notify_path(store, slot);
        let _ = std::fs::remove_file(&path); // missing on first create
        let c_path = CString::new(
            path.to_str()
                .ok_or_else(|| Error::StoreSetup {
                    message: format!("store path {} is not valid UTF-8", path.display()),
                })?
                .to_owned(),
        )
        .map_err(|error| Error::StoreSetup {
            message: format!("notify path for {} is invalid: {error}", path.display()),
        })?;
        // SAFETY: c_path is NUL-terminated.
        if unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) } != 0 {
            let error = std::io::Error::last_os_error();
            return Err(Error::StoreSetup {
                message: format!("cannot create notify pipe {}: {error}", path.display()),
            });
        }
    }
    Ok(())
}
