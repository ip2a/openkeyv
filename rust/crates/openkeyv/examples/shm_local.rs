//! Two-terminal demo of the shared-memory store: one process writes, another
//! reads and watches the change feed through the same backing file.
//!
//! Usage:
//!   cargo run --example shm_local --features shm -- create <path>
//!   cargo run --example shm_local --features shm -- put <path> <key> <value>
//!   cargo run --example shm_local --features shm -- get <path> <key>
//!   cargo run --example shm_local --features shm -- watch <path>
//!   cargo run --example shm_local --features shm -- open <path>
//!
//! Terminal 1: `create /tmp/demo` then `put /tmp/demo greeting hello`
//! Terminal 2: `watch /tmp/demo` (then `get /tmp/demo greeting`)

use openkeyv::change::{ChangeFeedRequest, ChangeStart};
use openkeyv::protocol::{AsyncChangeFeed, AsyncKeyValue};
use openkeyv::store::shm::{ShmCreateConfig, ShmStore};
use openkeyv::value::Value;

fn usage() -> ! {
    eprintln!(
        "usage:\n  shm_local create <path>\n  shm_local open <path>\n  \
         shm_local put <path> <key> <value>\n  shm_local get <path> <key>\n  \
         shm_local watch <path>"
    );
    std::process::exit(2);
}

fn main() {
    #[cfg(not(all(feature = "shm", unix)))]
    compile_error!("the shm_local example needs a unix platform with the shm feature");

    let args: Vec<String> = std::env::args().skip(1).collect();
    let command = args.first().map(String::as_str).unwrap_or_else(|| usage());

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime must build");

    match command {
        "create" => {
            let path = args.get(1).unwrap_or_else(|| usage());
            let store = ShmStore::create(ShmCreateConfig::new(path)).expect("create failed");
            println!(
                "created {} (store id {:?})",
                store.path().display(),
                store.store_id()
            );
        }
        "open" => {
            let path = args.get(1).unwrap_or_else(|| usage());
            let store = ShmStore::open(path).expect("open failed");
            println!(
                "attached to {} (store id {:?})",
                store.path().display(),
                store.store_id()
            );
        }
        "put" => {
            let path = args.get(1).unwrap_or_else(|| usage());
            let key = args.get(2).unwrap_or_else(|| usage());
            let value = args.get(3).unwrap_or_else(|| usage());
            let store = ShmStore::open(path).expect("open failed");
            rt.block_on(store.put(key, Value::utf8(value), None, None))
                .expect("put failed");
            println!("put {key}");
        }
        "get" => {
            let path = args.get(1).unwrap_or_else(|| usage());
            let key = args.get(2).unwrap_or_else(|| usage());
            let store = ShmStore::open(path).expect("open failed");
            match rt.block_on(store.get(key, None)).expect("get failed") {
                Some(value) => println!("{key} = {:?}", value),
                None => println!("{key} = <absent>"),
            }
        }
        "watch" => {
            let path = args.get(1).unwrap_or_else(|| usage());
            let store = ShmStore::open(path).expect("open failed");
            let mut stream = rt
                .block_on(store.subscribe(ChangeFeedRequest {
                    start: ChangeStart::Beginning,
                    filter: Default::default(),
                }))
                .expect("subscribe failed");
            println!("watching {} — Ctrl-C to stop", store.path().display());
            rt.block_on(async {
                loop {
                    match stream.recv().await {
                        Ok(Some(event)) => println!(
                            "rev {} {} {} {}",
                            event.revision,
                            event.collection,
                            format!("{:?}", event.operation).to_lowercase(),
                            event.key
                        ),
                        Ok(None) => {
                            println!("store destroyed or reset — stream ended");
                            return;
                        }
                        Err(error) => {
                            eprintln!("stream error: {error}");
                            return;
                        }
                    }
                }
            });
        }
        _ => usage(),
    }
}
