//! Advisory locks with namespacing and retry policies.
//!
//! Backends:
//! - **`PostgreSQL`** (`pg`): session-level `pg_try_advisory_lock` on a pinned pool connection
//! - **`MySQL`** (`mysql`): session-level `GET_LOCK(name, 0)` on a pinned pool connection
//! - **`SQLite` / fallback**: file marker locks under the OS cache dir
//!
//! ## Public API semantics
//!
//! - [`LockManager::lock`] — exactly one non-blocking acquisition attempt.
//!   Contention → [`DbLockError::AlreadyHeld`]. On PG/MySQL, SQL/protocol errors map to
//!   `DbLockError::Database`.
//! - [`LockManager::try_lock`] — owns retry/backoff; returns `Ok(None)` when limits are exhausted.
//!   `max_wait` bounds retry scheduling and time between **completed** acquisition attempts. It
//!   does **not** currently impose a cancellation-safe timeout on pool acquisition or an
//!   in-flight database query.
//! - Native backends never use blocking `pg_advisory_lock` or `GET_LOCK` with a positive timeout.
//!
//! ## Single-session model (native backends)
//!
//! Each PG/MySQL [`LockManager`] owns a **dedicated pool sized `max=1`/`min=1`** with idle and
//! lifetime reaping disabled. One database session therefore holds **all** advisory keys for the
//! process. A held lock does **not** pin the connection: `pg_try_advisory_lock` /
//! `GET_LOCK(_, 0)` return immediately and the session merely remembers the key, so the single
//! connection is free to service the next lock/unlock op. Guards carry only a cheap
//! `Arc<..Session>` handle plus the key — never a `PoolConnection`.
//!
//! ### Reconnect accounting (generation epoch)
//!
//! If the single connection dies, sqlx re-establishes a new backend and **all** advisory locks
//! held by the old session are gone server-side. To avoid acting on stale "I hold key X"
//! beliefs, the pool's `after_connect` callback bumps a shared [`AtomicU64`] generation on every
//! newly established physical connection (initial + each reconnect). Each [`DbLockGuard`] records
//! the generation observed right after its acquire; on release, if the current generation no
//! longer matches, the lock was already released by the disconnect and the unlock SQL is skipped.
//!
//! ### Keepalive + unlock draining
//!
//! A background maintenance task keeps the session warm with a periodic `SELECT 1` (defeating
//! proxy/PG idle-killers and bounding dead-session detection latency) and drains a pending-unlock
//! queue. [`DbLockGuard`] `Drop` performs a non-blocking, runtime-free send of `(key, generation)`
//! into that queue instead of running unlock SQL inline; the task performs the generation-checked
//! `pg_advisory_unlock` / `RELEASE_LOCK`. This makes `Drop` panic-free even after the runtime has
//! shut down and removes the previous `close_on_drop` / `mem::forget` connection-fate machinery.
//! The task stops when the last [`LockManager`] / `DbHandle` clone drops the shared `Arc<..Session>`.
//!
//! Prefer awaiting [`DbLockGuard::release`] on the normal path for deterministic unlock; `Drop`
//! is best-effort via the queue.
//!
//! ### In-process exclusivity (held-key registry)
//!
//! Session advisory locks are **re-entrant**: `pg_try_advisory_lock` / `GET_LOCK` grant a key the
//! same session already holds and just bump the server's hold count. Since one session now serves
//! the whole process, the database can no longer express in-process exclusivity on its own, so each
//! session keeps a registry of claimed keys (see `HeldKeys`) that is consulted before the SQL runs.
//!
//! A claim is released only where the server is **known** to no longer hold the key: a confirmed
//! unlock, a lost session, or a generation bump. When an unlock fails on a live session the key
//! stays claimed, because PG still holds it and re-acquiring would raise the hold count to two —
//! a leak no release could balance. Claims are recorded per generation, so such a claim is not
//! permanent: the next reconnect drops every server-side lock and frees the key.
//!
//! One consequence for native backends: after `Drop`, re-acquiring the same key in the same process
//! fails until the maintenance task has drained that unlock. Await [`DbLockGuard::release`] when the
//! key is re-acquired immediately.
//!
//! **Connection pooling caveat:** session advisory locks require a real session. A
//! transaction-pooling proxy (e.g. `PgBouncer` in `transaction` mode) breaks them entirely — use
//! session pooling or a direct connection for the lock pool.
//!
//! ### File-marker backend limits
//!
//! The SQLite/fallback backend uses an exclusive create of a marker file (not `fs2` kernel
//! locks). Process termination or cancellation during filesystem acquisition may leave a stale
//! marker — the file backend does not provide kernel-owned lock cleanup.
//!
//! After `open(...).await` returns successfully, ownership is transferred to the guard without
//! another await point. Cancellation while the filesystem open itself is in flight may still
//! leave a marker, depending on Tokio blocking-filesystem cancellation behavior.
//!
//! Implicit [`DbLockGuard`] Drop removes the marker **synchronously** (file cleanup does not
//! depend on a spawned Tokio task). Explicit [`DbLockGuard::release`] for the file backend is
//! likewise synchronous after taking ownership.
//!
//! #### Recovering a stale marker
//!
//! There is **no automatic reclaim**: a marker left by `SIGKILL`, a power loss, or cancellation
//! mid-`open` keeps the key un-acquirable until it is deleted. This is deliberate — the two
//! automatic policies on offer are both unsound here. A TTL cannot be chosen safely because the
//! library does not know how long a legitimate holder may hold a key, and PID liveness checks
//! misfire once PIDs are reused; either one can reclaim a *live* lock, which is a worse failure
//! than a stuck one. The file backend is only ever selected for `SQLite`, so the participants are
//! processes on one machine sharing one database file — a human (or the service's own start-up
//! script) can see the whole picture that a TTL cannot.
//!
//! The operational fallback is to remove the marker while no holder is running:
//!
//! ```text
//! <cache_dir>/cf-gears/locks/{database_scope:016x}/{xxh3_64(canonical_lock_input):016x}.lock
//! ```
//!
//! `<cache_dir>` is `dirs::cache_dir()` (falling back to the temp dir), e.g.
//! `%LOCALAPPDATA%` on Windows and `~/.cache` on Linux. Deleting the whole
//! `cf-gears/locks/{database_scope:016x}` directory clears every key for one database and is the
//! usual recovery step; deleting `cf-gears/locks` clears all of them. Doing this while a holder
//! is alive drops that holder's mutual exclusion without telling it, so scope the deletion to a
//! window where the participating processes are down.
//!
//! If crash recovery ever needs to be automatic, the sound mechanism is kernel-owned locks
//! (`flock` / `LockFileEx`), which the OS releases on process death — not a TTL.
//!
//! ## Semver note
//!
//! This module introduces a **breaking** public API change versus prior `0.8.x` releases:
//! [`DbLockGuard::release`] now returns [`Result`], and `DbLockError` (including
//! `Database(sqlx::Error)` when `pg`/`mysql` features are enabled) is publicly re-exported.
//! Additionally, the public `lock_keepalive: Option<Duration>` field was added to both
//! [`crate::ConnectOpts`] and [`crate::config::DbConnConfig`]; neither type is `#[non_exhaustive]`,
//! so this is source-breaking for callers that build them with an exhaustive struct literal
//! (config deserialization stays compatible via `#[serde(default)]`). Bump the crate major/minor
//! appropriately when publishing (intended: `0.9.0`). Local path/patch validation against consumers
//! pinned to `0.8.4` may keep the package version at `0.8.4` until a coordinated Gears release.
//!
//! ## Stable lock namespace
//!
//! ```text
//! canonical_lock_input =
//!   "cf-gears-toolkit-db:v2:{database_scope:016x}:g{gear_utf8_len}:{gear}:k{key_utf8_len}:{key}"
//! ```
//!
//! Length prefixes are UTF-8 byte lengths so `gear`/`key` values that contain `:` cannot collide
//! (e.g. `("a:b","c")` ≠ `("a","b:c")`). The `v2` prefix replaces the ambiguous `v1` colon-joined
//! encoding.
//!
//! `database_scope` is a cross-pod stable fingerprint of host + port + database name (no password,
//! no pod/PID, no implicit `PostgreSQL` `search_path`). File lock paths are derived from the same
//! scope + canonical input (raw DSN does not independently participate in the final path).

#![cfg_attr(
    not(any(feature = "pg", feature = "mysql", feature = "sqlite")),
    allow(unused_imports, unused_variables, dead_code, unreachable_code)
)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use thiserror::Error;
use tokio::fs::File;
use xxhash_rust::xxh3::xxh3_64;

#[cfg(any(feature = "pg", feature = "mysql"))]
use std::sync::Arc;

/// Default keepalive ping interval for the dedicated lock session.
///
/// Matches the reference production value; overridable per connection via config.
pub const DEFAULT_LOCK_KEEPALIVE: Duration = Duration::from_secs(5);

/// Consecutive keepalive-ping failures before the maintenance task escalates from `debug` to
/// `warn`. The lock session holds every advisory key for the process, so sustained ping failure
/// signals the whole lock subsystem is degraded and must be visible at normal log levels.
#[cfg(any(feature = "pg", feature = "mysql"))]
const KEEPALIVE_WARN_THRESHOLD: u32 = 3;

/// Reject a zero keepalive interval before it reaches `tokio::time::interval` (which panics on
/// zero). The public default is resolved once by the caller via `unwrap_or(DEFAULT_LOCK_KEEPALIVE)`;
/// a zero reaching here is an explicit misconfiguration, not a request for the default.
#[cfg(any(feature = "pg", feature = "mysql"))]
fn validate_keepalive(keepalive: Duration) -> Result<Duration, DbLockError> {
    if keepalive.is_zero() {
        return Err(DbLockError::InvalidConfig {
            message: "lock keepalive interval must be greater than zero".to_owned(),
        });
    }
    Ok(keepalive)
}

// --------------------------- Config ------------------------------------------

/// Configuration for lock acquisition attempts.
///
/// The backoff/jitter fields use the workspace-wide retry vocabulary and map
/// straight onto [`tokio_retry::strategy::ExponentialBackoff`]. `max_wait` and
/// `max_retries` are lock-specific termination knobs layered on top.
#[derive(Debug, Clone)]
pub struct LockConfig {
    /// Bounds retry scheduling and time between completed acquisition attempts (`None` = unlimited).
    ///
    /// Does **not** cancel an in-flight `pool.acquire()` or database query; those are limited by
    /// the sqlx/pool/database timeouts instead.
    pub max_wait: Option<Duration>,
    /// Maximum retries after the first attempt (`None` = unlimited, bounded
    /// only by `max_wait`).
    pub max_retries: Option<u32>,
    /// [`ExponentialBackoff`](tokio_retry::strategy::ExponentialBackoff) base —
    /// the growth ratio between retry delays.
    pub backoff_base_ms: u64,
    /// Multiplicative factor applied to every retry delay.
    pub backoff_factor: u64,
    /// Upper bound on any single retry delay (cap for exponential backoff).
    pub max_backoff: Duration,
    /// Apply full jitter to each retry delay.
    pub jitter: bool,
}

impl Default for LockConfig {
    fn default() -> Self {
        Self {
            max_wait: Some(Duration::from_secs(30)),
            max_retries: None,
            // base 2 × factor 25 → 50ms, 100ms, 200ms, … (doubling), capped at
            // `max_backoff`.
            backoff_base_ms: 2,
            backoff_factor: 25,
            max_backoff: Duration::from_secs(5),
            jitter: true,
        }
    }
}

impl LockConfig {
    /// Validate configuration before the first acquisition attempt.
    ///
    /// # Errors
    /// Returns [`DbLockError::InvalidConfig`] when values would panic or produce nonsense backoff.
    pub fn validate(&self) -> Result<(), DbLockError> {
        // Each of these collapses every `ExponentialBackoff` delay to zero, turning the retry
        // loop into a busy spin until `max_wait` expires. `tokio-retry` does not reject them,
        // so catch them before the first attempt.
        if self.backoff_base_ms == 0 {
            return Err(DbLockError::InvalidConfig {
                message: "backoff_base_ms must be greater than zero".to_owned(),
            });
        }
        if self.backoff_factor == 0 {
            return Err(DbLockError::InvalidConfig {
                message: "backoff_factor must be greater than zero".to_owned(),
            });
        }
        if self.max_backoff.is_zero() {
            return Err(DbLockError::InvalidConfig {
                message: "max_backoff must be greater than zero".to_owned(),
            });
        }
        // `max_retries: Some(0)` is valid: one attempt, no retries.
        Ok(())
    }
}

/// Outcome of a single `try_acquire_once` used to drive the retry loop:
/// `Pending` is the retryable "held elsewhere, try again" signal.
enum TryLockError {
    Pending,
    Fatal(DbLockError),
}

// --------------------------- Scope / key helpers -----------------------------

const CANONICAL_PREFIX: &str = "cf-gears-toolkit-db:v2";

/// Canonical lock input shared by all backends.
///
/// Uses UTF-8 length-prefixed `gear`/`key` fields so values containing `:` cannot collide.
#[must_use]
pub(crate) fn canonical_lock_input(database_scope: u64, gear: &str, key: &str) -> String {
    format!(
        "{CANONICAL_PREFIX}:{database_scope:016x}:g{}:{gear}:k{}:{key}",
        gear.len(),
        key.len(),
    )
}

/// Build a cross-pod-stable database scope fingerprint.
///
/// Identity must be identical for all peers coordinating on the same logical database.
/// Do **not** include pod hostname, PID, `instance_id`, passwords, or `PostgreSQL` `search_path`.
#[must_use]
pub(crate) fn database_scope_from_identity(identity: &str) -> u64 {
    xxh3_64(identity.as_bytes())
}

/// Server-style identity: `scheme://host:port/database` (no credentials).
///
/// Normalizes the two ways equivalent DSNs differ textually so peers still land on one scope:
/// hostnames are case-insensitive per DNS, and a trailing `/` on the database path is not part of
/// the name. Normalizing here rather than in [`database_scope_from_dsn`] keeps the DSN-parsed and
/// typed-options entry points (which pass `opts.get_host()` straight through) in agreement.
///
/// TODO(application-namespace): optionally append an explicit application namespace from
/// `DbOptions` when present, so independent apps sharing one database do not collide on
/// advisory locks. Until then, peers on the same host/port/database share one lock space.
#[must_use]
pub(crate) fn server_database_identity(
    scheme: &str,
    host: &str,
    port: u16,
    database: &str,
) -> String {
    let host = host.to_ascii_lowercase();
    let database = database.trim_end_matches('/');
    format!("{scheme}://{host}:{port}/{database}")
}

/// Parse a DSN into a non-secret scope identity string when possible.
#[must_use]
pub(crate) fn database_scope_from_dsn(dsn: &str) -> u64 {
    let trimmed = dsn.trim_start();
    if let Ok(url) = url::Url::parse(trimmed) {
        let scheme = url.scheme();
        if scheme == "postgres" || scheme == "postgresql" || scheme == "mysql" {
            let host = url.host_str().unwrap_or("");
            let port =
                url.port_or_known_default()
                    .unwrap_or(if scheme == "mysql" { 3306 } else { 5432 });
            let database = url.path().trim_start_matches('/');
            // Normalize postgres/postgresql so both DSNs share one scope.
            let normalized_scheme = if scheme == "postgresql" {
                "postgres"
            } else {
                scheme
            };
            return database_scope_from_identity(&server_database_identity(
                normalized_scheme,
                host,
                port,
                database,
            ));
        }
        #[cfg(feature = "sqlite")]
        if scheme == "sqlite" {
            return database_scope_from_identity(&crate::sqlite::path::sqlite_scope_identity(
                trimmed,
            ));
        }
    }
    #[cfg(feature = "sqlite")]
    if trimmed.starts_with("sqlite:") {
        return database_scope_from_identity(&crate::sqlite::path::sqlite_scope_identity(trimmed));
    }
    // Unrecognized: fingerprint the DSN string (no password stripping possible).
    database_scope_from_identity(&format!("dsn:{trimmed}"))
}

/// `PostgreSQL` advisory key: XXH3-64 bit pattern as signed `i64`.
#[must_use]
#[cfg_attr(not(feature = "pg"), allow(dead_code))]
pub(crate) fn stable_lock_key(canonical: &str) -> i64 {
    xxh3_64(canonical.as_bytes()).cast_signed()
}

/// `MySQL` `GET_LOCK` name: `cf:` + lowercase zero-padded 16-digit hex XXH3-64.
#[must_use]
#[cfg_attr(not(feature = "mysql"), allow(dead_code))]
pub(crate) fn mysql_lock_name(canonical: &str) -> String {
    format!("cf:{:016x}", xxh3_64(canonical.as_bytes()))
}

#[must_use]
#[cfg_attr(not(any(feature = "pg", feature = "mysql")), allow(dead_code))]
fn key_fingerprint(canonical: &str) -> String {
    format!("{:016x}", xxh3_64(canonical.as_bytes()))
}

static NEXT_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);

fn new_instance_id() -> u64 {
    let counter = NEXT_INSTANCE_ID.fetch_add(1, Ordering::Relaxed);
    let seed = format!(
        "{}:{}:{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default(),
        counter,
    );
    xxh3_64(seed.as_bytes())
}

// --------------------------- Lock session ------------------------------------

/// Keys currently claimed by a live guard on one shared native session.
///
/// Both `pg_try_advisory_lock` and `GET_LOCK` are **session-scoped and re-entrant**: a second
/// acquire of a key the *same session* already holds succeeds and merely bumps the server's hold
/// count. Because every guard of a [`LockManager`] shares one session, the database alone can no
/// longer express in-process exclusivity — without this registry a second `lock()` for a held key
/// would hand out a second guard instead of [`DbLockError::AlreadyHeld`], and the first release
/// would only decrement the count, leaving the key held server-side with no owner.
///
/// The claim is the map insert: it happens under the mutex with no `await` in between, so two tasks
/// racing for the same key cannot both reach the SQL. This restores the exclusive semantics the
/// file backend has always had.
///
/// ### Why claims outlive their guard
///
/// A claim is dropped only where the server-side hold is *known* to be gone. If the unlock cannot
/// be confirmed while the session is still alive — the SQL errored, or `RELEASE_LOCK` did not
/// report ownership — the database still holds the key, so the key stays claimed. Releasing it
/// early would let this process re-acquire re-entrantly and bump the server's hold count to two,
/// turning a recoverable failed unlock into a hold no release can ever balance.
///
/// That is why each claim records the session generation it was made under. A retained claim would
/// otherwise be permanent; instead it is scoped to one physical connection. When the session
/// reconnects every lock it held is gone server-side, so a claim from an older generation is stale
/// and the next claimant takes it over.
#[cfg(any(feature = "pg", feature = "mysql"))]
#[derive(Debug, Default)]
struct HeldKeys<K: Eq + std::hash::Hash>(std::sync::Mutex<std::collections::HashMap<K, u64>>);

#[cfg(any(feature = "pg", feature = "mysql"))]
impl<K: Eq + std::hash::Hash> HeldKeys<K> {
    fn new() -> Self {
        Self(std::sync::Mutex::new(std::collections::HashMap::new()))
    }

    /// Claim `key` under `generation`.
    ///
    /// Returns `false` only when a claim made under the *current* generation is still standing. A
    /// claim from an older generation belongs to a connection that has since died — its locks are
    /// gone server-side — so it is stale and gets taken over.
    fn claim(&self, key: K, generation: u64) -> bool {
        match self.guard().entry(key) {
            std::collections::hash_map::Entry::Occupied(mut held) => {
                if *held.get() == generation {
                    false
                } else {
                    held.insert(generation);
                    true
                }
            }
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(generation);
                true
            }
        }
    }

    /// Give up a claim; called only where the server is known to no longer hold the key.
    ///
    /// Borrow-generic so the `MySQL` side can unclaim from a `&str` without allocating a `String`.
    fn unclaim<Q>(&self, key: &Q)
    where
        K: std::borrow::Borrow<Q>,
        Q: Eq + std::hash::Hash + ?Sized,
    {
        self.guard().remove(key);
    }

    /// A poisoned registry only means some other caller panicked; the map itself stays consistent,
    /// so recover the data rather than propagating the panic into every later lock attempt.
    fn guard(&self) -> std::sync::MutexGuard<'_, std::collections::HashMap<K, u64>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Classify an `sqlx` error as "the session is gone" vs a genuine database error.
///
/// A dropped session cannot still hold an advisory lock, so on a connection-lost error the caller
/// treats the key as already released. Everything else is a real error the server answered with.
///
/// Deliberately **not** connection-lost: [`sqlx::Error::PoolTimedOut`]. With `max_connections(1)`
/// a busy lock session makes `acquire()` time out routinely while the session is alive and still
/// holding the key. Treating that as "released" would skip the unlock and report success, leaking
/// the key server-side until the session closes.
#[cfg(any(feature = "pg", feature = "mysql"))]
fn is_connection_lost(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Io(_) | sqlx::Error::Tls(_) | sqlx::Error::PoolClosed => true,
        // PostgreSQL: connection_exception class `08*`; admin shutdown / crash `57P01/02/03`.
        sqlx::Error::Database(db) => db.code().as_deref().is_some_and(|code| {
            code.starts_with("08") || matches!(code, "57P01" | "57P02" | "57P03")
        }),
        _ => false,
    }
}

// --- PostgreSQL session ---

/// Queued PG unlock request produced by [`DbLockGuard`] `Drop`.
#[cfg(feature = "pg")]
#[derive(Debug)]
struct PgPendingUnlock {
    lock_key: i64,
    generation: u64,
    key_fingerprint: String,
}

/// Dedicated single-connection PG lock session shared by all guards of one [`LockManager`].
#[cfg(feature = "pg")]
#[derive(Debug)]
struct PgSession {
    pool: sqlx::PgPool,
    generation: Arc<AtomicU64>,
    unlock_tx: tokio::sync::mpsc::UnboundedSender<PgPendingUnlock>,
    shutdown: Arc<tokio::sync::Notify>,
    /// Keys claimed on this session — see [`HeldKeys`]. Shared with the maintenance task, which
    /// unclaims a key once its queued unlock is confirmed.
    held_keys: Arc<HeldKeys<i64>>,
    // Only ever accessed from `Drop` under `&mut self`, so no shared-mutability wrapper is needed.
    task: Option<tokio::task::JoinHandle<()>>,
}

#[cfg(feature = "pg")]
impl Drop for PgSession {
    fn drop(&mut self) {
        // Last shared clone is going away: signal the maintenance task to shut down. It drains any
        // remaining queued unlocks, then exits and drops its pool clone; dropping our own `pool`
        // clone afterwards closes the physical session, which releases every advisory lock
        // server-side. We deliberately do not `abort()` the task — a hard abort would cut the
        // graceful drain short, and connection close already guarantees server-side cleanup.
        self.shutdown.notify_one();
        drop(self.task.take());
    }
}

/// Build the dedicated PG lock pool options: single connection, no reaping, generation-bumping
/// `after_connect`, and a liveness ping before hand-out so a dead session forces a reconnect.
#[cfg(feature = "pg")]
fn pg_lock_pool_options(generation: Arc<AtomicU64>) -> sqlx::postgres::PgPoolOptions {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .min_connections(1)
        .idle_timeout(None)
        .max_lifetime(None)
        .test_before_acquire(true)
        .after_connect(move |_conn, _meta| {
            let generation = Arc::clone(&generation);
            Box::pin(async move {
                // Release-ordered so a subsequent Acquire-load after `acquire()` observes the bump.
                generation.fetch_add(1, Ordering::Release);
                Ok(())
            })
        })
}

/// Generation-checked drain of one queued PG unlock (best-effort; runs on the maintenance task).
///
/// Owns the in-process claim released by [`DbLockGuard`] `Drop`: it is dropped here, once the key
/// is known to be free server-side. Every early return below is a path where the lock is gone
/// (reconnect / lost session) — the two paths that leave the key still held keep the claim, so a
/// pending or failed cleanup cannot be re-acquired re-entrantly. See [`HeldKeys`].
#[cfg(feature = "pg")]
async fn drain_pg_unlock(
    pool: &sqlx::PgPool,
    generation: &AtomicU64,
    held_keys: &HeldKeys<i64>,
    pending: PgPendingUnlock,
) {
    if generation.load(Ordering::Acquire) != pending.generation {
        held_keys.unclaim(&pending.lock_key);
        return; // session reconnected → lock already gone.
    }
    let mut conn = match pool.acquire().await {
        Ok(conn) => conn,
        Err(error) if is_connection_lost(&error) => {
            held_keys.unclaim(&pending.lock_key);
            return;
        }
        Err(error) => {
            // Session is alive and still holds the key: keep the claim (scoped to this generation).
            tracing::warn!(backend = "postgres", %error, "queued advisory unlock: pool acquire failed");
            return;
        }
    };
    if generation.load(Ordering::Acquire) != pending.generation {
        held_keys.unclaim(&pending.lock_key);
        return;
    }
    match sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock($1)")
        .bind(pending.lock_key)
        .fetch_one(&mut *conn)
        .await
    {
        Ok(true) => held_keys.unclaim(&pending.lock_key),
        // `false` means the server was not holding it, so there is nothing left to protect.
        Ok(false) => {
            held_keys.unclaim(&pending.lock_key);
            tracing::debug!(
                backend = "postgres",
                key_fingerprint = %pending.key_fingerprint,
                "queued pg_advisory_unlock returned false"
            );
        }
        Err(error) if is_connection_lost(&error) => held_keys.unclaim(&pending.lock_key),
        // Unlock unconfirmed on a live session → keep the key claimed.
        Err(error) => tracing::warn!(
            backend = "postgres",
            key_fingerprint = %pending.key_fingerprint,
            %error,
            "queued pg_advisory_unlock failed; key stays claimed until this session reconnects"
        ),
    }
}

/// Spawn the PG maintenance task: keepalive ping + pending-unlock drain until shutdown.
#[cfg(feature = "pg")]
fn spawn_pg_maintenance(
    pool: sqlx::PgPool,
    generation: Arc<AtomicU64>,
    held_keys: Arc<HeldKeys<i64>>,
    mut unlock_rx: tokio::sync::mpsc::UnboundedReceiver<PgPendingUnlock>,
    shutdown: Arc<tokio::sync::Notify>,
    keepalive: Duration,
) -> tokio::task::JoinHandle<()> {
    // `keepalive` is already validated non-zero by the session constructor.
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(keepalive);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticker.tick().await; // consume the immediate first tick.
        let mut ping_failures: u32 = 0;
        loop {
            tokio::select! {
                () = shutdown.notified() => {
                    // Graceful shutdown: drain already-queued unlocks so guards dropped just
                    // before teardown still release their keys before the session connection closes.
                    while let Ok(pending) = unlock_rx.try_recv() {
                        drain_pg_unlock(&pool, &generation, &held_keys, pending).await;
                    }
                    break;
                }
                // `None` = all senders gone; keep pinging until shutdown.
                maybe = unlock_rx.recv() => if let Some(pending) = maybe {
                    drain_pg_unlock(&pool, &generation, &held_keys, pending).await;
                },
                _ = ticker.tick() => match sqlx::query("SELECT 1").execute(&pool).await {
                    Ok(_) => ping_failures = 0,
                    Err(error) => {
                        ping_failures = ping_failures.saturating_add(1);
                        if ping_failures >= KEEPALIVE_WARN_THRESHOLD {
                            tracing::warn!(
                                backend = "postgres",
                                %error,
                                consecutive_failures = ping_failures,
                                "advisory-lock keepalive ping failing repeatedly; lock session may be dead"
                            );
                        } else {
                            tracing::debug!(backend = "postgres", %error, "advisory-lock keepalive ping failed");
                        }
                    }
                }
            }
        }
    })
}

// --- MySQL session ---

/// Queued `MySQL` unlock request produced by [`DbLockGuard`] `Drop`.
#[cfg(feature = "mysql")]
#[derive(Debug)]
struct MySqlPendingUnlock {
    lock_name: String,
    generation: u64,
    key_fingerprint: String,
}

/// Dedicated single-connection `MySQL` lock session shared by all guards of one [`LockManager`].
#[cfg(feature = "mysql")]
#[derive(Debug)]
struct MySqlSession {
    pool: sqlx::MySqlPool,
    generation: Arc<AtomicU64>,
    unlock_tx: tokio::sync::mpsc::UnboundedSender<MySqlPendingUnlock>,
    shutdown: Arc<tokio::sync::Notify>,
    /// Keys claimed on this session — see [`HeldKeys`]. Shared with the maintenance task, which
    /// unclaims a key once its queued unlock is confirmed.
    held_keys: Arc<HeldKeys<String>>,
    // Only ever accessed from `Drop` under `&mut self`, so no shared-mutability wrapper is needed.
    task: Option<tokio::task::JoinHandle<()>>,
}

#[cfg(feature = "mysql")]
impl Drop for MySqlSession {
    fn drop(&mut self) {
        // Signal graceful shutdown; the task drains queued unlocks and exits. No `abort()`:
        // connection close on the final pool drop releases every advisory lock server-side.
        self.shutdown.notify_one();
        drop(self.task.take());
    }
}

#[cfg(feature = "mysql")]
fn mysql_lock_pool_options(generation: Arc<AtomicU64>) -> sqlx::mysql::MySqlPoolOptions {
    sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(1)
        .min_connections(1)
        .idle_timeout(None)
        .max_lifetime(None)
        .test_before_acquire(true)
        .after_connect(move |_conn, _meta| {
            let generation = Arc::clone(&generation);
            Box::pin(async move {
                generation.fetch_add(1, Ordering::Release);
                Ok(())
            })
        })
}

/// `MySQL` counterpart of [`drain_pg_unlock`]; same claim-ownership rule.
#[cfg(feature = "mysql")]
async fn drain_mysql_unlock(
    pool: &sqlx::MySqlPool,
    generation: &AtomicU64,
    held_keys: &HeldKeys<String>,
    pending: MySqlPendingUnlock,
) {
    if generation.load(Ordering::Acquire) != pending.generation {
        held_keys.unclaim(pending.lock_name.as_str());
        return;
    }
    let mut conn = match pool.acquire().await {
        Ok(conn) => conn,
        Err(error) if is_connection_lost(&error) => {
            held_keys.unclaim(pending.lock_name.as_str());
            return;
        }
        Err(error) => {
            // Session is alive and still holds the key: keep the claim (scoped to this generation).
            tracing::warn!(backend = "mysql", %error, "queued advisory unlock: pool acquire failed");
            return;
        }
    };
    if generation.load(Ordering::Acquire) != pending.generation {
        held_keys.unclaim(pending.lock_name.as_str());
        return;
    }
    match sqlx::query_scalar::<_, Option<i64>>("SELECT RELEASE_LOCK(?)")
        .bind(&pending.lock_name)
        .fetch_one(&mut *conn)
        .await
    {
        Ok(Some(1)) => held_keys.unclaim(pending.lock_name.as_str()),
        // `0` = held by another session, `NULL` = no such lock. Either way *this* session is not
        // the holder, so there is no stale hold of ours to guard against.
        Ok(other) => {
            held_keys.unclaim(pending.lock_name.as_str());
            tracing::debug!(
                backend = "mysql",
                key_fingerprint = %pending.key_fingerprint,
                ?other,
                "queued RELEASE_LOCK did not confirm ownership"
            );
        }
        Err(error) if is_connection_lost(&error) => held_keys.unclaim(pending.lock_name.as_str()),
        // Unlock unconfirmed on a live session → keep the key claimed.
        Err(error) => tracing::warn!(
            backend = "mysql",
            key_fingerprint = %pending.key_fingerprint,
            %error,
            "queued RELEASE_LOCK failed; key stays claimed until this session reconnects"
        ),
    }
}

#[cfg(feature = "mysql")]
fn spawn_mysql_maintenance(
    pool: sqlx::MySqlPool,
    generation: Arc<AtomicU64>,
    held_keys: Arc<HeldKeys<String>>,
    mut unlock_rx: tokio::sync::mpsc::UnboundedReceiver<MySqlPendingUnlock>,
    shutdown: Arc<tokio::sync::Notify>,
    keepalive: Duration,
) -> tokio::task::JoinHandle<()> {
    // `keepalive` is already validated non-zero by the session constructor.
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(keepalive);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticker.tick().await;
        let mut ping_failures: u32 = 0;
        loop {
            tokio::select! {
                () = shutdown.notified() => {
                    // Graceful shutdown: drain already-queued unlocks before the session closes.
                    while let Ok(pending) = unlock_rx.try_recv() {
                        drain_mysql_unlock(&pool, &generation, &held_keys, pending).await;
                    }
                    break;
                }
                // `None` = all senders gone; keep pinging until shutdown.
                maybe = unlock_rx.recv() => if let Some(pending) = maybe {
                    drain_mysql_unlock(&pool, &generation, &held_keys, pending).await;
                },
                _ = ticker.tick() => match sqlx::query("SELECT 1").execute(&pool).await {
                    Ok(_) => ping_failures = 0,
                    Err(error) => {
                        ping_failures = ping_failures.saturating_add(1);
                        if ping_failures >= KEEPALIVE_WARN_THRESHOLD {
                            tracing::warn!(
                                backend = "mysql",
                                %error,
                                consecutive_failures = ping_failures,
                                "advisory-lock keepalive ping failing repeatedly; lock session may be dead"
                            );
                        } else {
                            tracing::debug!(backend = "mysql", %error, "advisory-lock keepalive ping failed");
                        }
                    }
                }
            }
        }
    })
}

// --------------------------- Guard -------------------------------------------

#[derive(Debug)]
enum GuardInner {
    File {
        path: PathBuf,
        file: File,
    },
    #[cfg(feature = "pg")]
    Postgres {
        session: Arc<PgSession>,
        lock_key: i64,
        /// Session generation observed at acquire; a mismatch at release means reconnect-loss.
        generation: u64,
        key_fingerprint: String,
    },
    #[cfg(feature = "mysql")]
    MySql {
        session: Arc<MySqlSession>,
        lock_name: String,
        /// Session generation observed at acquire; a mismatch at release means reconnect-loss.
        generation: u64,
        key_fingerprint: String,
    },
}

/// Database lock guard. Prefer [`DbLockGuard::release`]; `Drop` is best-effort only.
#[derive(Debug)]
pub struct DbLockGuard {
    /// Human display key (`"{gear}:{key}"`) — unchanged from prior API.
    namespaced_key: String,
    inner: Option<GuardInner>,
}

impl DbLockGuard {
    /// Lock key with gear namespace (`"gear:key"`).
    #[must_use]
    pub fn key(&self) -> &str {
        &self.namespaced_key
    }

    /// Deterministically release the lock (preferred path).
    ///
    /// # Errors
    /// Returns [`DbLockError`] if unlock fails or the lock was not held.
    pub async fn release(mut self) -> Result<(), DbLockError> {
        if let Some(inner) = self.inner.take() {
            release_inner(inner).await?;
        }
        tracing::debug!(key = %self.namespaced_key, "advisory lock released");
        Ok(())
    }
}

impl Drop for DbLockGuard {
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };

        match inner {
            // File markers do not need async cleanup: remove synchronously so runtime shutdown /
            // never-polled / aborted tasks cannot leave an orphan marker.
            GuardInner::File { path, file } => {
                drop(file);
                remove_file_lock_marker(&path);
            }
            // Native backends: non-blocking, runtime-free hand-off to the maintenance task.
            #[cfg(any(feature = "pg", feature = "mysql"))]
            native => enqueue_native_unlock(native),
        }
    }
}

/// Queue a native guard's unlock on `Drop`: a non-blocking send that needs no Tokio runtime.
///
/// The session's maintenance task drains the queue and runs the generation-checked unlock SQL, and
/// it is that drain — not this function — that gives up the in-process claim, once the key is known
/// to be free. The claim is only dropped here on the paths where no drain will ever run: the lock is
/// already gone (reconnect), or the queue is closed because the session is being torn down (which
/// closes the connection and releases everything server-side anyway).
#[cfg(any(feature = "pg", feature = "mysql"))]
fn enqueue_native_unlock(inner: GuardInner) {
    match inner {
        GuardInner::File { path, file } => {
            drop(file);
            remove_file_lock_marker(&path);
        }
        #[cfg(feature = "pg")]
        GuardInner::Postgres {
            session,
            lock_key,
            generation,
            key_fingerprint,
        } => {
            if session.generation.load(Ordering::Acquire) != generation {
                session.held_keys.unclaim(&lock_key);
                return; // reconnected → lock already gone.
            }
            if session
                .unlock_tx
                .send(PgPendingUnlock {
                    lock_key,
                    generation,
                    key_fingerprint,
                })
                .is_err()
            {
                session.held_keys.unclaim(&lock_key);
                tracing::debug!(backend = "postgres", "advisory unlock queue closed on drop");
            }
        }
        #[cfg(feature = "mysql")]
        GuardInner::MySql {
            session,
            lock_name,
            generation,
            key_fingerprint,
        } => {
            if session.generation.load(Ordering::Acquire) != generation {
                session.held_keys.unclaim(lock_name.as_str());
                return;
            }
            if let Err(error) = session.unlock_tx.send(MySqlPendingUnlock {
                lock_name,
                generation,
                key_fingerprint,
            }) {
                session.held_keys.unclaim(error.0.lock_name.as_str());
                tracing::debug!(backend = "mysql", "advisory unlock queue closed on drop");
            }
        }
    }
}

/// Synchronous removal of a file-backend lock marker.
///
/// `NotFound` is treated as success (already released / cleaned up).
fn remove_file_lock_marker_result(path: &std::path::Path) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Best-effort synchronous removal used from [`DbLockGuard`] Drop.
fn remove_file_lock_marker(path: &std::path::Path) {
    if let Err(error) = remove_file_lock_marker_result(path) {
        tracing::warn!(
            %error,
            path = %path.display(),
            "failed to remove file lock marker on Drop"
        );
    }
}

// Async for native unlock SQL; the file path is intentionally synchronous (no await after taking
// ownership) so cancelling `release()` cannot leave a stale marker. `unused_async` only applies
// when neither `pg` nor `mysql` is enabled.
#[cfg_attr(
    not(any(feature = "pg", feature = "mysql")),
    allow(clippy::unused_async)
)]
async fn release_inner(inner: GuardInner) -> Result<(), DbLockError> {
    match inner {
        GuardInner::File { path, file } => {
            drop(file);
            // Synchronous: no await after taking ownership, so cancelling `release()` cannot
            // leave a stale marker the way async `remove_file` could.
            Ok(remove_file_lock_marker_result(&path)?)
        }
        #[cfg(feature = "pg")]
        GuardInner::Postgres {
            session,
            lock_key,
            generation,
            key_fingerprint,
        } => release_pg(&session, lock_key, generation, &key_fingerprint).await,
        #[cfg(feature = "mysql")]
        GuardInner::MySql {
            session,
            lock_name,
            generation,
            key_fingerprint,
        } => release_mysql(&session, &lock_name, generation, &key_fingerprint).await,
    }
}

/// Generation-checked PG unlock on the shared session (used by [`DbLockGuard::release`]).
#[cfg(feature = "pg")]
async fn release_pg(
    session: &PgSession,
    lock_key: i64,
    generation: u64,
    key_fingerprint: &str,
) -> Result<(), DbLockError> {
    // The in-process claim is given up only where PG is known to no longer hold the key. If the
    // unlock cannot be confirmed on a live session the key stays claimed — see [`HeldKeys`].
    if session.generation.load(Ordering::Acquire) != generation {
        session.held_keys.unclaim(&lock_key);
        tracing::debug!(
            backend = "postgres",
            key_fingerprint = %key_fingerprint,
            "release skipped: session reconnected, lock already gone"
        );
        return Ok(());
    }
    let mut conn = match session.pool.acquire().await {
        Ok(conn) => conn,
        Err(error) if is_connection_lost(&error) => {
            session.held_keys.unclaim(&lock_key);
            return Ok(());
        }
        Err(error) => return Err(DbLockError::Database(error)),
    };
    // Re-check after acquire: a reconnect during acquire bumps the generation.
    if session.generation.load(Ordering::Acquire) != generation {
        session.held_keys.unclaim(&lock_key);
        return Ok(());
    }
    match sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock($1)")
        .bind(lock_key)
        .fetch_one(&mut *conn)
        .await
    {
        Ok(true) => {
            session.held_keys.unclaim(&lock_key);
            Ok(())
        }
        // `false` means PG was not holding it, so there is no stale hold to guard against.
        Ok(false) => {
            session.held_keys.unclaim(&lock_key);
            Err(DbLockError::NotHeld)
        }
        Err(error) if is_connection_lost(&error) => {
            session.held_keys.unclaim(&lock_key);
            tracing::debug!(
                backend = "postgres",
                key_fingerprint = %key_fingerprint,
                %error,
                "release: session lost; treating lock as released"
            );
            Ok(())
        }
        Err(error) => {
            tracing::warn!(
                backend = "postgres",
                key_fingerprint = %key_fingerprint,
                %error,
                "pg_advisory_unlock failed; key stays claimed until this session reconnects"
            );
            Err(DbLockError::Database(error))
        }
    }
}

/// Generation-checked `MySQL` unlock on the shared session (used by [`DbLockGuard::release`]).
#[cfg(feature = "mysql")]
async fn release_mysql(
    session: &MySqlSession,
    lock_name: &str,
    generation: u64,
    key_fingerprint: &str,
) -> Result<(), DbLockError> {
    // The in-process claim is given up only where `MySQL` is known to no longer hold the key. If the
    // unlock cannot be confirmed on a live session the key stays claimed — see [`HeldKeys`].
    if session.generation.load(Ordering::Acquire) != generation {
        session.held_keys.unclaim(lock_name);
        tracing::debug!(
            backend = "mysql",
            key_fingerprint = %key_fingerprint,
            "release skipped: session reconnected, lock already gone"
        );
        return Ok(());
    }
    let mut conn = match session.pool.acquire().await {
        Ok(conn) => conn,
        Err(error) if is_connection_lost(&error) => {
            session.held_keys.unclaim(lock_name);
            return Ok(());
        }
        Err(error) => return Err(DbLockError::Database(error)),
    };
    if session.generation.load(Ordering::Acquire) != generation {
        session.held_keys.unclaim(lock_name);
        return Ok(());
    }
    match sqlx::query_scalar::<_, Option<i64>>("SELECT RELEASE_LOCK(?)")
        .bind(lock_name)
        .fetch_one(&mut *conn)
        .await
    {
        Ok(Some(1)) => {
            session.held_keys.unclaim(lock_name);
            Ok(())
        }
        // `0` = held by another session, `NULL` = no such lock. Either way this session is not the
        // holder, so there is no stale hold of ours to guard against.
        Ok(Some(0)) => {
            session.held_keys.unclaim(lock_name);
            tracing::debug!(
                backend = "mysql",
                key_fingerprint = %key_fingerprint,
                reason = "not_owned_by_session",
                "RELEASE_LOCK returned 0"
            );
            Err(DbLockError::NotHeld)
        }
        Ok(None) => {
            session.held_keys.unclaim(lock_name);
            tracing::debug!(
                backend = "mysql",
                key_fingerprint = %key_fingerprint,
                reason = "not_found",
                "RELEASE_LOCK returned NULL"
            );
            Err(DbLockError::NotHeld)
        }
        // Unexpected value: cannot conclude the key is free, so keep the claim.
        Ok(Some(other)) => Err(DbLockError::UnexpectedDatabaseResult {
            message: format!("RELEASE_LOCK returned {other}"),
        }),
        Err(error) if is_connection_lost(&error) => {
            session.held_keys.unclaim(lock_name);
            tracing::debug!(
                backend = "mysql",
                key_fingerprint = %key_fingerprint,
                %error,
                "release: session lost; treating lock as released"
            );
            Ok(())
        }
        Err(error) => {
            tracing::warn!(
                backend = "mysql",
                key_fingerprint = %key_fingerprint,
                %error,
                "RELEASE_LOCK failed; key stays claimed until this session reconnects"
            );
            Err(DbLockError::Database(error))
        }
    }
}

// --------------------------- Lock Manager ------------------------------------

#[derive(Debug, Clone)]
enum LockBackend {
    #[cfg_attr(not(feature = "sqlite"), allow(dead_code))]
    File,
    #[cfg(feature = "pg")]
    Postgres(Arc<PgSession>),
    #[cfg(feature = "mysql")]
    MySql(Arc<MySqlSession>),
}

/// Internal lock manager handling different database backends.
#[derive(Debug, Clone)]
pub(crate) struct LockManager {
    backend: LockBackend,
    instance_id: u64,
    database_scope: u64,
}

impl LockManager {
    #[must_use]
    #[cfg_attr(not(any(feature = "sqlite", test)), allow(dead_code))]
    pub fn file(database_scope: u64) -> Self {
        Self {
            backend: LockBackend::File,
            instance_id: new_instance_id(),
            database_scope,
        }
    }

    /// Build a PG lock manager with a dedicated single-connection session from typed options.
    ///
    /// # Errors
    /// Returns [`DbLockError::InvalidConfig`] if `keepalive` is zero, or [`DbLockError::Database`]
    /// if the dedicated lock connection cannot be established.
    #[cfg(feature = "pg")]
    pub async fn postgres_session(
        opts: sqlx::postgres::PgConnectOptions,
        database_scope: u64,
        keepalive: Duration,
    ) -> Result<Self, DbLockError> {
        let keepalive = validate_keepalive(keepalive)?;
        let generation = Arc::new(AtomicU64::new(0));
        let pool = pg_lock_pool_options(Arc::clone(&generation))
            .connect_with(opts)
            .await?;
        Ok(Self::from_pg_pool(
            pool,
            generation,
            database_scope,
            keepalive,
        ))
    }

    /// Build a PG lock manager with a dedicated single-connection session from a DSN.
    ///
    /// # Errors
    /// Returns [`DbLockError::InvalidConfig`] if `keepalive` is zero, or [`DbLockError::Database`]
    /// if the dedicated lock connection cannot be established.
    #[cfg(feature = "pg")]
    pub async fn postgres_session_dsn(
        dsn: &str,
        database_scope: u64,
        keepalive: Duration,
    ) -> Result<Self, DbLockError> {
        let keepalive = validate_keepalive(keepalive)?;
        let generation = Arc::new(AtomicU64::new(0));
        let pool = pg_lock_pool_options(Arc::clone(&generation))
            .connect(dsn)
            .await?;
        Ok(Self::from_pg_pool(
            pool,
            generation,
            database_scope,
            keepalive,
        ))
    }

    #[cfg(feature = "pg")]
    fn from_pg_pool(
        pool: sqlx::PgPool,
        generation: Arc<AtomicU64>,
        database_scope: u64,
        keepalive: Duration,
    ) -> Self {
        let (unlock_tx, unlock_rx) = tokio::sync::mpsc::unbounded_channel();
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let held_keys = Arc::new(HeldKeys::new());
        let task = spawn_pg_maintenance(
            pool.clone(),
            Arc::clone(&generation),
            Arc::clone(&held_keys),
            unlock_rx,
            Arc::clone(&shutdown),
            keepalive,
        );
        let session = PgSession {
            pool,
            generation,
            unlock_tx,
            shutdown,
            held_keys,
            task: Some(task),
        };
        Self {
            backend: LockBackend::Postgres(Arc::new(session)),
            instance_id: new_instance_id(),
            database_scope,
        }
    }

    /// Build a `MySQL` lock manager with a dedicated single-connection session from typed options.
    ///
    /// # Errors
    /// Returns [`DbLockError::InvalidConfig`] if `keepalive` is zero, or [`DbLockError::Database`]
    /// if the dedicated lock connection cannot be established.
    #[cfg(feature = "mysql")]
    pub async fn mysql_session(
        opts: sqlx::mysql::MySqlConnectOptions,
        database_scope: u64,
        keepalive: Duration,
    ) -> Result<Self, DbLockError> {
        let keepalive = validate_keepalive(keepalive)?;
        let generation = Arc::new(AtomicU64::new(0));
        let pool = mysql_lock_pool_options(Arc::clone(&generation))
            .connect_with(opts)
            .await?;
        Ok(Self::from_mysql_pool(
            pool,
            generation,
            database_scope,
            keepalive,
        ))
    }

    /// Build a `MySQL` lock manager with a dedicated single-connection session from a DSN.
    ///
    /// # Errors
    /// Returns [`DbLockError::InvalidConfig`] if `keepalive` is zero, or [`DbLockError::Database`]
    /// if the dedicated lock connection cannot be established.
    #[cfg(feature = "mysql")]
    pub async fn mysql_session_dsn(
        dsn: &str,
        database_scope: u64,
        keepalive: Duration,
    ) -> Result<Self, DbLockError> {
        let keepalive = validate_keepalive(keepalive)?;
        let generation = Arc::new(AtomicU64::new(0));
        let pool = mysql_lock_pool_options(Arc::clone(&generation))
            .connect(dsn)
            .await?;
        Ok(Self::from_mysql_pool(
            pool,
            generation,
            database_scope,
            keepalive,
        ))
    }

    #[cfg(feature = "mysql")]
    fn from_mysql_pool(
        pool: sqlx::MySqlPool,
        generation: Arc<AtomicU64>,
        database_scope: u64,
        keepalive: Duration,
    ) -> Self {
        let (unlock_tx, unlock_rx) = tokio::sync::mpsc::unbounded_channel();
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let held_keys = Arc::new(HeldKeys::new());
        let task = spawn_mysql_maintenance(
            pool.clone(),
            Arc::clone(&generation),
            Arc::clone(&held_keys),
            unlock_rx,
            Arc::clone(&shutdown),
            keepalive,
        );
        let session = MySqlSession {
            pool,
            generation,
            unlock_tx,
            shutdown,
            held_keys,
            task: Some(task),
        };
        Self {
            backend: LockBackend::MySql(Arc::new(session)),
            instance_id: new_instance_id(),
            database_scope,
        }
    }

    #[must_use]
    #[allow(dead_code)] // diagnostics / tests
    pub fn database_scope(&self) -> u64 {
        self.database_scope
    }

    #[must_use]
    #[allow(dead_code)] // diagnostics / tests
    pub fn instance_id(&self) -> u64 {
        self.instance_id
    }

    /// Acquire an advisory lock for `{gear}:{key}` with a single non-blocking attempt.
    ///
    /// # Errors
    /// Returns [`DbLockError::AlreadyHeld`] on contention. On PG/MySQL, SQL errors map to
    /// `DbLockError::Database`.
    pub async fn lock(&self, gear: &str, key: &str) -> Result<DbLockGuard, DbLockError> {
        let display_key = format!("{gear}:{key}");
        let canonical = canonical_lock_input(self.database_scope, gear, key);
        match self.try_acquire_once(&display_key, &canonical).await? {
            Some(guard) => {
                tracing::debug!(key = %display_key, "advisory lock acquired");
                Ok(guard)
            }
            None => Err(DbLockError::AlreadyHeld {
                lock_name: display_key,
            }),
        }
    }

    /// Try to acquire an advisory lock with retry/backoff policy.
    ///
    /// Returns:
    /// - `Ok(Some(guard))` if lock acquired
    /// - `Ok(None)` if timed out or attempts exceeded
    /// - `Err(e)` on unrecoverable error (including invalid config)
    ///
    /// `config.max_wait` bounds retry scheduling between completed attempts; it does not cancel
    /// an in-flight pool acquire or advisory-lock SQL query.
    ///
    /// # Cancellation safety
    ///
    /// This future is cancellation-safe. Dropping it mid-retry will not leak a lock: file
    /// markers are cleaned synchronously on [`DbLockGuard`] Drop, and native guards queue a
    /// generation-checked unlock on Drop (drained by the session maintenance task). Callers that
    /// need cooperative shutdown may wrap the call in `tokio::select!`:
    ///
    /// ```ignore
    /// tokio::select! {
    ///     result = manager.try_lock(gear, key, config) => { /* handle */ }
    ///     _ = cancellation_token.cancelled() => { /* shutdown */ }
    /// }
    /// ```
    pub async fn try_lock(
        &self,
        gear: &str,
        key: &str,
        config: LockConfig,
    ) -> Result<Option<DbLockGuard>, DbLockError> {
        use tokio_retry::RetryIf;
        use tokio_retry::strategy::{ExponentialBackoff, jitter};

        config.validate()?;

        let display_key = format!("{gear}:{key}");
        let canonical = canonical_lock_input(self.database_scope, gear, key);
        let start = Instant::now();

        // Exponential backoff in the shared retry vocabulary, capped at
        // `max_backoff`.
        let jitter_on = config.jitter;
        let max_wait = config.max_wait;
        let strategy = ExponentialBackoff::from_millis(config.backoff_base_ms)
            .factor(config.backoff_factor)
            .max_delay(config.max_backoff)
            // Stop yielding delays once the wall-clock budget is spent — this is
            // what bounds total wait when `max_wait` is set. Pulled between
            // attempts, exactly where the previous loop checked the deadline.
            .take_while(move |_| max_wait.is_none_or(|mw| start.elapsed() < mw))
            // Cap each delay by the remaining budget, then optionally jitter.
            .map(move |d| {
                let capped = max_wait.map_or(d, |mw| d.min(mw.saturating_sub(start.elapsed())));
                if jitter_on { jitter(capped) } else { capped }
            })
            // Retries after the first attempt (unlimited when `None`, bounded
            // only by `max_wait`).
            .take(config.max_retries.map_or(usize::MAX, |r| r as usize));

        // `try_acquire_once` returns `Ok(None)` while the lock is held
        // elsewhere; model that as a retryable sentinel so tokio-retry drives
        // the backoff, and fold the exhausted sentinel back into `Ok(None)`.
        //
        // The attempt counter is an atomic, not a `Cell`, so the returned future stays `Send`
        // and callers can keep spawning `try_lock`.
        let attempts = AtomicU32::new(0);
        let action = || async {
            attempts.fetch_add(1, Ordering::Relaxed);
            match self.try_acquire_once(&display_key, &canonical).await {
                Ok(Some(guard)) => Ok(guard),
                Ok(None) => Err(TryLockError::Pending),
                Err(e) => Err(TryLockError::Fatal(e)),
            }
        };
        let retryable = |e: &TryLockError| matches!(e, TryLockError::Pending);

        match RetryIf::start(strategy, action, retryable).await {
            Ok(guard) => {
                tracing::debug!(
                    key = %display_key,
                    attempt = attempts.load(Ordering::Relaxed),
                    elapsed = ?start.elapsed(),
                    "advisory lock acquired via try_lock"
                );
                Ok(Some(guard))
            }
            Err(TryLockError::Pending) => Ok(None),
            Err(TryLockError::Fatal(e)) => Err(e),
        }
    }

    async fn try_acquire_once(
        &self,
        display_key: &str,
        canonical: &str,
    ) -> Result<Option<DbLockGuard>, DbLockError> {
        match &self.backend {
            LockBackend::File => self.try_lock_file(display_key, canonical).await,
            #[cfg(feature = "pg")]
            LockBackend::Postgres(session) => {
                Self::try_lock_postgres(session, display_key, canonical).await
            }
            #[cfg(feature = "mysql")]
            LockBackend::MySql(session) => {
                Self::try_lock_mysql(session, display_key, canonical).await
            }
        }
    }

    async fn try_lock_file(
        &self,
        display_key: &str,
        canonical: &str,
    ) -> Result<Option<DbLockGuard>, DbLockError> {
        let path = self.get_lock_file_path(canonical);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            // Marker creation defines acquisition. Transfer ownership to the guard immediately
            // after a successful open result — no further await in this path. (Cancellation
            // while the open itself is in flight may still leave a marker; see module docs.)
            Ok(file) => Ok(Some(DbLockGuard {
                namespaced_key: display_key.to_owned(),
                inner: Some(GuardInner::File { path, file }),
            })),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// File path derived from `database_scope` + hash(canonical). Raw DSN does not participate.
    fn get_lock_file_path(&self, canonical: &str) -> PathBuf {
        let base_dir = if cfg!(test) {
            std::env::temp_dir().join("cf_gears_test_locks")
        } else {
            let cache = dirs::cache_dir().unwrap_or_else(std::env::temp_dir);
            cache.join("cf-gears").join("locks")
        };

        let scope_dir = format!("{:016x}", self.database_scope);
        let key_hash = format!("{:016x}", xxh3_64(canonical.as_bytes()));
        base_dir.join(scope_dir).join(format!("{key_hash}.lock"))
    }

    /// Single non-blocking PG acquire on the shared session. A held lock does not pin the
    /// connection — it returns to the pool immediately while the session remembers the key.
    #[cfg(feature = "pg")]
    async fn try_lock_postgres(
        session: &Arc<PgSession>,
        display_key: &str,
        canonical: &str,
    ) -> Result<Option<DbLockGuard>, DbLockError> {
        let mut conn = session.pool.acquire().await?;
        // Read AFTER acquire so a reconnect during acquire is reflected in the recorded generation.
        let generation = session.generation.load(Ordering::Acquire);
        let lock_key = stable_lock_key(canonical);
        let fingerprint = key_fingerprint(canonical);

        // `pg_try_advisory_lock` is re-entrant on the session every guard shares, so in-process
        // exclusivity is decided here, before the SQL. Contended → `None`, exactly like a key held
        // by another process.
        if !session.held_keys.claim(lock_key, generation) {
            return Ok(None);
        }

        let result: Result<bool, sqlx::Error> =
            sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
                .bind(lock_key)
                .fetch_one(&mut *conn)
                .await;

        match result {
            Ok(true) => Ok(Some(DbLockGuard {
                namespaced_key: display_key.to_owned(),
                inner: Some(GuardInner::Postgres {
                    session: Arc::clone(session),
                    lock_key,
                    generation,
                    key_fingerprint: fingerprint,
                }),
            })),
            // No guard is produced on either path, so the claim must not outlive this call.
            Ok(false) => {
                session.held_keys.unclaim(&lock_key);
                Ok(None)
            }
            Err(error) => {
                session.held_keys.unclaim(&lock_key);
                tracing::warn!(
                    backend = "postgres",
                    key_fingerprint = %fingerprint,
                    %error,
                    "pg_try_advisory_lock failed"
                );
                Err(DbLockError::Database(error))
            }
        }
    }

    /// Single non-blocking `MySQL` acquire (`GET_LOCK(_, 0)`) on the shared session.
    #[cfg(feature = "mysql")]
    async fn try_lock_mysql(
        session: &Arc<MySqlSession>,
        display_key: &str,
        canonical: &str,
    ) -> Result<Option<DbLockGuard>, DbLockError> {
        let mut conn = session.pool.acquire().await?;
        let generation = session.generation.load(Ordering::Acquire);
        let lock_name = mysql_lock_name(canonical);
        let fingerprint = key_fingerprint(canonical);

        // `GET_LOCK` is re-entrant on the session every guard shares (MySQL 5.7.5+ grants the same
        // name to the same session repeatedly), so in-process exclusivity is decided here, before
        // the SQL. Contended → `None`, exactly like a key held by another process.
        if !session.held_keys.claim(lock_name.clone(), generation) {
            return Ok(None);
        }

        // Timeout 0 — non-blocking only.
        let result: Result<Option<i64>, sqlx::Error> = sqlx::query_scalar("SELECT GET_LOCK(?, 0)")
            .bind(&lock_name)
            .fetch_one(&mut *conn)
            .await;

        match result {
            Ok(Some(1)) => Ok(Some(DbLockGuard {
                namespaced_key: display_key.to_owned(),
                inner: Some(GuardInner::MySql {
                    session: Arc::clone(session),
                    lock_name,
                    generation,
                    key_fingerprint: fingerprint,
                }),
            })),
            // No guard is produced on any path below, so the claim must not outlive this call.
            Ok(Some(0)) => {
                session.held_keys.unclaim(&lock_name);
                Ok(None)
            }
            Ok(None) => {
                session.held_keys.unclaim(&lock_name);
                tracing::warn!(
                    backend = "mysql",
                    key_fingerprint = %fingerprint,
                    "GET_LOCK returned NULL"
                );
                Err(DbLockError::UnexpectedDatabaseResult {
                    message: "GET_LOCK returned NULL".to_owned(),
                })
            }
            Ok(Some(other)) => {
                session.held_keys.unclaim(&lock_name);
                Err(DbLockError::UnexpectedDatabaseResult {
                    message: format!("GET_LOCK returned {other}"),
                })
            }
            Err(error) => {
                session.held_keys.unclaim(&lock_name);
                tracing::warn!(
                    backend = "mysql",
                    key_fingerprint = %fingerprint,
                    %error,
                    "GET_LOCK failed"
                );
                Err(DbLockError::Database(error))
            }
        }
    }
}

// --------------------------- Errors ------------------------------------------

#[derive(Error, Debug)]
pub enum DbLockError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[cfg(any(feature = "pg", feature = "mysql"))]
    #[error("Database advisory-lock operation failed: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Lock already held: {lock_name}")]
    AlreadyHeld { lock_name: String },

    #[error("Advisory lock was not held during release")]
    NotHeld,

    #[error("Unexpected database advisory-lock result: {message}")]
    UnexpectedDatabaseResult { message: String },

    #[error("Lock configuration is invalid: {message}")]
    InvalidConfig { message: String },
}

// --------------------------- Tests -------------------------------------------

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::sync::Arc;

    #[cfg(any(feature = "pg", feature = "mysql"))]
    #[test]
    fn is_connection_lost_classification() {
        use std::io;

        // Transport-level failures mean the session is gone → treat lock as released.
        assert!(is_connection_lost(&sqlx::Error::Io(io::Error::from(
            io::ErrorKind::BrokenPipe
        ))));
        assert!(is_connection_lost(&sqlx::Error::PoolClosed));

        // A pool timeout does NOT mean the session died: with `max_connections(1)` it can fire
        // while the session is alive and still holding the key, so the unlock must not be skipped.
        assert!(!is_connection_lost(&sqlx::Error::PoolTimedOut));

        // Non-connection sqlx errors are NOT connection-lost.
        assert!(!is_connection_lost(&sqlx::Error::RowNotFound));
    }

    #[cfg(any(feature = "pg", feature = "mysql"))]
    #[test]
    fn validate_keepalive_rejects_zero_and_passes_positive() {
        // Zero would panic `tokio::time::interval`; it must be rejected, not silently defaulted.
        assert!(matches!(
            validate_keepalive(Duration::ZERO),
            Err(DbLockError::InvalidConfig { .. })
        ));
        // A positive interval passes through unchanged (no default re-resolution here).
        let keepalive = Duration::from_millis(250);
        assert_eq!(validate_keepalive(keepalive).unwrap(), keepalive);
    }

    #[test]
    fn stable_lock_key_is_stable() {
        // database_scope = 1 → "0000000000000001"
        // input: UTF-8 of full canonical; XXH3-64; PG key = bit pattern as i64
        let canonical = canonical_lock_input(1, "zoveon", "phone-case");
        assert_eq!(
            canonical,
            "cf-gears-toolkit-db:v2:0000000000000001:g6:zoveon:k10:phone-case"
        );
        assert_eq!(stable_lock_key(&canonical), 7_193_862_067_539_650_702_i64);
        assert_eq!(mysql_lock_name(&canonical), "cf:63d5bb6f8adba88e");
    }

    #[test]
    fn canonical_input_has_no_component_boundary_collisions() {
        let a = canonical_lock_input(1, "a:b", "c");
        let b = canonical_lock_input(1, "a", "b:c");

        assert_ne!(a, b);
        assert_eq!(a, "cf-gears-toolkit-db:v2:0000000000000001:g3:a:b:k1:c");
        assert_eq!(b, "cf-gears-toolkit-db:v2:0000000000000001:g1:a:k3:b:c");
        assert_ne!(stable_lock_key(&a), stable_lock_key(&b));
        assert_ne!(mysql_lock_name(&a), mysql_lock_name(&b));
    }

    #[test]
    fn canonical_has_no_case_normalization() {
        let a = canonical_lock_input(1, "Gear", "Key");
        let b = canonical_lock_input(1, "gear", "key");
        assert_ne!(a, b);
        assert_ne!(stable_lock_key(&a), stable_lock_key(&b));
    }

    /// Hostnames are case-insensitive per DNS and a trailing `/` is not part of the database name,
    /// so peers writing the DSN either way must coordinate on one lock scope.
    #[test]
    fn database_scope_normalizes_host_case_and_trailing_slash() {
        let canonical = database_scope_from_dsn("postgres://db.example:5432/zoveon");
        assert_eq!(
            database_scope_from_dsn("postgres://DB.Example:5432/zoveon"),
            canonical
        );
        assert_eq!(
            database_scope_from_dsn("postgres://db.example:5432/zoveon/"),
            canonical
        );
        // Normalization lives in `server_database_identity`, so the typed-options entry point
        // (which passes `opts.get_host()` straight through) agrees with the parsed-DSN one.
        assert_eq!(
            database_scope_from_identity(&server_database_identity(
                "postgres",
                "DB.Example",
                5432,
                "zoveon/"
            )),
            canonical
        );
    }

    /// Normalization must not blur genuinely different databases.
    #[test]
    fn database_scope_still_separates_distinct_databases() {
        assert_ne!(
            database_scope_from_dsn("postgres://db.example:5432/zoveon"),
            database_scope_from_dsn("postgres://db.example:5432/zoveonx")
        );
    }

    /// The shared native session makes the database's own advisory locks re-entrant, so
    /// [`HeldKeys`] is what preserves exclusivity within one process.
    #[cfg(any(feature = "pg", feature = "mysql"))]
    #[test]
    fn held_keys_claim_is_exclusive_until_unclaimed() {
        let held: HeldKeys<i64> = HeldKeys::new();

        assert!(held.claim(42, 1), "first claim must succeed");
        assert!(
            !held.claim(42, 1),
            "second claim of a held key must be refused"
        );
        // Distinct keys stay independent — one held key must not block the rest.
        assert!(held.claim(43, 1));

        held.unclaim(&42);
        assert!(held.claim(42, 1), "key is claimable again after unclaim");
    }

    /// A claim retained because an unlock could not be confirmed must not be permanent: once the
    /// session reconnects, the lock it protected is gone server-side and the key frees up.
    ///
    /// This is what keeps the fail-closed rule in [`HeldKeys`] from deadlocking a key forever.
    #[cfg(any(feature = "pg", feature = "mysql"))]
    #[test]
    fn held_keys_claim_from_a_dead_generation_is_stale() {
        let held: HeldKeys<i64> = HeldKeys::new();

        // Generation 1 holds the key and its unlock failed, so the claim was deliberately kept.
        assert!(held.claim(42, 1));
        assert!(!held.claim(42, 1), "still held while generation 1 is live");

        // Reconnect: generation 2 is a new physical connection, so PG/MySQL dropped every lock the
        // old one held.
        assert!(held.claim(42, 2), "stale claim must be taken over");
        // The key now belongs to generation 2 and excludes again.
        assert!(!held.claim(42, 2));
    }

    /// `MySQL` keys are `String`s; unclaiming borrows as `&str` (no allocation on the release path).
    #[cfg(any(feature = "pg", feature = "mysql"))]
    #[test]
    fn held_keys_unclaims_string_keys_by_borrow() {
        let held: HeldKeys<String> = HeldKeys::new();

        assert!(held.claim("cf:dead".to_owned(), 1));
        assert!(!held.claim("cf:dead".to_owned(), 1));

        held.unclaim("cf:dead");
        assert!(held.claim("cf:dead".to_owned(), 1));
    }

    #[test]
    fn database_scope_ignores_credentials_in_dsn() {
        let a = database_scope_from_dsn("postgres://alice:secret@db.example:5432/zoveon");
        let b = database_scope_from_dsn("postgres://bob:other@db.example:5432/zoveon");
        assert_eq!(a, b);
    }

    #[test]
    fn database_scope_normalizes_postgres_scheme() {
        let a = database_scope_from_dsn("postgres://db.example:5432/app");
        let b = database_scope_from_dsn("postgresql://db.example:5432/app");
        assert_eq!(a, b);
    }

    #[test]
    #[cfg(feature = "sqlite")]
    fn sqlite_relative_dot_paths_share_scope() {
        let a = database_scope_from_dsn("sqlite:./test.db");
        let b = database_scope_from_dsn("sqlite:test.db");
        assert_eq!(a, b);
    }

    #[test]
    #[cfg(feature = "sqlite")]
    fn sqlite_dotdot_paths_share_scope_lexically() {
        let a = database_scope_from_dsn("sqlite:./data/../data/app.db");
        let b = database_scope_from_dsn("sqlite:data/app.db");
        assert_eq!(a, b);
    }

    /// Pins the contract that lets the two lock-scope entry points agree: for a file-backed
    /// database the scope depends **only** on the resolved file path, never on DSN query
    /// parameters (`extract_file_path_from_dsn` strips the query).
    ///
    /// This matters because the two callers see different DSN text for the same database.
    /// `DbHandle::connect` hashes `clean_dsn`, which still carries non-PRAGMA parameters, while
    /// `DbConnectOptions::Sqlite` rebuilds `sqlite://{filename}` from a path that
    /// `parse_sqlite_path_from_dsn` already stripped. They land on one scope only as long as
    /// parameters are scope-irrelevant.
    ///
    /// If parameters ever start influencing *which* database is opened — e.g. passing
    /// `mode=memory&cache=shared` through to `connect` instead of dropping it — this assertion
    /// must be revisited: the scope would then have to encode them, or the same logical
    /// shared-memory database would be split across two lock namespaces.
    #[test]
    #[cfg(feature = "sqlite")]
    fn sqlite_scope_ignores_dsn_query_parameters() {
        let plain = database_scope_from_dsn("sqlite:data/app.db");

        assert_eq!(
            database_scope_from_dsn("sqlite:data/app.db?mode=rwc&cache=shared"),
            plain
        );
        assert_eq!(
            database_scope_from_dsn("sqlite:data/app.db?_pragma=busy_timeout(5000)"),
            plain
        );

        // The file path itself still separates databases.
        assert_ne!(database_scope_from_dsn("sqlite:data/other.db"), plain);
    }

    #[test]
    fn database_scope_differs_by_database_name() {
        let a = database_scope_from_dsn("postgres://db.example:5432/zoveon");
        let b = database_scope_from_dsn("postgres://db.example:5432/other");
        assert_ne!(a, b);
    }

    /// Every rejected value would collapse the exponential backoff to zero delay, turning the
    /// retry loop into a busy spin until `max_wait` expires.
    #[test]
    fn lock_config_rejects_invalid_values() {
        assert!(matches!(
            LockConfig {
                backoff_base_ms: 0,
                ..Default::default()
            }
            .validate(),
            Err(DbLockError::InvalidConfig { .. })
        ));

        assert!(matches!(
            LockConfig {
                backoff_factor: 0,
                ..Default::default()
            }
            .validate(),
            Err(DbLockError::InvalidConfig { .. })
        ));

        assert!(matches!(
            LockConfig {
                max_backoff: Duration::ZERO,
                ..Default::default()
            }
            .validate(),
            Err(DbLockError::InvalidConfig { .. })
        ));
    }

    /// `max_retries` counts retries *after* the first attempt, so zero means "one attempt, no
    /// retries" — valid, unlike the old `max_attempts: Some(0)`.
    #[test]
    fn lock_config_allows_zero_retries() {
        let cfg = LockConfig {
            max_retries: Some(0),
            ..Default::default()
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn lock_config_allows_unlimited_wait() {
        let cfg = LockConfig {
            max_wait: None,
            max_retries: None,
            ..Default::default()
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn clone_preserves_instance_id_and_scope() {
        let a = LockManager::file(0xdead_beef);
        let b = a.clone();
        assert_eq!(a.instance_id(), b.instance_id());
        assert_eq!(a.database_scope(), b.database_scope());

        let c = LockManager::file(0xdead_beef);
        assert_ne!(a.instance_id(), c.instance_id());
        assert_eq!(a.database_scope(), c.database_scope());
    }

    #[tokio::test]
    async fn test_namespaced_locks() -> Result<()> {
        let lock_manager = LockManager::file(0x11);
        let test_id = format!(
            "test_ns_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let guard1 = lock_manager
            .lock("gear1", &format!("{test_id}_key"))
            .await?;
        let guard2 = lock_manager
            .lock("gear2", &format!("{test_id}_key"))
            .await?;

        assert!(!guard1.key().is_empty());
        assert!(!guard2.key().is_empty());

        guard1.release().await?;
        guard2.release().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_try_lock_different_key_succeeds() -> Result<()> {
        let lock_manager = Arc::new(LockManager::file(0x22));
        let test_id = format!(
            "test_diff_key_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let _guard1 = lock_manager
            .lock("test_gear", &format!("{test_id}_key"))
            .await?;

        let config = LockConfig {
            max_wait: Some(Duration::from_millis(200)),
            max_retries: Some(3),
            ..Default::default()
        };

        let result = lock_manager
            .try_lock("test_gear", &format!("{test_id}_different_key"), config)
            .await?;
        assert!(result.is_some(), "expected successful lock acquisition");
        Ok(())
    }

    #[tokio::test]
    async fn test_try_lock_exhausted_attempts_returns_none_without_extra_sleep() -> Result<()> {
        let lock_manager = LockManager::file(0x66);
        let key = format!(
            "test_no_extra_sleep_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let _guard = lock_manager.lock("gear", &key).await?;
        let config = LockConfig {
            max_wait: Some(Duration::from_secs(30)),
            // Pin every delay to ~100ms so the elapsed-time assertion below is meaningful.
            max_backoff: Duration::from_millis(100),
            // One retry after the first attempt = two attempts total.
            max_retries: Some(1),
            ..Default::default()
        };

        let start = Instant::now();
        let res = lock_manager.try_lock("gear", &key, config).await?;
        assert!(res.is_none());
        // Two failed attempts + one inter-attempt sleep (~100ms), not a second post-final sleep.
        assert!(
            start.elapsed() < Duration::from_millis(500),
            "unexpected long wait: {:?}",
            start.elapsed()
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_try_lock_success() -> Result<()> {
        let lock_manager = LockManager::file(0x33);
        let test_id = format!(
            "test_success_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let result = lock_manager
            .try_lock(
                "test_gear",
                &format!("{test_id}_key"),
                LockConfig::default(),
            )
            .await?;
        assert!(result.is_some(), "expected lock acquisition");
        if let Some(g) = result {
            g.release().await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_double_lock_same_key_errors() -> Result<()> {
        let lock_manager = LockManager::file(0x44);
        let test_id = format!(
            "test_double_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let guard = lock_manager.lock("test_gear", &test_id).await?;
        let err = lock_manager.lock("test_gear", &test_id).await.unwrap_err();
        match err {
            DbLockError::AlreadyHeld { lock_name } => {
                assert!(lock_name.contains(&test_id));
            }
            other => panic!("unexpected error: {other:?}"),
        }

        guard.release().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_try_lock_conflict_returns_none() -> Result<()> {
        let lock_manager = LockManager::file(0x55);
        let key = format!(
            "test_conflict_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let _guard = lock_manager.lock("gear", &key).await?;
        let config = LockConfig {
            max_wait: Some(Duration::from_millis(100)),
            max_retries: Some(2),
            ..Default::default()
        };
        let res = lock_manager.try_lock("gear", &key, config).await?;
        assert!(res.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn file_explicit_release_allows_immediate_reacquire() -> Result<()> {
        let manager = LockManager::file(0x71);
        let key = format!(
            "reacquire_release_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let guard = manager.lock("gear", &key).await?;
        guard.release().await?;

        let guard = manager.lock("gear", &key).await?;
        guard.release().await?;
        Ok(())
    }

    #[tokio::test]
    async fn file_drop_allows_immediate_reacquire() -> Result<()> {
        let manager = LockManager::file(0x72);
        let key = format!(
            "reacquire_drop_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        {
            let _guard = manager.lock("gear", &key).await?;
        }

        let guard = manager.lock("gear", &key).await?;
        guard.release().await?;
        Ok(())
    }

    #[test]
    fn file_drop_after_runtime_shutdown_does_not_panic() {
        // File cleanup is synchronous and must not depend on a live runtime / spawned task.
        let manager = LockManager::file(0x73);
        let key = format!(
            "drop_after_shutdown_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let guard = runtime
            .block_on(async { manager.lock("gear", &key).await })
            .expect("lock");
        drop(runtime);

        // No current Tokio handle: file Drop must still remove the marker without panicking.
        drop(guard);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let guard = manager.lock("gear", &key).await.expect("reacquire");
            guard.release().await.expect("release");
        });
    }

    #[tokio::test]
    async fn file_path_uses_scope_not_independent_dsn() -> Result<()> {
        let a = LockManager::file(0xabc);
        let b = LockManager::file(0xabc);
        let path_a = a.get_lock_file_path(&canonical_lock_input(0xabc, "g", "k"));
        let path_b = b.get_lock_file_path(&canonical_lock_input(0xabc, "g", "k"));
        assert_eq!(path_a, path_b);

        let c = LockManager::file(0xdef);
        let path_c = c.get_lock_file_path(&canonical_lock_input(0xdef, "g", "k"));
        assert_ne!(path_a, path_c);
        Ok(())
    }

    /// A `max_backoff` below the first delay is legitimate now: it clamps every delay rather
    /// than being a misconfiguration (the old `initial_backoff` field it was compared against
    /// no longer exists).
    #[test]
    fn lock_config_allows_max_backoff_below_first_delay() {
        let cfg = LockConfig {
            backoff_base_ms: 2,
            backoff_factor: 25,
            max_backoff: Duration::from_millis(10),
            ..Default::default()
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn database_scope_fallback_for_unrecognized_dsn() {
        let scope = database_scope_from_dsn("custom://foo/bar");
        assert_ne!(scope, 0);
        assert_eq!(scope, database_scope_from_dsn("custom://foo/bar"));
        assert_ne!(scope, database_scope_from_dsn("postgres://foo:5432/bar"));
    }
}
