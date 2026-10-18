//! Coalescing write-behind cache with incremental JSONL persistence.
//!
//! Apps post session snapshots keyed by session id; the writer thread drains
//! the newest snapshot of every session per wake and performs O(delta)
//! appends. Deletes travel through the same per-session slot as saves, so
//! whichever the app asked for last is what reaches disk.

use std::collections::{HashMap, HashSet};
use std::io;
use std::mem;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use maki_storage::checkpoint::{
    CheckpointAck, CheckpointError, CheckpointFuture, CheckpointPublication, CheckpointRequest,
    CheckpointVersion, CheckpointWriter,
};
use maki_storage::id::MakiId;
use maki_storage::session_lock::SessionPublicationGuard;
use maki_storage::sessions::{
    HistoryIdentity, HistorySnapshot, SESSIONS_DIR, SessionError, SessionLog,
};
use maki_storage::{StateDir, StorageError};
use tracing::warn;

use maki_agent::session_coordinator::SessionCheckpoint;
use maki_agent::session_options::{
    ENABLED_VALUE, FAST_OPTION_ID, SessionOptionOwner, THINKING_OPTION_ID, WORKFLOW_OPTION_ID,
    YOLO_OPTION_ID,
};
use maki_agent::tools::offload::OffloadStore;
use maki_agent::{AgentMode, ThinkingConfig};
use maki_storage::sessions::StoredMode;

use crate::AppSession;
use maki_providers::Message;

pub(crate) const SAVE_FAILED_PREFIX: &str = "Session save failed";
const SAVE_RECOVERED: &str = "Session save recovered";
const CHECKPOINT_REPLACED: &str = "checkpoint payload superseded by authoritative session state";
const CHECKPOINT_CANCELLED: &str = "child approval checkpoint cancelled";
#[cfg(not(test))]
const CHECKPOINT_RETRY_BACKOFFS: &[Duration] = &[
    Duration::from_millis(100),
    Duration::from_millis(500),
    Duration::from_secs(2),
];
#[cfg(test)]
const CHECKPOINT_RETRY_BACKOFFS: &[Duration] =
    &[Duration::from_millis(600), Duration::from_millis(1200)];
#[cfg(not(test))]
const BACKGROUND_RETRY_BACKOFFS: &[Duration] = &[
    Duration::from_secs(1),
    Duration::from_secs(5),
    Duration::from_secs(30),
];
#[cfg(test)]
const BACKGROUND_RETRY_BACKOFFS: &[Duration] =
    &[Duration::from_millis(20), Duration::from_millis(50)];

type Pending = Arc<Mutex<PendingState>>;

#[derive(Default)]
struct PendingState {
    closed: bool,
    entries: HashMap<MakiId, Entry>,
    /// Latest authoritative state from the app or a successful coordinator
    /// checkpoint. It may still be newer than durable storage.
    latest: HashMap<MakiId, Arc<AppSession>>,
    latest_generations: HashMap<MakiId, u64>,
    coordinator_pending: HashMap<MakiId, CoordinatorPending>,
    coordinator_history_bases: HashMap<MakiId, HistoryIdentity>,
    config_versions: HashMap<MakiId, (u64, u64)>,
    live_children: HashMap<MakiId, Vec<String>>,
    next_generation: u64,
    child_approvals: Vec<ChildApprovalSave>,
    child_publications: HashMap<MakiId, Vec<CheckpointPublication>>,
    incarnations: HashMap<MakiId, Arc<()>>,
    #[cfg(test)]
    child_stage_gate: Option<(flume::Sender<()>, flume::Receiver<()>)>,
}

struct ChildApprovalSave {
    session_id: MakiId,
    child_key: String,
    history: HistorySnapshot<Message>,
    publication: CheckpointPublication,
    guard: SessionPublicationGuard,
    incarnation: Arc<()>,
    reply: flume::Sender<Result<(), String>>,
}

struct CoordinatorPending {
    generation: u64,
    session: Arc<AppSession>,
    history_base: Option<HistoryIdentity>,
}

type DeleteCallback = Box<dyn FnOnce(Result<(), SessionError>) + Send>;

/// One slot per session, holding whatever the app asked for last. Deletes
/// used to ride a side channel, where a flush queued before a delete could
/// drain a save enqueued after it, so the delete unlinked a session the app
/// had just saved.
enum Entry {
    Save(PendingSave),
    Delete(
        DeleteCallback,
        Option<SessionPublicationGuard>,
        Option<Arc<OffloadStore>>,
    ),
}

struct PendingSave {
    session: Arc<AppSession>,
    waiters: Vec<CheckpointWaiter>,
    generation: u64,
    coordinator_generation: Option<u64>,
    config_version: Option<(u64, u64)>,
    coordinator_history_base: Option<HistoryIdentity>,
    retry_attempt: usize,
    retry_at: Option<Instant>,
}

enum RequiredHistory {
    Root(Arc<Vec<Message>>),
    Child {
        key: String,
        history: HistorySnapshot<Message>,
    },
}

impl RequiredHistory {
    fn retained_by(&self, session: &AppSession) -> bool {
        match self {
            Self::Root(messages) => Arc::ptr_eq(messages, &session.history_snapshot().messages),
            Self::Child { key, history } => {
                session.subagent_history_snapshot(key).is_some_and(|saved| {
                    saved.epoch == history.epoch && Arc::ptr_eq(&saved.messages, &history.messages)
                })
            }
        }
    }
}

struct CheckpointWaiter {
    version: CheckpointVersion,
    config_version: Option<(u64, u64)>,
    histories: Vec<RequiredHistory>,
    reply: flume::Sender<Result<CheckpointAck, CheckpointError>>,
}

fn required_histories(session: &AppSession) -> Vec<RequiredHistory> {
    let mut histories = vec![RequiredHistory::Root(session.history_snapshot().messages)];
    histories.extend(session.subagent_messages().keys().filter_map(|key| {
        session
            .subagent_history_snapshot(key)
            .map(|history| RequiredHistory::Child {
                key: key.clone(),
                history,
            })
    }));
    histories
}

pub struct StorageWriter {
    pending: Pending,
    wake: flume::Sender<()>,
    done_rx: flume::Receiver<()>,
    /// Asks the writer thread to finish. Closing the wake channel is not
    /// enough: every coordinator's checkpoint writer holds a clone of the
    /// sender, and those live in tasks that exit on their own schedule, so
    /// waiting for the last one to drop makes exit take as long as the
    /// timeout allows.
    stop: Arc<AtomicBool>,
}

#[derive(Clone)]
struct CoordinatorCheckpointWriter {
    pending: Pending,
    wake: flume::Sender<()>,
}

impl CheckpointWriter<AppSession> for StorageWriter {
    fn checkpoint(&self, request: CheckpointRequest<AppSession>) -> CheckpointFuture {
        self.enqueue_checkpoint(request)
    }
}

impl CheckpointWriter<SessionCheckpoint> for CoordinatorCheckpointWriter {
    fn checkpoint(&self, request: CheckpointRequest<SessionCheckpoint>) -> CheckpointFuture {
        let session_id = request.session_id;
        let version = request.version;
        let mut state = lock(&self.pending);
        if state.closed {
            return Box::pin(async move { Err(CheckpointError::Closed(session_id)) });
        }
        let config_version = request
            .snapshot
            .config
            .as_ref()
            .map(|config| (config.runtime_epoch, config.generation));
        if config_version.is_some_and(|incoming| {
            state
                .config_versions
                .get(&session_id)
                .is_some_and(|current| incoming < *current)
        }) {
            return Box::pin(async move {
                Err(CheckpointError::Save {
                    session_id,
                    message: Arc::from("configuration checkpoint superseded by a newer generation"),
                })
            });
        }
        let has_provisional = state.coordinator_pending.contains_key(&session_id);
        let (base, mut history_base) =
            if let Some(pending) = state.coordinator_pending.get(&session_id) {
                (Arc::clone(&pending.session), pending.history_base)
            } else if let Some(latest) = state.latest.get(&session_id) {
                (
                    Arc::clone(latest),
                    state.coordinator_history_bases.get(&session_id).copied(),
                )
            } else {
                return Box::pin(async move {
                    Err(CheckpointError::Save {
                        session_id,
                        message: Arc::from("session snapshot is unavailable"),
                    })
                });
            };
        if history_base.is_none() && request.snapshot.history.is_some() {
            history_base = Some(base.history_identity());
        }
        let live_ids = state.live_children.get(&session_id).map(Vec::as_slice);
        let mut merged = merge_checkpoint(&base, &request.snapshot, live_ids);
        if config_version.is_none() && state.config_versions.contains_key(&session_id) {
            merged.set_model(base.model.clone());
            merged.meta.fast = base.meta.fast;
            merged.meta.workflow = base.meta.workflow;
            merged.meta.thinking = base.meta.thinking;
            merged.meta.mode = base.meta.mode.clone();
            merged.meta.plan_path = base.meta.plan_path.clone();
        }
        let merged = Arc::new(merged);
        let generation = next_generation(&mut state);
        state.latest_generations.insert(session_id, generation);
        if let Some(config_version) = config_version {
            state.config_versions.insert(session_id, config_version);
            let authoritative = state
                .latest
                .get(&session_id)
                .map(|latest| {
                    Arc::new(merge_checkpoint(
                        latest,
                        &request.snapshot,
                        state.live_children.get(&session_id).map(Vec::as_slice),
                    ))
                })
                .unwrap_or_else(|| Arc::clone(&merged));
            state.latest.insert(session_id, authoritative);
            if !has_provisional && let Some(history_base) = history_base {
                state
                    .coordinator_history_bases
                    .insert(session_id, history_base);
            }
        }
        let provisional = config_version.is_none() || has_provisional;
        if provisional {
            state.coordinator_pending.insert(
                session_id,
                CoordinatorPending {
                    generation,
                    session: Arc::clone(&merged),
                    history_base,
                },
            );
        }
        let (reply, response) = flume::bounded(1);
        let retained_config_version = state.config_versions.get(&session_id).copied();
        let histories = request
            .snapshot
            .history
            .as_ref()
            .map(|messages| RequiredHistory::Root(Arc::clone(messages)))
            .into_iter()
            .collect();
        enqueue_locked(
            &mut state.entries,
            PendingSave {
                session: merged,
                waiters: vec![CheckpointWaiter {
                    version,
                    config_version,
                    histories,
                    reply,
                }],
                generation,
                coordinator_generation: provisional.then_some(generation),
                config_version: retained_config_version,
                coordinator_history_base: history_base,
                retry_attempt: 0,
                retry_at: None,
            },
        );
        drop(state);
        wake_checkpoint(&self.pending, &self.wake, session_id);
        Box::pin(async move {
            response
                .recv_async()
                .await
                .map_err(|_| CheckpointError::Closed(session_id))?
        })
    }
}

impl StorageWriter {
    pub fn new(dir: StateDir, warn_tx: flume::Sender<String>) -> Self {
        let pending: Pending = Arc::default();
        let writer_pending = Arc::clone(&pending);
        let (wake, wake_rx) = flume::unbounded::<()>();
        let (done_tx, done_rx) = flume::bounded::<()>(1);
        let stop: Arc<AtomicBool> = Arc::default();
        let writer_stop = Arc::clone(&stop);

        std::thread::Builder::new()
            .name("storage-writer".into())
            .spawn(move || {
                let mut writer = Writer {
                    dir,
                    warn_tx,
                    logs: HashMap::new(),
                    failing: HashSet::new(),
                };
                let mut retry_at: Option<Instant> = None;
                loop {
                    let wake = if let Some(deadline) = retry_at {
                        wake_rx.recv_timeout(deadline.saturating_duration_since(Instant::now()))
                    } else {
                        wake_rx
                            .recv()
                            .map_err(|_| flume::RecvTimeoutError::Disconnected)
                    };
                    if matches!(wake, Err(flume::RecvTimeoutError::Disconnected)) {
                        break;
                    }
                    retry_at = writer.flush(&writer_pending);
                    if writer_stop.load(Ordering::Acquire) {
                        break;
                    }
                }
                for entry in lock(&writer_pending).entries.values_mut() {
                    if let Entry::Save(save) = entry {
                        save.retry_at = None;
                    }
                }
                writer.flush(&writer_pending);
                let _ = done_tx.send(());
            })
            .expect("failed to spawn storage writer thread");

        Self {
            pending,
            wake,
            done_rx,
            stop,
        }
    }

    pub fn bind_config_runtime(&self, id: MakiId, runtime_epoch: u64) {
        let mut state = lock(&self.pending);
        if state
            .config_versions
            .get(&id)
            .is_some_and(|(epoch, _)| *epoch >= runtime_epoch)
        {
            return;
        }
        cancel_child_publications(&mut state, id);
        state.config_versions.insert(id, (runtime_epoch, 0));
        state.coordinator_pending.remove(&id);
        let generation = next_generation(&mut state);
        state.latest_generations.insert(id, generation);
        let latest = state.latest.get(&id).cloned();
        if let Some(Entry::Save(save)) = state.entries.get_mut(&id) {
            fail_waiters(id, mem::take(&mut save.waiters), CHECKPOINT_REPLACED);
            if let Some(latest) = latest {
                save.session = latest;
            }
            save.generation = generation;
            save.config_version = Some((runtime_epoch, 0));
            save.coordinator_generation = None;
            save.coordinator_history_base = None;
        }
    }

    pub fn seed(&self, session: Arc<AppSession>) {
        let id = session.id;
        let mut state = lock(&self.pending);
        cancel_child_publications(&mut state, id);
        let generation = next_generation(&mut state);
        state.latest.insert(id, session);
        state.latest_generations.insert(id, generation);
    }

    pub fn set_live_children(&self, session_id: MakiId, live_ids: Vec<String>) {
        let mut state = lock(&self.pending);
        if !state.closed {
            state.live_children.insert(session_id, live_ids);
        }
    }

    pub fn send(&self, session: Arc<AppSession>) {
        let id = session.id;
        let incoming_history = session.history_identity();
        let mut state = lock(&self.pending);
        if state.closed {
            return;
        }
        let preserve_history = state
            .coordinator_history_bases
            .get(&id)
            .is_some_and(|base| incoming_history == *base);
        if !preserve_history {
            state.coordinator_history_bases.remove(&id);
        }
        let authoritative = state
            .latest
            .get(&id)
            .map(|latest| {
                let mut merged = merge_tui_snapshot(
                    &session,
                    latest,
                    preserve_history,
                    state.live_children.get(&id).map(Vec::as_slice),
                );
                if state.config_versions.contains_key(&id)
                    && (merged.meta.mode != latest.meta.mode
                        || merged.meta.plan_path != latest.meta.plan_path)
                {
                    let merged = Arc::make_mut(&mut merged);
                    merged.meta.mode = latest.meta.mode.clone();
                    merged.meta.plan_path = latest.meta.plan_path.clone();
                }
                merged
            })
            .unwrap_or(session);
        let generation = next_generation(&mut state);
        state.latest.insert(id, Arc::clone(&authoritative));
        state.latest_generations.insert(id, generation);
        let coordinator_generation = state
            .coordinator_pending
            .contains_key(&id)
            .then_some(generation);
        let live_ids = state.live_children.get(&id).cloned();
        let (session, coordinator_history_base) = state
            .coordinator_pending
            .get_mut(&id)
            .map(|pending| {
                if pending
                    .history_base
                    .is_some_and(|base| base != authoritative.history_identity())
                {
                    pending.history_base = None;
                }
                let merged = merge_tui_snapshot(
                    &authoritative,
                    &pending.session,
                    pending.history_base.is_some(),
                    live_ids.as_deref(),
                );
                pending.generation = coordinator_generation.unwrap();
                pending.session = Arc::clone(&merged);
                (merged, pending.history_base)
            })
            .unwrap_or((authoritative, None));
        let config_version = state.config_versions.get(&id).copied();
        enqueue_locked(
            &mut state.entries,
            PendingSave {
                session,
                waiters: Vec::new(),
                config_version,
                generation,
                coordinator_generation,
                coordinator_history_base,
                retry_attempt: 0,
                retry_at: None,
            },
        );
        drop(state);
        wake_checkpoint(&self.pending, &self.wake, id);
    }

    pub fn checkpoint_child_history(
        &self,
        session_id: MakiId,
        child_key: String,
        history: HistorySnapshot<Message>,
    ) -> CheckpointFuture {
        let mut state = lock(&self.pending);
        if state.closed {
            return Box::pin(async move { Err(CheckpointError::Closed(session_id)) });
        }
        let Some(base) = state
            .coordinator_pending
            .get(&session_id)
            .map(|pending| Arc::clone(&pending.session))
            .or_else(|| state.latest.get(&session_id).cloned())
        else {
            return Box::pin(async move {
                Err(CheckpointError::Save {
                    session_id,
                    message: Arc::from("session snapshot is unavailable"),
                })
            });
        };
        let mut candidate = base.as_ref().clone();
        if !candidate.adopt_subagent_history(child_key.clone(), &history) {
            return Box::pin(async move {
                Err(CheckpointError::Save {
                    session_id,
                    message: Arc::from(CHECKPOINT_REPLACED),
                })
            });
        }
        candidate.prune_posts(Some(&child_key), history.messages.len());
        let candidate = Arc::new(candidate);
        let generation = next_generation(&mut state);
        let history_base = state
            .coordinator_pending
            .get(&session_id)
            .and_then(|pending| pending.history_base);
        state.coordinator_pending.insert(
            session_id,
            CoordinatorPending {
                generation,
                session: Arc::clone(&candidate),
                history_base,
            },
        );
        state.latest_generations.insert(session_id, generation);
        let version = CheckpointVersion {
            revision: generation,
            epoch: candidate.history_snapshot().epoch,
        };
        let config_version = state.config_versions.get(&session_id).copied();
        let (reply, response) = flume::bounded(1);
        enqueue_locked(
            &mut state.entries,
            PendingSave {
                session: candidate,
                waiters: vec![CheckpointWaiter {
                    version,
                    config_version: None,
                    histories: vec![RequiredHistory::Child {
                        key: child_key,
                        history,
                    }],
                    reply,
                }],
                generation,
                coordinator_generation: Some(generation),
                config_version,
                coordinator_history_base: history_base,
                retry_attempt: 0,
                retry_at: None,
            },
        );
        drop(state);
        wake_checkpoint(&self.pending, &self.wake, session_id);
        Box::pin(async move {
            response
                .recv_async()
                .await
                .map_err(|_| CheckpointError::Closed(session_id))?
        })
    }

    pub async fn checkpoint_child_approval(
        &self,
        session_id: MakiId,
        child_key: String,
        history: HistorySnapshot<Message>,
        publication: CheckpointPublication,
        guard: SessionPublicationGuard,
    ) -> Result<(), String> {
        let (reply, response) = flume::bounded(1);
        {
            let mut state = lock(&self.pending);
            if state.closed {
                return Err(CheckpointError::Closed(session_id).to_string());
            }
            let incarnation = Arc::clone(state.incarnations.entry(session_id).or_default());
            state
                .child_publications
                .entry(session_id)
                .or_default()
                .push(publication.clone());
            state.child_approvals.push(ChildApprovalSave {
                session_id,
                child_key,
                history,
                publication,
                guard,
                incarnation,
                reply,
            });
        }
        self.wake.send(()).map_err(|error| error.to_string())?;
        response
            .recv_async()
            .await
            .map_err(|error| error.to_string())?
    }

    pub fn coordinator_checkpoint(&self) -> Arc<dyn CheckpointWriter<SessionCheckpoint>> {
        Arc::new(CoordinatorCheckpointWriter {
            pending: Arc::clone(&self.pending),
            wake: self.wake.clone(),
        })
    }

    #[cfg(test)]
    pub(crate) fn latest_snapshot(&self, id: MakiId) -> Option<Arc<AppSession>> {
        lock(&self.pending).latest.get(&id).cloned()
    }

    fn enqueue_checkpoint(&self, request: CheckpointRequest<AppSession>) -> CheckpointFuture {
        let session_id = request.session_id;
        let version = request.version;
        let (reply, response) = flume::bounded(1);
        let mut state = lock(&self.pending);
        if state.closed {
            return Box::pin(async move { Err(CheckpointError::Closed(session_id)) });
        }
        let generation = next_generation(&mut state);
        let config_version = state.config_versions.get(&session_id).copied();
        let histories = required_histories(&request.snapshot);
        let snapshot = if config_version.is_some() {
            state
                .latest
                .get(&session_id)
                .map(|latest| {
                    let mut merged = merge_tui_snapshot(&request.snapshot, latest, false, None);
                    if merged.meta.mode != latest.meta.mode
                        || merged.meta.plan_path != latest.meta.plan_path
                    {
                        let merged = Arc::make_mut(&mut merged);
                        merged.meta.mode = latest.meta.mode.clone();
                        merged.meta.plan_path = latest.meta.plan_path.clone();
                    }
                    merged
                })
                .unwrap_or(request.snapshot)
        } else {
            request.snapshot
        };
        state.latest.insert(session_id, Arc::clone(&snapshot));
        state.latest_generations.insert(session_id, generation);
        enqueue_locked(
            &mut state.entries,
            PendingSave {
                session: snapshot,
                config_version,
                waiters: vec![CheckpointWaiter {
                    version,
                    config_version: None,
                    histories,
                    reply,
                }],
                generation,
                coordinator_generation: None,
                coordinator_history_base: None,
                retry_attempt: 0,
                retry_at: None,
            },
        );
        drop(state);
        wake_checkpoint(&self.pending, &self.wake, session_id);
        Box::pin(async move {
            response
                .recv_async()
                .await
                .map_err(|_| CheckpointError::Closed(session_id))?
        })
    }

    /// Drops an in-memory snapshot kept alive by `delete_empty`. A session
    /// cleaned up as empty is never deleted through the picker, so without
    /// this its snapshot outlives it: `/new` on an untouched session would
    /// leave one behind every time.
    pub fn forget(&self, id: MakiId) {
        let mut state = lock(&self.pending);
        cancel_child_publications(&mut state, id);
        state.latest.remove(&id);
        state.latest_generations.remove(&id);
        state.coordinator_pending.remove(&id);
        state.coordinator_history_bases.remove(&id);
        state.config_versions.remove(&id);
        state.live_children.remove(&id);
    }

    /// Removes an empty session's files while keeping its in-memory snapshot.
    /// The session is still live in its tab, and its coordinator checkpoints by
    /// merging into that snapshot, so forgetting it would leave the next
    /// checkpoint with no base to merge into. The files come back if the
    /// coordinator later has something worth saving.
    pub fn delete_empty(&self, id: MakiId) {
        self.delete_inner(id, |_| {}, false);
    }

    /// Delete a session's files on the writer thread; `done` fires there, so
    /// callers never block on disk. Deleting a session that was never written
    /// reports success, and a save enqueued afterwards supersedes the delete.
    /// The session is forgotten entirely: this is the user asking for it to be
    /// gone, not the cleanup of a session that has not earned a file yet.
    #[cfg(test)]
    pub fn delete(&self, id: MakiId, done: impl FnOnce(Result<(), SessionError>) + Send + 'static) {
        self.delete_inner(id, done, true);
    }

    fn delete_inner(
        &self,
        id: MakiId,
        done: impl FnOnce(Result<(), SessionError>) + Send + 'static,
        forget_snapshot: bool,
    ) {
        self.delete_inner_guarded(id, done, forget_snapshot, None, None);
    }

    fn delete_inner_guarded(
        &self,
        id: MakiId,
        done: impl FnOnce(Result<(), SessionError>) + Send + 'static,
        forget_snapshot: bool,
        guard: Option<SessionPublicationGuard>,
        store: Option<Arc<OffloadStore>>,
    ) {
        let mut state = lock(&self.pending);
        if state.closed {
            drop(state);
            done(Err(writer_gone()));
            return;
        }
        cancel_child_publications(&mut state, id);
        if forget_snapshot {
            state.latest.remove(&id);
            state.latest_generations.remove(&id);
            state.coordinator_pending.remove(&id);
            state.coordinator_history_bases.remove(&id);
            state.config_versions.remove(&id);
            state.live_children.remove(&id);
        }
        let replaced = state
            .entries
            .insert(id, Entry::Delete(Box::new(done), guard, store));
        drop(state);
        if let Some(Entry::Save(save)) = replaced {
            discard_coordinator_candidate(&self.pending, id, &save);
            fail_waiters(
                id,
                save.waiters,
                "checkpoint superseded by session deletion",
            );
        }
        if self.wake.send(()).is_err()
            && let Some(entry) = lock(&self.pending).entries.remove(&id)
        {
            match entry {
                Entry::Delete(done, _, _) => done(Err(writer_gone())),
                Entry::Save(save) => fail_waiters(id, save.waiters, "storage writer unavailable"),
            }
        }
    }

    pub fn delete_guarded(
        &self,
        id: MakiId,
        guard: SessionPublicationGuard,
        store: Option<Arc<OffloadStore>>,
        done: impl FnOnce(Result<(), SessionError>) + Send + 'static,
    ) {
        self.delete_inner_guarded(id, done, true, Some(guard), store);
    }

    pub fn shutdown(&self, timeout: Duration) {
        {
            let mut state = lock(&self.pending);
            state.closed = true;
            for publications in state.child_publications.values() {
                for publication in publications {
                    publication.cancel();
                }
            }
            self.stop.store(true, Ordering::Release);
        }
        // Wake it so it observes the flag; the final flush still runs.
        let _ = self.wake.send(());
        if self.done_rx.recv_timeout(timeout).is_err() {
            warn!("storage writer did not drain within {timeout:?}");
        }
    }
}

fn lock(pending: &Pending) -> std::sync::MutexGuard<'_, PendingState> {
    pending.lock().unwrap_or_else(|e| e.into_inner())
}

fn cancel_child_publications(state: &mut PendingState, id: MakiId) {
    state.incarnations.insert(id, Arc::new(()));
    if let Some(publications) = state.child_publications.remove(&id) {
        for publication in publications {
            publication.cancel();
        }
    }
}

fn next_generation(state: &mut PendingState) -> u64 {
    let generation = state.next_generation;
    state.next_generation = generation.wrapping_add(1);
    generation
}

fn enqueue_locked(entries: &mut HashMap<MakiId, Entry>, mut save: PendingSave) {
    let id = save.session.id;
    if let Some(Entry::Save(mut previous)) = entries.remove(&id) {
        retain_history_waiters(id, &save.session, &mut previous.waiters);
        if !previous.waiters.is_empty() {
            save.retry_attempt = previous.retry_attempt;
            save.retry_at = previous.retry_at;
        }
        save.waiters.extend(previous.waiters);
    }
    entries.insert(id, Entry::Save(save));
}

fn wake_checkpoint(pending: &Pending, wake: &flume::Sender<()>, id: MakiId) {
    if wake.send(()).is_err()
        && let Some(Entry::Save(save)) = lock(pending).entries.remove(&id)
    {
        discard_coordinator_candidate(pending, id, &save);
        fail_waiters(id, save.waiters, "storage writer unavailable");
    }
}

fn commit_save(pending: &Pending, id: MakiId, save: &PendingSave) {
    let Some(generation) = save.coordinator_generation else {
        return;
    };
    let mut state = lock(pending);
    let Some(latest) = state.latest.get(&id) else {
        return;
    };
    let preserve_root = save.coordinator_history_base.is_some_and(|base| {
        latest.history_identity() == base
            || Arc::ptr_eq(
                &latest.history_snapshot().messages,
                &save.session.history_snapshot().messages,
            )
    });
    let live_ids = state.live_children.get(&id).map(Vec::as_slice);
    let mut authoritative = if state.config_versions.get(&id).copied() == save.config_version {
        merge_tui_snapshot(latest, &save.session, false, live_ids)
    } else {
        Arc::clone(latest)
    };
    preserve_newer_child_histories(&mut authoritative, &save.session);
    if preserve_root {
        let authoritative = Arc::make_mut(&mut authoritative);
        authoritative.adopt_history(&save.session.history_snapshot());
        prune_main_history(authoritative, live_ids);
    }
    let committed_base = preserve_root
        .then_some(save.coordinator_history_base)
        .flatten();
    state.latest.insert(id, Arc::clone(&authoritative));
    if let Some(history_base) = committed_base {
        state
            .coordinator_history_bases
            .entry(id)
            .or_insert(history_base);
    }
    let candidate_committed = state.coordinator_pending.get(&id).is_some_and(|candidate| {
        candidate.generation == generation
            || (same_histories(&candidate.session, &authoritative)
                && same_coordinator_metadata(&candidate.session, &authoritative))
    });
    if candidate_committed {
        state.coordinator_pending.remove(&id);
        if let Some(Entry::Save(queued)) = state.entries.get_mut(&id) {
            queued.coordinator_generation = None;
            queued.coordinator_history_base = committed_base;
        }
    } else if let Some(candidate) = state.coordinator_pending.get_mut(&id) {
        if candidate.history_base == save.coordinator_history_base && preserve_root {
            candidate.history_base = Some(authoritative.history_identity());
        }
        let candidate_generation = candidate.generation;
        let history_base = candidate.history_base;
        let candidate_session = Arc::clone(&candidate.session);
        if let Some(Entry::Save(queued)) = state.entries.get_mut(&id)
            && queued.coordinator_generation == Some(candidate_generation)
        {
            queued.coordinator_history_base = history_base;
            queued.session = candidate_session;
        }
    }
}

fn same_histories(left: &AppSession, right: &AppSession) -> bool {
    left.subagent_messages().len() == right.subagent_messages().len()
        && required_histories(left)
            .iter()
            .all(|history| history.retained_by(right))
}

fn discard_coordinator_candidate(pending: &Pending, id: MakiId, save: &PendingSave) {
    let Some(generation) = save.coordinator_generation else {
        return;
    };
    let mut state = lock(pending);
    if state
        .coordinator_pending
        .get(&id)
        .is_some_and(|pending| pending.generation == generation)
    {
        state.coordinator_pending.remove(&id);
    }
}

fn requeue_save(
    pending: &Pending,
    id: MakiId,
    mut save: PendingSave,
) -> Result<(), Vec<CheckpointWaiter>> {
    let mut state = lock(pending);
    let current_config = state.config_versions.get(&id).copied();
    let history_retained = save
        .config_version
        .zip(current_config)
        .is_some_and(|(saved, current)| saved.0 == current.0)
        && matches!(
            state.entries.get(&id),
            Some(Entry::Save(newer))
                if newer.session.history_identity() == save.session.history_identity()
        );
    if current_config.is_some_and(|version| Some(version) != save.config_version)
        && !history_retained
    {
        if let Some(latest) = state.latest.get(&id) {
            fail_waiters(id, mem::take(&mut save.waiters), CHECKPOINT_REPLACED);
            save.session = Arc::clone(latest);
            save.generation = state.latest_generations[&id];
            save.config_version = state.config_versions.get(&id).copied();
            save.coordinator_generation = None;
            save.coordinator_history_base = None;
        } else {
            return Err(save.waiters);
        }
    }
    match state.entries.remove(&id) {
        Some(Entry::Save(mut newer)) => {
            retain_history_waiters(id, &newer.session, &mut save.waiters);
            if !save.waiters.is_empty() {
                newer.retry_attempt = newer.retry_attempt.max(save.retry_attempt);
                newer.retry_at = save.retry_at;
            }
            newer.waiters.extend(save.waiters);
            state.entries.insert(id, Entry::Save(newer));
            Ok(())
        }
        Some(Entry::Delete(done, guard, store)) => {
            state.entries.insert(id, Entry::Delete(done, guard, store));
            Err(save.waiters)
        }
        None => {
            state.entries.insert(id, Entry::Save(save));
            Ok(())
        }
    }
}

fn preserve_newer_child_histories(incoming: &mut Arc<AppSession>, latest: &AppSession) {
    for key in latest.subagent_messages().keys() {
        let Some(history) = latest.subagent_history_snapshot(key) else {
            continue;
        };
        if incoming
            .subagent_history_snapshot(key)
            .is_none_or(|incoming| incoming.epoch < history.epoch)
        {
            let incoming = Arc::make_mut(incoming);
            incoming.adopt_subagent_history(key.clone(), &history);
            incoming.prune_posts(Some(key), history.messages.len());
        }
    }
}

fn promote_child_approval(session: &mut Arc<AppSession>, approval: &ChildApprovalSave) {
    let current = session.subagent_history_snapshot(&approval.child_key);
    if current.is_none_or(|history| history.epoch <= approval.history.epoch) {
        let session = Arc::make_mut(session);
        session.adopt_subagent_history(approval.child_key.clone(), &approval.history);
        session.prune_posts(Some(&approval.child_key), approval.history.messages.len());
    }
}

fn merge_tui_snapshot(
    incoming: &Arc<AppSession>,
    latest: &AppSession,
    preserve_history: bool,
    live_ids: Option<&[String]>,
) -> Arc<AppSession> {
    let mut session = Arc::clone(incoming);
    preserve_newer_child_histories(&mut session, latest);
    if preserve_history && session.history_identity() != latest.history_identity() {
        let session = Arc::make_mut(&mut session);
        session.adopt_history(&latest.history_snapshot());
        prune_main_history(session, live_ids);
    }
    if !same_coordinator_metadata(&session, latest) {
        let session = Arc::make_mut(&mut session);
        session.set_model(latest.model.clone());
        session.set_cwd(latest.cwd.clone());
        session.meta.yolo = latest.meta.yolo;
        session.meta.fast = latest.meta.fast;
        session.meta.workflow = latest.meta.workflow;
        session.meta.thinking = latest.meta.thinking;
        session.meta.session_options = latest.meta.session_options.clone();
    }
    session
}

fn same_coordinator_metadata(left: &AppSession, right: &AppSession) -> bool {
    left.model == right.model
        && left.cwd == right.cwd
        && left.meta.yolo == right.meta.yolo
        && left.meta.fast == right.meta.fast
        && left.meta.workflow == right.meta.workflow
        && left.meta.thinking == right.meta.thinking
        && left.meta.session_options == right.meta.session_options
}

fn prune_main_history(session: &mut AppSession, live_ids: Option<&[String]>) {
    let preserved;
    let live_ids = match live_ids {
        Some(ids) => ids,
        None => {
            preserved = session
                .subagent_messages()
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            &preserved
        }
    };
    session.prune_orphans_preserving(
        |message| {
            message
                .tool_uses()
                .map(|(id, _, _)| id.to_owned())
                .collect()
        },
        live_ids,
    );
}

fn merge_checkpoint(
    base: &AppSession,
    checkpoint: &SessionCheckpoint,
    live_ids: Option<&[String]>,
) -> AppSession {
    let mut session = base.clone();
    // Only a history replacement carries messages. An option or model change
    // leaves them alone: while a turn holds the lease the coordinator's copy
    // is the pre-turn one, and writing it would rewind the stored session to
    // before the running turn.
    if let Some(history) = &checkpoint.history {
        session.replace_shared_messages(Arc::clone(history));
        prune_main_history(&mut session, live_ids);
    }
    if let Some(projection) = &checkpoint.config {
        session.meta.mode = Some(match &projection.config.mode {
            AgentMode::Build => StoredMode::Build,
            AgentMode::Plan(_) => StoredMode::Plan,
            AgentMode::Custom(mode) => StoredMode::Custom(mode.to_string()),
        });
        session.meta.plan_path = match &projection.config.mode {
            AgentMode::Plan(path) => Some(path.to_string_lossy().into_owned()),
            _ => None,
        };
    }
    session.set_model(checkpoint.model.to_string());
    session.set_cwd(checkpoint.cwd.to_string_lossy().into_owned());
    session.meta.yolo = Some(option_enabled(&checkpoint.options, YOLO_OPTION_ID));
    session.meta.fast = option_enabled(&checkpoint.options, FAST_OPTION_ID);
    session.meta.workflow = option_enabled(&checkpoint.options, WORKFLOW_OPTION_ID);
    session.meta.thinking = checkpoint
        .options
        .options
        .iter()
        .find(|state| state.definition.id.as_ref() == THINKING_OPTION_ID)
        .and_then(|state| state.current_value.parse::<ThinkingConfig>().ok())
        .map(Into::into);
    session.meta.session_options = checkpoint
        .options
        .options
        .iter()
        .filter(|state| {
            state.definition.persistent
                && matches!(state.definition.owner, SessionOptionOwner::Plugin { .. })
        })
        .map(|state| {
            (
                state.definition.id.to_string(),
                state.current_value.to_string(),
            )
        })
        .collect();
    session
}

fn option_enabled(options: &maki_agent::session_options::SessionOptionsSnapshot, id: &str) -> bool {
    options
        .options
        .iter()
        .find(|state| state.definition.id.as_ref() == id)
        .is_some_and(|state| state.current_value.as_ref() == ENABLED_VALUE)
}

fn writer_gone() -> SessionError {
    StorageError::Io(io::Error::other("storage writer unavailable")).into()
}

fn retain_history_waiters(id: MakiId, session: &AppSession, waiters: &mut Vec<CheckpointWaiter>) {
    waiters.retain(|waiter| {
        let retained = waiter
            .histories
            .iter()
            .all(|history| history.retained_by(session));
        if !retained {
            let _ = waiter.reply.send(Err(CheckpointError::Save {
                session_id: id,
                message: Arc::from(CHECKPOINT_REPLACED),
            }));
        }
        retained
    });
}

fn acknowledge_waiters(
    pending: &Pending,
    session: &AppSession,
    mut waiters: Vec<CheckpointWaiter>,
) {
    let id = session.id;
    retain_history_waiters(id, session, &mut waiters);
    let state = lock(pending);
    for waiter in waiters {
        if waiter
            .config_version
            .is_some_and(|version| state.config_versions.get(&id) != Some(&version))
        {
            fail_waiters(
                id,
                vec![waiter],
                "configuration checkpoint superseded by a newer generation",
            );
        } else {
            let _ = waiter.reply.send(Ok(CheckpointAck {
                session_id: id,
                version: waiter.version,
            }));
        }
    }
}

fn fail_waiters(id: MakiId, waiters: Vec<CheckpointWaiter>, message: &str) {
    for waiter in waiters {
        let _ = waiter.reply.send(Err(CheckpointError::Save {
            session_id: id,
            message: Arc::from(message),
        }));
    }
}

/// Everything the writer thread owns. It never leaves that thread, so nothing
/// here needs a lock.
struct Writer {
    dir: StateDir,
    warn_tx: flume::Sender<String>,
    /// Only cursors that still describe their file.
    logs: HashMap<MakiId, SessionLog>,
    /// Sessions whose last write failed, so a sick disk warns once instead of
    /// once per frame.
    failing: HashSet<MakiId>,
}

impl Writer {
    fn forget(&mut self, id: MakiId) {
        self.logs.remove(&id);
        self.failing.remove(&id);
    }

    fn flush(&mut self, pending: &Pending) -> Option<Instant> {
        // Bound first: a `for` head temporary lives for the whole loop, so
        // iterating the guard directly would deadlock the re-insert below.
        let batch = mem::take(&mut lock(pending).entries);
        for (id, entry) in batch {
            match entry {
                Entry::Save(mut save) => {
                    if save
                        .retry_at
                        .is_some_and(|retry_at| retry_at > Instant::now())
                    {
                        if let Err(waiters) = requeue_save(pending, id, save) {
                            fail_waiters(id, waiters, "checkpoint superseded by session deletion");
                        }
                        continue;
                    }
                    save.retry_at = None;
                    match self.write(&save.session) {
                        Ok(()) => {
                            commit_save(pending, id, &save);
                            self.report_save(pending, &save, Ok(()));
                            acknowledge_waiters(pending, &save.session, save.waiters);
                        }
                        Err(error) => {
                            let message = error.to_string();
                            self.report_save(pending, &save, Err(&message));
                            if !save.waiters.is_empty()
                                && save.retry_attempt < CHECKPOINT_RETRY_BACKOFFS.len()
                            {
                                save.retry_at = Some(
                                    Instant::now() + CHECKPOINT_RETRY_BACKOFFS[save.retry_attempt],
                                );
                                save.retry_attempt += 1;
                                if let Err(waiters) = requeue_save(pending, id, save) {
                                    fail_waiters(
                                        id,
                                        waiters,
                                        "checkpoint superseded by session deletion",
                                    );
                                }
                                continue;
                            }

                            let mut waiters = mem::take(&mut save.waiters);
                            if save.coordinator_generation.is_some() {
                                discard_coordinator_candidate(pending, id, &save);
                                fail_waiters(id, mem::take(&mut waiters), &message);
                                let state = lock(pending);
                                if save.config_version.is_none() || !state.latest.contains_key(&id)
                                {
                                    continue;
                                }
                                save.session = Arc::clone(&state.latest[&id]);
                                save.generation = state.latest_generations[&id];
                                save.config_version = state.config_versions.get(&id).copied();
                                save.coordinator_generation = None;
                                save.coordinator_history_base = None;
                            }
                            if !waiters.is_empty() {
                                save.retry_attempt = 0;
                            }
                            let backoff = BACKGROUND_RETRY_BACKOFFS
                                [save.retry_attempt.min(BACKGROUND_RETRY_BACKOFFS.len() - 1)];
                            save.retry_attempt = save.retry_attempt.saturating_add(1);
                            save.retry_at = Some(Instant::now() + backoff);
                            let replaced_by_delete = requeue_save(pending, id, save).is_err();
                            if !waiters.is_empty() {
                                fail_waiters(
                                    id,
                                    waiters,
                                    if replaced_by_delete {
                                        "checkpoint superseded by session deletion"
                                    } else {
                                        &message
                                    },
                                );
                            }
                        }
                    }
                }
                Entry::Delete(done, guard, store) => {
                    self.forget(id);
                    let result = match guard {
                        Some(guard) => guard
                            .publish(|| {
                                let result = AppSession::delete(id, &self.dir);
                                if (result.is_ok()
                                    || matches!(result, Err(SessionError::Storage(StorageError::NotFound(_)))))
                                    && let Some(store) = store
                                    && let Err(error) = store.close_and_remove()
                                {
                                    warn!(%error, session_id = %id, "offload cleanup failed after session deletion");
                                }
                                result
                            })
                            .map_err(StorageError::from)
                            .map_err(SessionError::from)
                            .and_then(|result| result.unwrap_or(Err(SessionError::OpenElsewhere))),
                        None => AppSession::delete(id, &self.dir),
                    };
                    done(match result {
                        Err(SessionError::Storage(StorageError::NotFound(_))) => Ok(()),
                        result => result,
                    });
                }
            }
        }
        let approvals = mem::take(&mut lock(pending).child_approvals);
        for approval in approvals {
            let result = self.write_child_approval(pending, &approval);
            if let Some(publications) = lock(pending)
                .child_publications
                .get_mut(&approval.session_id)
            {
                publications.retain(|publication| !publication.same_request(&approval.publication));
            }
            let _ = approval.reply.send(result);
        }
        lock(pending)
            .entries
            .values()
            .filter_map(|entry| match entry {
                Entry::Save(save) => save.retry_at,
                Entry::Delete(_, _, _) => None,
            })
            .min()
    }

    fn write_child_approval(
        &mut self,
        pending: &Pending,
        approval: &ChildApprovalSave,
    ) -> Result<(), String> {
        if approval.publication.is_cancelled() {
            return Err(CHECKPOINT_CANCELLED.into());
        }
        let mut candidate = {
            let state = lock(pending);
            if state.closed {
                return Err(CheckpointError::Closed(approval.session_id).to_string());
            }
            state
                .latest
                .get(&approval.session_id)
                .cloned()
                .ok_or_else(|| CHECKPOINT_REPLACED.to_owned())?
        };
        let session = Arc::make_mut(&mut candidate);
        if !session.adopt_subagent_history(approval.child_key.clone(), &approval.history) {
            return Err(CHECKPOINT_REPLACED.into());
        }
        session.prune_posts(Some(&approval.child_key), approval.history.messages.len());
        let directory = self
            .dir
            .ensure_subdir(SESSIONS_DIR)
            .map_err(|error| error.to_string())?;
        let staged = SessionLog::stage_child_history(&directory, session)
            .map_err(|error| error.to_string())?;
        #[cfg(test)]
        {
            let gate = lock(pending).child_stage_gate.clone();
            if let Some((entered, release)) = gate {
                let _ = entered.send(());
                let _ = release.recv();
            }
        }
        let published = staged
            .publish(&approval.guard, || {
                let state = lock(pending);
                !state.closed
                    && state
                        .incarnations
                        .get(&approval.session_id)
                        .is_some_and(|incarnation| Arc::ptr_eq(incarnation, &approval.incarnation))
                    && approval.publication.try_publish()
            })
            .map_err(|error| error.to_string())?;
        if !published {
            return Err(CHECKPOINT_CANCELLED.into());
        }
        self.logs.remove(&approval.session_id);
        let mut state = lock(pending);
        if !state
            .incarnations
            .get(&approval.session_id)
            .is_some_and(|incarnation| Arc::ptr_eq(incarnation, &approval.incarnation))
        {
            return Ok(());
        }
        if let Some(latest) = state.latest.get_mut(&approval.session_id) {
            promote_child_approval(latest, approval);
        }
        if let Some(provisional) = state.coordinator_pending.get_mut(&approval.session_id) {
            promote_child_approval(&mut provisional.session, approval);
        }
        if let Some(Entry::Save(queued)) = state.entries.get_mut(&approval.session_id) {
            promote_child_approval(&mut queued.session, approval);
        }
        Ok(())
    }

    fn write(&mut self, session: &AppSession) -> Result<(), SessionError> {
        let sessions_dir = self.dir.ensure_subdir(SESSIONS_DIR)?;
        if let Some(mut log) = self.logs.remove(&session.id) {
            // A failed `append` rolls the file back to the last record boundary,
            // so the cursor still fits and is worth keeping: rebuilding it costs
            // a second full write, the last thing a failing disk needs.
            // Divergence is the one answer a cursor cannot survive.
            let appended = log.append(session);
            if !matches!(appended, Err(SessionError::LogDiverged { .. })) {
                self.logs.insert(session.id, log);
                return appended;
            }
        }
        // No usable cursor, whether because this thread never wrote the file
        // or because the log diverged, so the file starts over. Reading the
        // old file back gains nothing: a cursor recovered from disk describes
        // the session that was stored, never the live one.
        self.logs
            .insert(session.id, SessionLog::rewrite(&sessions_dir, session)?);
        Ok(())
    }

    fn report_save(&mut self, pending: &Pending, save: &PendingSave, result: Result<(), &str>) {
        let id = save.session.id;
        let state = lock(pending);
        let current = (save.config_version.is_none() && !state.config_versions.contains_key(&id))
            || (state.latest_generations.get(&id) == Some(&save.generation)
                && state.config_versions.get(&id) == save.config_version.as_ref());
        if !current {
            return;
        }
        match (result, save.config_version) {
            (Err(message), Some((epoch, generation))) => self.report(id, Err(format!(
                "Configuration applied but not saved (runtime {epoch}, generation {generation}): {message}"
            ))),
            (result, _) => self.report(id, result),
        }
    }

    fn report(&mut self, id: MakiId, result: Result<(), impl std::fmt::Display>) {
        match result {
            Ok(()) => {
                if self.failing.remove(&id) {
                    let _ = self.warn_tx.send(SAVE_RECOVERED.to_string());
                }
            }
            Err(e) => {
                warn!(error = %e, %id, "session write failed");
                if self.failing.insert(id) {
                    let _ = self.warn_tx.send(format!("{SAVE_FAILED_PREFIX}: {e}"));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use maki_agent::actor::EffectiveAgentConfig;
    use maki_agent::session_coordinator::CommittedConfigProjection;
    use maki_agent::session_coordinator::builtin_option_definitions;
    use maki_agent::session_options::SessionOptions;
    use maki_agent::{RunSettings, ToolOutput};
    use maki_domain::{Post, PostLevel};
    use maki_providers::provider::{BoxFuture, Provider};
    use maki_providers::{
        AgentError, Model, ModelInfo, ProviderEvent, RequestOptions, StreamResponse,
    };
    use maki_providers::{Effort, Message};
    use maki_storage::id::SessionRef;
    use maki_storage::session_lock;
    use maki_storage::sessions::{StoredMode, StoredPost, StoredThinking};
    use tempfile::TempDir;
    use test_case::test_case;

    const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
    const DEFERRED_RETRY: Duration = Duration::from_secs(3600);
    const MODEL: &str = "test-model";
    const FAILED_MODEL: &str = "failed-model";
    const CWD: &str = "/tmp/writer";
    const MSG_PREFIX: &str = "msg-";
    const RESUMED_MSG: &str = "resumed";
    const TOOL_ID: &str = "tool-1";
    const SIBLING_AGENT_ID: &str = "sibling";
    const TOOL_TEXT: &str = "tool output";
    const TITLE: &str = "renamed after reload";
    const BASE_MSG: &str = "base";
    const COORDINATOR_MSG: &str = "coordinator";
    const REWOUND_MSG: &str = "rewound";
    const POST_TEXT: &str = "checkpoint post";
    const LIVE_CHILD: &str = "live-child";
    const ORPHAN_CHILD: &str = "orphan-child";

    struct ConfigTestProvider;

    impl Provider for ConfigTestProvider {
        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            _: &'a [Message],
            _: &'a str,
            _: &'a serde_json::Value,
            _: &'a flume::Sender<ProviderEvent>,
            _: RequestOptions,
            _: Option<&'a SessionRef>,
        ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
            Box::pin(std::future::pending())
        }

        fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    fn config_checkpoint(epoch: u64, generation: u64) -> SessionCheckpoint {
        SessionCheckpoint {
            history: None,
            model: Arc::from(FAILED_MODEL),
            cwd: CWD.into(),
            options: SessionOptions::new(
                builtin_option_definitions(
                    FAILED_MODEL,
                    [Arc::from(FAILED_MODEL)],
                    false,
                    true,
                    true,
                    ThinkingConfig::Off,
                ),
                &Default::default(),
            )
            .unwrap()
            .snapshot(),
            config: Some(CommittedConfigProjection {
                runtime: Arc::new(()),
                runtime_epoch: epoch,
                generation,
                config: Arc::new(EffectiveAgentConfig::new(
                    RunSettings {
                        provider: Arc::new(ConfigTestProvider),
                        model: crate::components::test_model(),
                        fast: true,
                        workflow: true,
                        thinking: ThinkingConfig::Off,
                    },
                    AgentMode::Plan(CWD.into()),
                )),
            }),
        }
    }

    fn manual_writer() -> (StorageWriter, flume::Receiver<()>) {
        let (wake, wake_rx) = flume::unbounded();
        let (_, done_rx) = flume::bounded(1);
        (
            StorageWriter {
                pending: Arc::default(),
                wake,
                done_rx,
                stop: Arc::default(),
            },
            wake_rx,
        )
    }

    #[test]
    fn committed_config_survives_failure_and_retries_with_lease_history() {
        let (_tmp, dir) = state_dir();
        block_sessions_dir(&dir);
        let (writer, _wake_rx) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        session.push_message(Message::user(BASE_MSG.into()));
        let id = session.id;
        writer.seed(Arc::new(session.clone()));
        let checkpoint = writer
            .coordinator_checkpoint()
            .checkpoint(CheckpointRequest {
                session_id: id,
                version: CheckpointVersion {
                    revision: 1,
                    epoch: 1,
                },
                snapshot: Arc::new(config_checkpoint(1, 1)),
            });
        let (warn_tx, warn_rx) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        for _ in 0..=CHECKPOINT_RETRY_BACKOFFS.len() {
            if let Some(Entry::Save(save)) = lock(&writer.pending).entries.get_mut(&id) {
                save.retry_at = None;
            }
            disk.flush(&writer.pending);
        }
        assert!(smol::block_on(checkpoint).is_err());
        assert_eq!(writer.latest_snapshot(id).unwrap().model, FAILED_MODEL);
        let warning = warn_rx.try_recv().unwrap();
        assert!(warning.contains("Configuration applied but not saved (runtime 1, generation 1)"));
        assert!(warn_rx.try_recv().is_err());
        std::fs::remove_file(dir.path().join(SESSIONS_DIR)).unwrap();
        if let Some(Entry::Save(save)) = lock(&writer.pending).entries.get_mut(&id) {
            save.retry_at = None;
        }
        disk.flush(&writer.pending);
        let recovered = AppSession::load(id, &dir).unwrap();
        assert_eq!(recovered.model, FAILED_MODEL);
        assert_eq!(message_texts(&recovered), [BASE_MSG]);
        session.push_message(Message::user(COORDINATOR_MSG.into()));
        writer.send(Arc::new(session));
        disk.flush(&writer.pending);
        let stored = AppSession::load(id, &dir).unwrap();
        assert_eq!(stored.model, FAILED_MODEL);
        assert_eq!(stored.meta.mode, Some(StoredMode::Plan));
        assert_eq!(stored.meta.plan_path.as_deref(), Some(CWD));
        assert!(stored.meta.fast);
        assert!(stored.meta.workflow);
        assert_eq!(message_texts(&stored), [BASE_MSG, COORDINATOR_MSG]);
        assert_eq!(warn_rx.try_recv().unwrap(), SAVE_RECOVERED);
        assert!(!disk.failing.contains(&id));
    }

    #[test_case(0; "success_preserves_pending_history")]
    #[test_case(1; "transient_failure_preserves_pending_history")]
    #[test_case(CHECKPOINT_RETRY_BACKOFFS.len() + 1; "exhausted_history_is_not_promoted")]
    fn pending_history_then_config_keeps_truthful_history_ack(failures: usize) {
        let (_tmp, dir) = state_dir();
        if failures > 0 {
            block_sessions_dir(&dir);
        }
        let (writer, _wake_rx) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        session.push_message(Message::user(BASE_MSG.into()));
        let id = session.id;
        writer.seed(Arc::new(session));
        let checkpoint = writer.coordinator_checkpoint();
        let mut history = config_checkpoint(1, 1);
        history.config = None;
        history.model = Arc::from(MODEL);
        history.history = Some(Arc::new(vec![Message::user(COORDINATOR_MSG.into())]));
        let history_ack = checkpoint.checkpoint(CheckpointRequest {
            session_id: id,
            version: CheckpointVersion {
                revision: 1,
                epoch: 1,
            },
            snapshot: Arc::new(history),
        });
        let config_ack = checkpoint.checkpoint(CheckpointRequest {
            session_id: id,
            version: CheckpointVersion {
                revision: 2,
                epoch: 1,
            },
            snapshot: Arc::new(config_checkpoint(1, 1)),
        });
        {
            let state = lock(&writer.pending);
            assert_eq!(message_texts(&state.latest[&id]), [BASE_MSG]);
            assert_eq!(state.latest[&id].model, FAILED_MODEL);
            let Entry::Save(save) = &state.entries[&id] else {
                unreachable!()
            };
            assert_eq!(message_texts(&save.session), [COORDINATOR_MSG]);
        }
        let (warn_tx, _warn_rx) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        for _ in 0..failures {
            if let Some(Entry::Save(save)) = lock(&writer.pending).entries.get_mut(&id) {
                save.retry_at = None;
            }
            disk.flush(&writer.pending);
            assert_eq!(
                message_texts(&writer.latest_snapshot(id).unwrap()),
                [BASE_MSG]
            );
        }
        if failures > 0 {
            std::fs::remove_file(dir.path().join(SESSIONS_DIR)).unwrap();
        }
        if let Some(Entry::Save(save)) = lock(&writer.pending).entries.get_mut(&id) {
            save.retry_at = None;
        }
        disk.flush(&writer.pending);
        let exhausted = failures > CHECKPOINT_RETRY_BACKOFFS.len();
        assert_eq!(smol::block_on(history_ack).is_err(), exhausted);
        assert_eq!(smol::block_on(config_ack).is_err(), exhausted);
        let expected = if exhausted { BASE_MSG } else { COORDINATOR_MSG };
        let reopened = AppSession::load(id, &dir).unwrap();
        assert_eq!(message_texts(&reopened), [expected]);
        assert_eq!(
            message_texts(&writer.latest_snapshot(id).unwrap()),
            [expected]
        );
        assert_eq!(reopened.model, FAILED_MODEL);
        assert_eq!(reopened.meta.mode, Some(StoredMode::Plan));
        assert!(lock(&writer.pending).coordinator_pending.is_empty());
    }

    #[test_case(false, false; "before_first_config_projection")]
    #[test_case(true, false; "after_config_projection")]
    #[test_case(true, true; "replacement_history_invalidates_waiter")]
    fn in_flight_history_retry_handles_config_advancement(
        initial_config: bool,
        replace_history: bool,
    ) {
        let (_tmp, dir) = state_dir();
        let (writer, _wake_rx) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        session.push_message(Message::user(BASE_MSG.into()));
        let id = session.id;
        writer.seed(Arc::new(session));
        writer.bind_config_runtime(id, 1);
        let checkpoint = writer.coordinator_checkpoint();
        let (warn_tx, _warn_rx) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        if initial_config {
            let ack = checkpoint.checkpoint(CheckpointRequest {
                session_id: id,
                version: CheckpointVersion {
                    revision: 1,
                    epoch: 1,
                },
                snapshot: Arc::new(config_checkpoint(1, 1)),
            });
            disk.flush(&writer.pending);
            assert!(smol::block_on(ack).is_ok());
        }
        let mut history = config_checkpoint(1, 1);
        history.config = None;
        history.history = Some(Arc::new(vec![Message::user(COORDINATOR_MSG.into())]));
        let mut history_ack = checkpoint.checkpoint(CheckpointRequest {
            session_id: id,
            version: CheckpointVersion {
                revision: 2,
                epoch: 1,
            },
            snapshot: Arc::new(history),
        });
        let Entry::Save(in_flight) = lock(&writer.pending).entries.remove(&id).unwrap() else {
            unreachable!()
        };
        let mut config = config_checkpoint(1, 2);
        if replace_history {
            config.history = Some(Arc::new(vec![Message::user(BASE_MSG.into())]));
        }
        let config_ack = checkpoint.checkpoint(CheckpointRequest {
            session_id: id,
            version: CheckpointVersion {
                revision: 3,
                epoch: 1,
            },
            snapshot: Arc::new(config),
        });
        assert!(requeue_save(&writer.pending, id, in_flight).is_ok());
        if !replace_history {
            assert!(smol::block_on(futures_lite::future::poll_once(&mut history_ack)).is_none());
        }
        disk.flush(&writer.pending);
        if replace_history {
            let Err(CheckpointError::Save { message, .. }) = smol::block_on(history_ack) else {
                panic!("replaced history must fail its checkpoint")
            };
            assert_eq!(message.as_ref(), CHECKPOINT_REPLACED);
        } else {
            assert!(smol::block_on(history_ack).is_ok());
        }
        assert!(smol::block_on(config_ack).is_ok());
        let expected = if replace_history {
            BASE_MSG
        } else {
            COORDINATOR_MSG
        };
        assert_eq!(
            message_texts(&AppSession::load(id, &dir).unwrap()),
            [expected]
        );
        assert_eq!(
            message_texts(&writer.latest_snapshot(id).unwrap()),
            [expected]
        );
        assert!(lock(&writer.pending).coordinator_pending.is_empty());
    }

    #[test]
    fn provisional_checkpoint_failure_keeps_committed_config_retry() {
        let (_tmp, dir) = state_dir();
        block_sessions_dir(&dir);
        let (writer, _wake_rx) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        session.push_message(Message::user(BASE_MSG.into()));
        let id = session.id;
        writer.seed(Arc::new(session));
        let checkpoint = writer.coordinator_checkpoint();
        let committed = checkpoint.checkpoint(CheckpointRequest {
            session_id: id,
            version: CheckpointVersion {
                revision: 1,
                epoch: 1,
            },
            snapshot: Arc::new(config_checkpoint(1, 1)),
        });
        let mut provisional = config_checkpoint(1, 1);
        provisional.config = None;
        provisional.model = Arc::from(MODEL);
        provisional.history = Some(Arc::new(vec![Message::user(REWOUND_MSG.into())]));
        let candidate = checkpoint.checkpoint(CheckpointRequest {
            session_id: id,
            version: CheckpointVersion {
                revision: 2,
                epoch: 1,
            },
            snapshot: Arc::new(provisional),
        });
        let (warn_tx, _warn_rx) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        for _ in 0..=CHECKPOINT_RETRY_BACKOFFS.len() {
            if let Some(Entry::Save(save)) = lock(&writer.pending).entries.get_mut(&id) {
                save.retry_at = None;
            }
            disk.flush(&writer.pending);
        }
        assert!(smol::block_on(committed).is_err());
        assert!(smol::block_on(candidate).is_err());
        std::fs::remove_file(dir.path().join(SESSIONS_DIR)).unwrap();
        if let Some(Entry::Save(save)) = lock(&writer.pending).entries.get_mut(&id) {
            save.retry_at = None;
        }
        disk.flush(&writer.pending);
        let stored = AppSession::load(id, &dir).unwrap();
        assert_eq!(stored.model, FAILED_MODEL);
        assert_eq!(message_texts(&stored), [BASE_MSG]);
    }

    #[test_case(false; "older_config_save")]
    #[test_case(true; "older_untagged_save")]
    fn stale_save_does_not_publish_or_clear_latest_config_warning(untagged: bool) {
        const FAILURE: &str = "test disk failure";
        const WARNING: &str = "Configuration applied but not saved (runtime 2, generation 1)";
        let (_tmp, dir) = state_dir();
        let (writer, _wake_rx) = manual_writer();
        let session = AppSession::new(MODEL, CWD);
        let id = session.id;
        writer.seed(Arc::new(session));
        let checkpoint = writer.coordinator_checkpoint();
        let enqueue = |epoch| {
            checkpoint.checkpoint(CheckpointRequest {
                session_id: id,
                version: CheckpointVersion { revision: 1, epoch },
                snapshot: Arc::new(config_checkpoint(epoch, 1)),
            })
        };
        let _old_ack = enqueue(1);
        let Entry::Save(mut old) = lock(&writer.pending).entries.remove(&id).unwrap() else {
            unreachable!()
        };
        if untagged {
            old.config_version = None;
        }
        let _latest_ack = enqueue(2);
        let Entry::Save(latest) = lock(&writer.pending).entries.remove(&id).unwrap() else {
            unreachable!()
        };
        let (warn_tx, warn_rx) = flume::unbounded();
        let mut disk = Writer {
            dir,
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        disk.report_save(&writer.pending, &old, Err(FAILURE));
        assert!(warn_rx.try_recv().is_err());
        assert!(!disk.failing.contains(&id));
        disk.report_save(&writer.pending, &latest, Err(FAILURE));
        assert!(warn_rx.try_recv().unwrap().contains(WARNING));
        disk.report_save(&writer.pending, &latest, Err(FAILURE));
        assert!(warn_rx.try_recv().is_err());
        disk.report_save(&writer.pending, &old, Ok(()));
        assert!(disk.failing.contains(&id));
        assert!(warn_rx.try_recv().is_err());
        disk.report_save(&writer.pending, &latest, Ok(()));
        assert_eq!(warn_rx.try_recv().unwrap(), SAVE_RECOVERED);
        assert!(!disk.failing.contains(&id));
    }

    #[test_case(false, false; "bind_before_flush")]
    #[test_case(true, false; "stale_requeue_without_newer_save")]
    #[test_case(true, true; "stale_requeue_with_newer_config")]
    fn replaced_history_payload_fails_waiter_and_persists_authoritative_state(
        in_flight: bool,
        newer_config: bool,
    ) {
        let (_tmp, dir) = state_dir();
        let (writer, _wake_rx) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        session.push_message(Message::user(BASE_MSG.into()));
        let id = session.id;
        writer.seed(Arc::new(session));
        writer.bind_config_runtime(id, 1);
        let checkpoint = writer.coordinator_checkpoint();
        let mut history = config_checkpoint(1, 1);
        history.config = None;
        history.history = Some(Arc::new(vec![Message::user(COORDINATOR_MSG.into())]));
        let history_ack = checkpoint.checkpoint(CheckpointRequest {
            session_id: id,
            version: CheckpointVersion {
                revision: 1,
                epoch: 1,
            },
            snapshot: Arc::new(history),
        });
        let old_save = if in_flight {
            let Entry::Save(save) = lock(&writer.pending).entries.remove(&id).unwrap() else {
                unreachable!()
            };
            Some(save)
        } else {
            None
        };
        writer.bind_config_runtime(id, 2);
        let config_ack = newer_config.then(|| {
            checkpoint.checkpoint(CheckpointRequest {
                session_id: id,
                version: CheckpointVersion {
                    revision: 2,
                    epoch: 2,
                },
                snapshot: Arc::new(config_checkpoint(2, 1)),
            })
        });
        if let Some(save) = old_save {
            assert!(requeue_save(&writer.pending, id, save).is_ok());
        }
        let Err(CheckpointError::Save { message, .. }) = smol::block_on(history_ack) else {
            panic!("discarded history must fail its checkpoint")
        };
        assert_eq!(message.as_ref(), CHECKPOINT_REPLACED);
        {
            let state = lock(&writer.pending);
            let Entry::Save(save) = &state.entries[&id] else {
                unreachable!()
            };
            assert_eq!(save.waiters.len(), usize::from(newer_config));
            assert_eq!(message_texts(&save.session), [BASE_MSG]);
        }
        let (warn_tx, _warn_rx) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        disk.flush(&writer.pending);
        if let Some(ack) = config_ack {
            assert!(smol::block_on(ack).is_ok());
        }
        let reopened = AppSession::load(id, &dir).unwrap();
        assert_eq!(message_texts(&reopened), [BASE_MSG]);
        assert_eq!(
            reopened.model,
            if newer_config { FAILED_MODEL } else { MODEL }
        );
    }

    #[test]
    fn registered_runtime_rejects_old_config_before_first_new_projection() {
        let (writer, _wake_rx) = manual_writer();
        let session = AppSession::new(MODEL, CWD);
        let id = session.id;
        writer.seed(Arc::new(session));
        let checkpoint = writer.coordinator_checkpoint();
        let enqueue = || {
            checkpoint.checkpoint(CheckpointRequest {
                session_id: id,
                version: CheckpointVersion {
                    revision: 1,
                    epoch: 1,
                },
                snapshot: Arc::new(config_checkpoint(1, 1)),
            })
        };
        let old_ack = enqueue();
        writer.bind_config_runtime(id, 2);
        assert!(smol::block_on(enqueue()).is_err());
        let Entry::Save(save) = lock(&writer.pending).entries.remove(&id).unwrap() else {
            unreachable!()
        };
        acknowledge_waiters(&writer.pending, &save.session, save.waiters);
        assert!(smol::block_on(old_ack).is_err());
        assert_eq!(lock(&writer.pending).config_versions[&id], (2, 0));
    }

    #[test]
    fn config_runtime_replacement_rejects_old_enqueue_and_ack() {
        let (writer, _wake_rx) = manual_writer();
        let session = AppSession::new(MODEL, CWD);
        let id = session.id;
        writer.seed(Arc::new(session));
        let enqueue = |epoch, generation| {
            writer
                .coordinator_checkpoint()
                .checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision: generation,
                        epoch,
                    },
                    snapshot: Arc::new(config_checkpoint(epoch, generation)),
                })
        };
        let old_ack = enqueue(1, 10);
        let new_ack = enqueue(2, 1);
        assert!(smol::block_on(enqueue(1, 11)).is_err());
        let Entry::Save(save) = lock(&writer.pending).entries.remove(&id).unwrap() else {
            unreachable!()
        };
        acknowledge_waiters(&writer.pending, &save.session, save.waiters);
        assert!(smol::block_on(old_ack).is_err());
        assert!(smol::block_on(new_ack).is_ok());
        assert_eq!(lock(&writer.pending).config_versions[&id], (2, 1));
    }

    fn state_dir() -> (TempDir, StateDir) {
        let tmp = TempDir::new().unwrap();
        let dir = StateDir::from_path(tmp.path().to_path_buf());
        (tmp, dir)
    }

    fn writer(dir: &StateDir) -> (StorageWriter, flume::Receiver<String>) {
        let (warn_tx, warn_rx) = flume::unbounded();
        (StorageWriter::new(dir.clone(), warn_tx), warn_rx)
    }

    #[test_case(false; "failed_log_delete_preserves_artifacts")]
    #[test_case(true; "cleanup_failure_does_not_fail_log_delete")]
    fn guarded_deletion_preserves_artifacts_until_log_removal(cleanup_failure: bool) {
        use maki_agent::tools::offload::OffloadBackend;
        use maki_storage::session_lock;
        use std::path::PathBuf;

        struct FailingCleanup {
            path: PathBuf,
            attempted: Arc<AtomicBool>,
        }

        impl OffloadBackend for FailingCleanup {
            fn create_new(&self, _: &str, _: &[u8]) -> io::Result<bool> {
                unreachable!()
            }

            fn total_bytes(&self) -> io::Result<u64> {
                Ok(0)
            }

            fn remove_all(&self) -> io::Result<()> {
                self.attempted.store(true, Ordering::Release);
                Err(io::Error::other("gated cleanup failure"))
            }

            fn path(&self, _: &str) -> PathBuf {
                self.path.clone()
            }
        }

        const BODY: &str = "referenced output";
        let (_temp, dir) = state_dir();
        let mut session = AppSession::new(MODEL, CWD);
        session.push_message(Message::observation(BODY.into()));
        session.save(&dir).unwrap();
        let id = session.id;
        let sessions_dir = dir.path().join(SESSIONS_DIR);
        let lease = session_lock::claim(&sessions_dir, &id).unwrap().unwrap();
        let guard = lease.publication_guard();
        let artifact = dir.path().join("retained-artifact.txt");
        std::fs::write(&artifact, BODY).unwrap();
        let attempted = Arc::new(AtomicBool::new(false));
        let store = Arc::new(OffloadStore::new(Box::new(FailingCleanup {
            path: artifact.clone(),
            attempted: Arc::clone(&attempted),
        })));
        store.close_and_drain();
        let log = sessions_dir.join(format!("{id}.jsonl"));
        let backup = sessions_dir.join("retained-log.jsonl");
        if !cleanup_failure {
            std::fs::rename(&log, &backup).unwrap();
            std::fs::create_dir(&log).unwrap();
        }
        let (writer, _) = writer(&dir);
        let (done_tx, done_rx) = flume::bounded(1);
        writer.delete_guarded(id, guard, Some(store), move |result| {
            done_tx.send(result).unwrap();
        });
        let result = done_rx.recv_timeout(DRAIN_TIMEOUT).unwrap();
        assert_eq!(result.is_ok(), cleanup_failure);
        assert_eq!(attempted.load(Ordering::Acquire), cleanup_failure);
        assert_eq!(std::fs::read_to_string(&artifact).unwrap(), BODY);
        if cleanup_failure {
            assert!(AppSession::load(id, &dir).is_err());
        } else {
            std::fs::remove_dir(&log).unwrap();
            std::fs::rename(&backup, &log).unwrap();
            assert_eq!(message_texts(&AppSession::load(id, &dir).unwrap()), [BODY]);
        }
        writer.shutdown(DRAIN_TIMEOUT);
        drop(lease);
    }

    fn message_texts(session: &AppSession) -> Vec<String> {
        session
            .messages()
            .iter()
            .map(|m| m.user_text().unwrap_or_default().to_string())
            .collect()
    }

    fn msg_text(n: usize) -> String {
        format!("{MSG_PREFIX}{n}")
    }

    fn user_message(n: usize) -> Message {
        Message::user(msg_text(n))
    }

    /// A plain file where the sessions dir should be. `create_dir_all` cannot
    /// turn that into a directory, so every flush fails until it is removed.
    fn block_sessions_dir(dir: &StateDir) {
        std::fs::write(dir.path().join(SESSIONS_DIR), "").unwrap();
    }

    /// Snapshots must coalesce per session id, not into one `latest` slot:
    /// two racing sessions used to silently drop one.
    #[test]
    fn shutdown_drains_newest_snapshot_of_every_session() {
        let (_tmp, dir) = state_dir();
        let (writer, _warn_rx) = writer(&dir);
        let a = AppSession::new("test-model", "/tmp/a");
        let mut b = AppSession::new("test-model", "/tmp/b");
        let (a_id, b_id) = (a.id, b.id);
        writer.send(Arc::new(a));
        writer.send(Arc::new(b.clone()));
        b.set_title("renamed".into());
        writer.send(Arc::new(b));
        writer.shutdown(DRAIN_TIMEOUT);

        assert!(AppSession::load(a_id, &dir).is_ok());
        assert_eq!(AppSession::load(b_id, &dir).unwrap().title, "renamed");
    }

    #[test_case(false; "root")]
    #[test_case(true; "child")]
    fn acknowledged_in_flight_history_survives_newer_metadata_failure(child: bool) {
        let (_tmp, dir) = state_dir();
        let (writer, _wake_rx) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        session.push_message(Message::user(BASE_MSG.into()));
        session.set_subagent_messages(TOOL_ID.into(), vec![Message::user(BASE_MSG.into())]);
        let id = session.id;
        writer.seed(Arc::new(session.clone()));
        let ack = if child {
            writer.checkpoint_child_history(
                id,
                TOOL_ID.into(),
                HistorySnapshot::new(vec![Message::user(COORDINATOR_MSG.into())]),
            )
        } else {
            let mut history = config_checkpoint(1, 1);
            history.config = None;
            history.model = MODEL.into();
            history.history = Some(Arc::new(vec![Message::user(COORDINATOR_MSG.into())]));
            writer
                .coordinator_checkpoint()
                .checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision: 1,
                        epoch: 1,
                    },
                    snapshot: Arc::new(history),
                })
        };
        let Entry::Save(in_flight) = lock(&writer.pending).entries.remove(&id).unwrap() else {
            unreachable!()
        };
        session.set_title(TITLE.into());
        writer.send(Arc::new(session.clone()));
        let (warn_tx, _warn_rx) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        disk.write(&in_flight.session).unwrap();
        commit_save(&writer.pending, id, &in_flight);
        acknowledge_waiters(&writer.pending, &in_flight.session, in_flight.waiters);
        assert!(smol::block_on(ack).is_ok());
        let sessions_path = dir.path().join(SESSIONS_DIR);
        let saved_path = dir.path().join(TITLE);
        std::fs::rename(&sessions_path, &saved_path).unwrap();
        block_sessions_dir(&dir);
        disk.logs.clear();
        disk.flush(&writer.pending);
        assert!(disk.failing.contains(&id));
        assert!(lock(&writer.pending).entries.contains_key(&id));
        std::fs::remove_file(&sessions_path).unwrap();
        std::fs::rename(&saved_path, &sessions_path).unwrap();
        let latest = writer.latest_snapshot(id).unwrap();
        let messages = if child {
            &latest.subagent_messages()[TOOL_ID]
        } else {
            latest.messages()
        };
        assert_eq!(messages[0].first_text_content(), Some(COORDINATOR_MSG));
        assert_eq!(latest.title, TITLE);
        writer.send(Arc::new(session));
        if let Some(Entry::Save(save)) = lock(&writer.pending).entries.get_mut(&id) {
            save.retry_at = None;
        }
        disk.flush(&writer.pending);
        let recovered = AppSession::load(id, &dir).unwrap();
        let messages = if child {
            &recovered.subagent_messages()[TOOL_ID]
        } else {
            recovered.messages()
        };
        assert_eq!(messages[0].first_text_content(), Some(COORDINATOR_MSG));
        assert_eq!(recovered.title, TITLE);
    }

    #[test_case(false; "unversioned_metadata")]
    #[test_case(true; "versioned_config")]
    fn in_flight_history_keeps_newer_metadata_until_promoted(versioned: bool) {
        let (_tmp, dir) = state_dir();
        let (writer, _wake_rx) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        session.push_message(Message::user(BASE_MSG.into()));
        let id = session.id;
        writer.seed(Arc::new(session.clone()));
        let checkpoint = writer.coordinator_checkpoint();
        let mut history = config_checkpoint(1, 1);
        history.config = None;
        history.model = MODEL.into();
        history.history = Some(Arc::new(vec![Message::user(COORDINATOR_MSG.into())]));
        let history_ack = checkpoint.checkpoint(CheckpointRequest {
            session_id: id,
            version: CheckpointVersion {
                revision: 1,
                epoch: 1,
            },
            snapshot: Arc::new(history),
        });
        let Entry::Save(in_flight) = lock(&writer.pending).entries.remove(&id).unwrap() else {
            unreachable!()
        };
        let mut metadata = config_checkpoint(1, 1);
        if !versioned {
            metadata.config = None;
        }
        let metadata_ack = checkpoint.checkpoint(CheckpointRequest {
            session_id: id,
            version: CheckpointVersion {
                revision: 2,
                epoch: 1,
            },
            snapshot: Arc::new(metadata),
        });
        let (warn_tx, _warn_rx) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        disk.write(&in_flight.session).unwrap();
        commit_save(&writer.pending, id, &in_flight);
        acknowledge_waiters(&writer.pending, &in_flight.session, in_flight.waiters);
        assert!(smol::block_on(history_ack).is_ok());
        assert_eq!(
            writer.latest_snapshot(id).unwrap().model,
            if versioned { FAILED_MODEL } else { MODEL }
        );
        disk.flush(&writer.pending);
        assert!(smol::block_on(metadata_ack).is_ok());
        assert_eq!(AppSession::load(id, &dir).unwrap().model, FAILED_MODEL);
        assert_eq!(writer.latest_snapshot(id).unwrap().model, FAILED_MODEL);
        session.set_title(TITLE.into());
        writer.send(Arc::new(session));
        disk.flush(&writer.pending);
        let reopened = AppSession::load(id, &dir).unwrap();
        assert_eq!(reopened.model, FAILED_MODEL);
        assert_eq!(reopened.title, TITLE);
        assert_eq!(message_texts(&reopened), [COORDINATOR_MSG]);
        assert_eq!(writer.latest_snapshot(id).unwrap().model, FAILED_MODEL);
        if versioned {
            assert_eq!(reopened.meta.mode, Some(StoredMode::Plan));
        }
        assert!(lock(&writer.pending).coordinator_pending.is_empty());
    }

    #[test_case(false, false; "root_rollback")]
    #[test_case(true, false; "child_rollback")]
    #[test_case(false, true; "root_metadata_before_success")]
    #[test_case(true, true; "child_metadata_before_success")]
    fn in_flight_success_rebases_newer_history_rollback(child: bool, succeed: bool) {
        let (_tmp, dir) = state_dir();
        let (writer, _wake_rx) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        session.push_message(Message::user(BASE_MSG.into()));
        session.set_subagent_messages(TOOL_ID.into(), vec![Message::user(BASE_MSG.into())]);
        let id = session.id;
        writer.seed(Arc::new(session.clone()));
        let checkpoint = |text: &str| {
            if child {
                writer.checkpoint_child_history(
                    id,
                    TOOL_ID.into(),
                    HistorySnapshot::new(vec![Message::user(text.into())]),
                )
            } else {
                let mut history = config_checkpoint(1, 1);
                history.config = None;
                history.model = MODEL.into();
                history.history = Some(Arc::new(vec![Message::user(text.into())]));
                writer
                    .coordinator_checkpoint()
                    .checkpoint(CheckpointRequest {
                        session_id: id,
                        version: CheckpointVersion {
                            revision: 1,
                            epoch: 1,
                        },
                        snapshot: Arc::new(history),
                    })
            }
        };
        let first_ack = checkpoint(COORDINATOR_MSG);
        let Entry::Save(first) = lock(&writer.pending).entries.remove(&id).unwrap() else {
            unreachable!()
        };
        let newer_ack = checkpoint(REWOUND_MSG);
        let (warn_tx, _warn_rx) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        disk.write(&first.session).unwrap();
        commit_save(&writer.pending, id, &first);
        acknowledge_waiters(&writer.pending, &first.session, first.waiters);
        assert!(smol::block_on(first_ack).is_ok());
        {
            let state = lock(&writer.pending);
            let candidate = &state.coordinator_pending[&id].session;
            let messages = if child {
                &candidate.subagent_messages()[TOOL_ID]
            } else {
                candidate.messages()
            };
            assert_eq!(messages[0].first_text_content(), Some(REWOUND_MSG));
        }
        if succeed {
            session.set_title(TITLE.into());
            writer.send(Arc::new(session));
            disk.flush(&writer.pending);
            assert!(smol::block_on(newer_ack).is_ok());
            let latest = writer.latest_snapshot(id).unwrap();
            let reopened = AppSession::load(id, &dir).unwrap();
            for saved in [&*latest, &reopened] {
                let messages = if child {
                    &saved.subagent_messages()[TOOL_ID]
                } else {
                    saved.messages()
                };
                assert_eq!(messages[0].first_text_content(), Some(REWOUND_MSG));
                assert_eq!(saved.title, TITLE);
            }
            return;
        }
        let sessions_path = dir.path().join(SESSIONS_DIR);
        let saved_path = dir.path().join(TITLE);
        std::fs::rename(&sessions_path, &saved_path).unwrap();
        block_sessions_dir(&dir);
        disk.logs.clear();
        if let Some(Entry::Save(save)) = lock(&writer.pending).entries.get_mut(&id) {
            save.retry_attempt = CHECKPOINT_RETRY_BACKOFFS.len();
        }
        disk.flush(&writer.pending);
        assert!(smol::block_on(newer_ack).is_err());
        std::fs::remove_file(&sessions_path).unwrap();
        std::fs::rename(&saved_path, &sessions_path).unwrap();
        let latest = writer.latest_snapshot(id).unwrap();
        let messages = if child {
            &latest.subagent_messages()[TOOL_ID]
        } else {
            latest.messages()
        };
        assert_eq!(messages[0].first_text_content(), Some(COORDINATOR_MSG));
        let recovered = AppSession::load(id, &dir).unwrap();
        let messages = if child {
            &recovered.subagent_messages()[TOOL_ID]
        } else {
            recovered.messages()
        };
        assert_eq!(messages[0].first_text_content(), Some(COORDINATOR_MSG));
    }

    #[test]
    fn older_in_flight_success_cannot_replace_newer_authoritative_snapshot() {
        let pending: Pending = Arc::default();
        let mut old = AppSession::new(MODEL, CWD);
        let id = old.id;
        old.set_title("old write".into());
        let old = Arc::new(old);
        {
            let mut state = lock(&pending);
            state.latest.insert(id, Arc::clone(&old));
            state.latest_generations.insert(id, 1);
            state.entries.insert(
                id,
                Entry::Save(PendingSave {
                    session: Arc::clone(&old),
                    waiters: Vec::new(),
                    generation: 1,
                    coordinator_generation: Some(1),
                    config_version: None,
                    coordinator_history_base: None,
                    retry_attempt: 0,
                    retry_at: None,
                }),
            );
        }
        let Entry::Save(old_in_flight) = lock(&pending).entries.remove(&id).unwrap() else {
            unreachable!()
        };
        let mut newer = old.as_ref().clone();
        newer.set_title("new authoritative".into());
        {
            let mut state = lock(&pending);
            state.latest.insert(id, Arc::new(newer));
            state.latest_generations.insert(id, 2);
        }

        commit_save(&pending, id, &old_in_flight);

        assert_eq!(lock(&pending).latest[&id].title, "new authoritative");
    }

    #[test]
    fn deferred_retry_merges_into_newer_generation_without_losing_waiters() {
        const OLD_GENERATION: u64 = 1;
        const NEW_GENERATION: u64 = 2;
        const RETRY_ATTEMPT: usize = 1;

        let pending: Pending = Arc::default();
        let mut old = AppSession::new(MODEL, CWD);
        let id = old.id;
        old.set_title("old retry".into());
        let mut newer = old.clone();
        newer.set_title("new generation".into());
        let old_version = CheckpointVersion {
            revision: old.revision(),
            epoch: OLD_GENERATION,
        };
        let newer_version = CheckpointVersion {
            revision: newer.revision(),
            epoch: NEW_GENERATION,
        };
        let (old_reply, old_ack) = flume::bounded(1);
        let (newer_reply, newer_ack) = flume::bounded(1);
        let retry_at = Instant::now() + Duration::from_secs(60);
        let old_retry = PendingSave {
            session: Arc::new(old),
            waiters: vec![CheckpointWaiter {
                version: old_version,
                config_version: None,
                histories: required_histories(&newer),
                reply: old_reply,
            }],
            generation: OLD_GENERATION,
            coordinator_generation: Some(OLD_GENERATION),
            config_version: None,
            coordinator_history_base: None,
            retry_attempt: RETRY_ATTEMPT,
            retry_at: Some(retry_at),
        };
        let newer_histories = required_histories(&newer);
        lock(&pending).entries.insert(
            id,
            Entry::Save(PendingSave {
                session: Arc::new(newer),
                waiters: vec![CheckpointWaiter {
                    version: newer_version,
                    config_version: None,
                    histories: newer_histories,
                    reply: newer_reply,
                }],
                generation: NEW_GENERATION,
                coordinator_generation: Some(NEW_GENERATION),
                config_version: None,
                coordinator_history_base: None,
                retry_attempt: 0,
                retry_at: None,
            }),
        );

        assert!(requeue_save(&pending, id, old_retry).is_ok());

        let Entry::Save(merged) = lock(&pending).entries.remove(&id).unwrap() else {
            unreachable!()
        };
        assert_eq!(merged.session.title, "new generation");
        assert_eq!(merged.generation, NEW_GENERATION);
        assert_eq!(merged.coordinator_generation, Some(NEW_GENERATION));
        assert_eq!(merged.retry_attempt, RETRY_ATTEMPT);
        assert_eq!(merged.retry_at, Some(retry_at));
        acknowledge_waiters(&pending, &merged.session, merged.waiters);
        assert_eq!(old_ack.recv().unwrap().unwrap().version, old_version);
        assert_eq!(newer_ack.recv().unwrap().unwrap().version, newer_version);
    }

    #[test]
    fn coalesced_checkpoints_complete_every_acknowledgement() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _warn_rx) = writer(&dir);
            let mut first = AppSession::new(MODEL, CWD);
            let id = first.id;
            first.set_title("first".into());
            let mut second = first.clone();
            second.set_title("second".into());
            let first_version = CheckpointVersion {
                revision: first.revision(),
                epoch: 1,
            };
            let second_version = CheckpointVersion {
                revision: second.revision(),
                epoch: 1,
            };

            let first_ack = writer.checkpoint(CheckpointRequest {
                session_id: id,
                version: first_version,
                snapshot: Arc::new(first),
            });
            let second_ack = writer.checkpoint(CheckpointRequest {
                session_id: id,
                version: second_version,
                snapshot: Arc::new(second),
            });
            let (first_ack, second_ack) = futures_lite::future::zip(first_ack, second_ack).await;

            assert_eq!(first_ack.unwrap().version, first_version);
            assert_eq!(second_ack.unwrap().version, second_version);
            writer.shutdown(DRAIN_TIMEOUT);
            assert_eq!(AppSession::load(id, &dir).unwrap().title, "second");
        });
    }

    /// Every coordinator's checkpoint writer holds a clone of the wake
    /// sender, and those live in tasks that exit on their own schedule. If
    /// shutdown waited for the last clone to drop, exit would take the whole
    /// timeout whenever one outlived the event loop.
    #[test]
    fn shutdown_does_not_wait_for_a_lingering_checkpoint_writer() {
        let (_tmp, dir) = state_dir();
        let (writer, _warn_rx) = writer(&dir);
        // Stands in for a coordinator task that has not been polled yet.
        let lingering = writer.coordinator_checkpoint();

        let started = std::time::Instant::now();
        writer.shutdown(Duration::from_secs(3));
        let elapsed = started.elapsed();

        drop(lingering);
        assert!(
            elapsed < Duration::from_secs(1),
            "shutdown waited {elapsed:?} for a checkpoint writer that outlived the loop"
        );
    }

    /// A session with nothing in it yet gets its files cleaned up, but it is
    /// still live in its tab and its coordinator checkpoints by merging into
    /// the writer's snapshot. Forgetting that snapshot left the next
    /// coordinator checkpoint -- the model change on a freshly `/new`ed
    /// session -- failing with "session snapshot is unavailable".
    #[test]
    fn cleaning_up_an_empty_session_keeps_its_coordinator_base() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _warn_rx) = writer(&dir);
            let session = AppSession::new(MODEL, CWD);
            let id = session.id;
            writer.send(Arc::new(session));

            writer.delete_empty(id);

            let options = SessionOptions::new(
                builtin_option_definitions(
                    "next/model",
                    [Arc::from("next/model")],
                    false,
                    false,
                    false,
                    ThinkingConfig::Off,
                ),
                &Default::default(),
            )
            .unwrap()
            .snapshot();
            let ack = writer
                .coordinator_checkpoint()
                .checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision: 1,
                        epoch: 1,
                    },
                    snapshot: Arc::new(SessionCheckpoint {
                        history: Some(Arc::new(Vec::new())),
                        model: Arc::from("next/model"),
                        cwd: std::path::PathBuf::from(CWD),
                        options,
                        config: None,
                    }),
                })
                .await;
            assert!(
                ack.is_ok(),
                "an emptied session must still accept a coordinator checkpoint: {ack:?}"
            );
        });
    }

    /// A user-requested delete is different: the session is meant to be gone,
    /// so its snapshot goes too.
    #[test]
    fn deleting_a_session_forgets_its_coordinator_base() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _warn_rx) = writer(&dir);
            let session = AppSession::new(MODEL, CWD);
            let id = session.id;
            writer.send(Arc::new(session));

            let (done_tx, done_rx) = flume::bounded(1);
            writer.delete(id, move |res| {
                let _ = done_tx.send(res);
            });
            done_rx.recv_async().await.unwrap().unwrap();

            let options = SessionOptions::new(
                builtin_option_definitions(
                    "next/model",
                    [Arc::from("next/model")],
                    false,
                    false,
                    false,
                    ThinkingConfig::Off,
                ),
                &Default::default(),
            )
            .unwrap()
            .snapshot();
            let ack = writer
                .coordinator_checkpoint()
                .checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision: 1,
                        epoch: 1,
                    },
                    snapshot: Arc::new(SessionCheckpoint {
                        history: Some(Arc::new(Vec::new())),
                        model: Arc::from("next/model"),
                        cwd: std::path::PathBuf::from(CWD),
                        options,
                        config: None,
                    }),
                })
                .await;
            assert!(ack.is_err(), "a deleted session must not be resurrected");
        });
    }

    fn stored_post(chat: Option<&str>, after_message: usize) -> StoredPost {
        StoredPost {
            chat: chat.map(str::to_owned),
            after_message,
            post: Post {
                text: POST_TEXT.into(),
                level: PostLevel::Info,
                agent: None,
            },
        }
    }

    #[test]
    fn send_reuses_unchanged_snapshot_and_ignores_closed_writer() {
        let (writer, _wake_rx) = manual_writer();
        let session = Arc::new(AppSession::new(MODEL, CWD));
        writer.seed(Arc::clone(&session));
        writer.send(Arc::clone(&session));
        assert!(Arc::ptr_eq(
            &writer.latest_snapshot(session.id).unwrap(),
            &session
        ));
        let mut state = lock(&writer.pending);
        state.closed = true;
        state.entries.clear();
        drop(state);
        writer.send(Arc::clone(&session));
        assert!(lock(&writer.pending).entries.is_empty());
        assert!(Arc::ptr_eq(
            &writer.latest_snapshot(session.id).unwrap(),
            &session
        ));
    }

    #[test]
    fn sending_compacted_history_keeps_child_state() {
        let (writer, _wake_rx) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        session.set_subagent_messages(ORPHAN_CHILD.into(), vec![Message::user(BASE_MSG.into())]);
        session.push_post(stored_post(None, 2));
        session.push_post(stored_post(Some(ORPHAN_CHILD), 2));
        writer.seed(Arc::new(session.clone()));
        let mut history = HistorySnapshot::new(vec![Message::user(COORDINATOR_MSG.into())]);
        history.compacted = true;
        session.adopt_history(&history);
        let session = Arc::new(session);
        writer.send(Arc::clone(&session));
        let latest = writer.latest_snapshot(session.id).unwrap();
        assert_eq!(latest.posts(), session.posts());
        assert!(latest.subagent_messages().contains_key(ORPHAN_CHILD));
    }

    #[test]
    fn main_checkpoint_prunes_posts_and_orphans_before_acknowledgement() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _warn_rx) = writer(&dir);
            let mut session = AppSession::new(MODEL, CWD);
            session.push_message(Message::user(BASE_MSG.into()));
            for child in [LIVE_CHILD, ORPHAN_CHILD] {
                session.set_subagent_messages(child.into(), vec![Message::user(BASE_MSG.into())]);
            }
            session.push_post(stored_post(None, 1));
            session.push_post(stored_post(None, 2));
            let id = session.id;
            writer.send(Arc::new(session));
            writer.set_live_children(id, vec![LIVE_CHILD.into()]);
            let mut checkpoint = config_checkpoint(0, 0);
            checkpoint.config = None;
            checkpoint.model = MODEL.into();
            checkpoint.history = Some(Arc::new(vec![Message::user(REWOUND_MSG.into())]));
            writer
                .coordinator_checkpoint()
                .checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision: 1,
                        epoch: 1,
                    },
                    snapshot: Arc::new(checkpoint),
                })
                .await
                .unwrap();
            let loaded = AppSession::load(id, &dir).unwrap();
            assert_eq!(loaded.posts(), [stored_post(None, 1)]);
            assert!(loaded.subagent_messages().contains_key(LIVE_CHILD));
            assert!(!loaded.subagent_messages().contains_key(ORPHAN_CHILD));
            let latest = writer.latest_snapshot(id).unwrap();
            assert_eq!(latest.posts(), loaded.posts());
            assert!(!latest.subagent_messages().contains_key(ORPHAN_CHILD));
            writer.shutdown(DRAIN_TIMEOUT);
        });
    }

    #[test]
    fn child_history_checkpoint_preserves_root_sibling_and_rejects_stale_save() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _warn_rx) = writer(&dir);
            let mut session = AppSession::new(MODEL, CWD);
            session.push_message(Message::user(BASE_MSG.into()));
            session.set_subagent_messages(TOOL_ID.into(), vec![Message::user(BASE_MSG.into())]);
            session.set_subagent_messages(
                SIBLING_AGENT_ID.into(),
                vec![Message::user(BASE_MSG.into())],
            );
            session.push_post(stored_post(None, 2));
            session.push_post(stored_post(Some(TOOL_ID), 1));
            session.push_post(stored_post(Some(TOOL_ID), 2));
            session.push_post(stored_post(Some(SIBLING_AGENT_ID), 2));
            let id = session.id;
            writer.send(Arc::new(session.clone()));
            writer
                .checkpoint_child_history(
                    id,
                    TOOL_ID.into(),
                    HistorySnapshot::new(vec![Message::user(REWOUND_MSG.into())]),
                )
                .await
                .unwrap();
            let expected_posts = [
                stored_post(None, 2),
                stored_post(Some(TOOL_ID), 1),
                stored_post(Some(SIBLING_AGENT_ID), 2),
            ];
            assert_eq!(AppSession::load(id, &dir).unwrap().posts(), expected_posts);
            assert_eq!(writer.latest_snapshot(id).unwrap().posts(), expected_posts);
            session.set_title(TITLE.into());
            session.set_subagent_messages(
                SIBLING_AGENT_ID.into(),
                vec![Message::user(RESUMED_MSG.into())],
            );
            writer.send(Arc::new(session));
            writer.shutdown(DRAIN_TIMEOUT);
            let loaded = AppSession::load(id, &dir).unwrap();
            assert_eq!(loaded.title, TITLE);
            assert_eq!(loaded.messages().len(), 1);
            assert_eq!(loaded.posts(), expected_posts);
            let child = serde_json::to_value(&loaded.subagent_messages()[TOOL_ID]).unwrap();
            let expected = serde_json::to_value(vec![Message::user(REWOUND_MSG.into())]).unwrap();
            assert_eq!(child, expected);
            let sibling =
                serde_json::to_value(&loaded.subagent_messages()[SIBLING_AGENT_ID]).unwrap();
            assert_eq!(
                sibling,
                serde_json::to_value(vec![Message::user(RESUMED_MSG.into())]).unwrap()
            );
        });
    }

    #[test_case(false; "retry_after_child_publish")]
    #[test_case(true; "cancel_before_root_retry")]
    fn child_approval_does_not_revert_acknowledged_root_history(cancel: bool) {
        let (_tmp, dir) = state_dir();
        let (writer, _wake) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        session.set_subagent_messages(TOOL_ID.into(), vec![Message::user(BASE_MSG.into())]);
        let id = session.id;
        writer.seed(Arc::new(session));
        let (warn_tx, _warnings) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        let checkpoint = |message: &str, revision| {
            let mut snapshot = config_checkpoint(1, revision);
            snapshot.config = None;
            snapshot.history = Some(Arc::new(vec![Message::user(message.into())]));
            writer
                .coordinator_checkpoint()
                .checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision,
                        epoch: revision,
                    },
                    snapshot: Arc::new(snapshot),
                })
        };
        let accepted = checkpoint(REWOUND_MSG, 1);
        disk.flush(&writer.pending);
        smol::block_on(accepted).unwrap();
        let newer = checkpoint(RESUMED_MSG, 2);
        let sessions_path = dir.path().join(SESSIONS_DIR);
        let saved_path = dir.path().join(TITLE);
        std::fs::rename(&sessions_path, &saved_path).unwrap();
        block_sessions_dir(&dir);
        disk.logs.clear();
        disk.flush(&writer.pending);
        assert!(disk.failing.contains(&id));
        std::fs::remove_file(&sessions_path).unwrap();
        std::fs::rename(&saved_path, &sessions_path).unwrap();
        if let Some(Entry::Save(save)) = lock(&writer.pending).entries.get_mut(&id) {
            save.retry_at = Some(Instant::now() + DRAIN_TIMEOUT);
        }
        let publication = CheckpointPublication::default();
        let lease = session_lock::claim(&dir.ensure_subdir(SESSIONS_DIR).unwrap(), &id)
            .unwrap()
            .unwrap();
        let mut child = Box::pin(writer.checkpoint_child_approval(
            id,
            TOOL_ID.into(),
            HistorySnapshot::new(Vec::new()),
            publication.clone(),
            lease.publication_guard(),
        ));
        assert!(smol::block_on(futures_lite::future::poll_once(child.as_mut())).is_none());
        if cancel {
            assert!(publication.cancel());
        }
        disk.flush(&writer.pending);
        assert_eq!(smol::block_on(child).is_err(), cancel);
        let loaded = AppSession::load(id, &dir).unwrap();
        assert_eq!(loaded.messages()[0].user_text(), Some(REWOUND_MSG));
        if let Some(Entry::Save(save)) = lock(&writer.pending).entries.get_mut(&id) {
            save.retry_at = None;
        }
        disk.flush(&writer.pending);
        smol::block_on(newer).unwrap();
        let loaded = AppSession::load(id, &dir).unwrap();
        assert_eq!(loaded.messages()[0].user_text(), Some(RESUMED_MSG));
        assert_eq!(
            loaded
                .subagent_messages()
                .get(TOOL_ID)
                .map_or(0, |messages| messages.len()),
            usize::from(cancel)
        );
    }

    #[test_case(false; "without_later_save")]
    #[test_case(true; "with_later_save")]
    fn child_approval_cancelled_before_dequeue(interleave: bool) {
        let (_tmp, dir) = state_dir();
        let (writer, _wake) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        session.set_subagent_messages(TOOL_ID.into(), vec![Message::user(BASE_MSG.into())]);
        let id = session.id;
        writer.seed(Arc::new(session.clone()));
        let publication = CheckpointPublication::default();
        let lease = session_lock::claim(&dir.ensure_subdir(SESSIONS_DIR).unwrap(), &id)
            .unwrap()
            .unwrap();
        let mut checkpoint = Box::pin(writer.checkpoint_child_approval(
            id,
            TOOL_ID.into(),
            HistorySnapshot::new(Vec::new()),
            publication.clone(),
            lease.publication_guard(),
        ));
        assert!(smol::block_on(futures_lite::future::poll_once(checkpoint.as_mut())).is_none());
        assert!(publication.cancel());
        if interleave {
            session.set_title(TITLE.into());
        }
        writer.send(Arc::new(session));
        let (warn_tx, _warn_rx) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        disk.flush(&writer.pending);
        assert_eq!(
            smol::block_on(checkpoint).unwrap_err(),
            CHECKPOINT_CANCELLED
        );
        let loaded = AppSession::load(id, &dir).unwrap();
        assert_eq!(loaded.subagent_messages()[TOOL_ID].len(), 1);
        assert!(lock(&writer.pending).coordinator_pending.is_empty());
        assert!(writer.latest_snapshot(id).unwrap().subagent_messages()[TOOL_ID].len() == 1);
        if interleave {
            assert_eq!(loaded.title, TITLE);
        }
    }

    #[test_case(false, false, false, false; "publish_rebases_later_save")]
    #[test_case(true, false, false, false; "cancel_preserves_later_save")]
    #[test_case(false, true, false, false; "replacement_fences_staged_publication")]
    #[test_case(false, false, true, false; "runtime_fences_staged_publication")]
    #[test_case(false, false, false, true; "lock_loss_fences_staged_publication")]
    fn child_approval_staging_is_isolated(
        cancel: bool,
        replace: bool,
        rebind: bool,
        lose_lock: bool,
    ) {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _warnings) = writer(&dir);
            let writer = Arc::new(writer);
            let mut session = AppSession::new(MODEL, CWD);
            session.push_message(Message::user(BASE_MSG.into()));
            session.set_subagent_messages(TOOL_ID.into(), vec![Message::user(BASE_MSG.into())]);
            session.set_subagent_messages(
                SIBLING_AGENT_ID.into(),
                vec![Message::user(BASE_MSG.into())],
            );
            session.push_post(stored_post(Some(TOOL_ID), 2));
            let id = session.id;
            writer.send(Arc::new(session.clone()));
            let publication = CheckpointPublication::default();
            let history = HistorySnapshot::new(Vec::new());
            let lease = session_lock::claim(&dir.ensure_subdir(SESSIONS_DIR).unwrap(), &id)
                .unwrap()
                .unwrap();
            let guard = lease.publication_guard();
            let (entered, entry) = flume::bounded(1);
            let (release, gate) = flume::bounded(1);
            lock(&writer.pending).child_stage_gate = Some((entered, gate));
            let checkpoint = smol::spawn({
                let writer = Arc::clone(&writer);
                let publication = publication.clone();
                async move {
                    writer
                        .checkpoint_child_approval(id, TOOL_ID.into(), history, publication, guard)
                        .await
                }
            });
            entry.recv_async().await.unwrap();
            assert!(writer.latest_snapshot(id).unwrap().subagent_messages()[TOOL_ID].len() == 1);
            assert!(lock(&writer.pending).coordinator_pending.is_empty());
            session.set_title(TITLE.into());
            session.push_message(Message::user(RESUMED_MSG.into()));
            session.set_subagent_messages(
                SIBLING_AGENT_ID.into(),
                vec![Message::user(RESUMED_MSG.into())],
            );
            if replace {
                writer.forget(id);
                writer.seed(Arc::new(session.clone()));
            }
            if rebind {
                writer.bind_config_runtime(id, 2);
            }
            if lose_lock {
                lease.release().unwrap();
            }
            writer.send(Arc::new(session));
            if cancel {
                assert!(publication.cancel());
            }
            let cancelled = cancel || replace || rebind || lose_lock;
            release.send(()).unwrap();
            let result = checkpoint.await;
            assert_eq!(result.is_err(), cancelled);
            if !cancelled {
                assert!(!publication.cancel());
            }
            writer.shutdown(DRAIN_TIMEOUT);
            let loaded = AppSession::load(id, &dir).unwrap();
            assert_eq!(loaded.title, TITLE);
            assert_eq!(loaded.messages().len(), 2);
            assert_eq!(
                loaded
                    .subagent_messages()
                    .get(TOOL_ID)
                    .map_or(0, |messages| messages.len()),
                usize::from(cancelled)
            );
            assert_eq!(loaded.posts().len(), usize::from(cancelled));
            assert_eq!(
                serde_json::to_value(&loaded.subagent_messages()[SIBLING_AGENT_ID]).unwrap(),
                serde_json::to_value(vec![Message::user(RESUMED_MSG.into())]).unwrap()
            );
        });
    }

    #[test_case(false; "without_interleaved_save")]
    #[test_case(true; "with_interleaved_save")]
    fn failed_child_replacement_keeps_committed_history(interleaved: bool) {
        let (_tmp, dir) = state_dir();
        block_sessions_dir(&dir);
        let (writer, _wake_rx) = manual_writer();
        let mut session = AppSession::new(MODEL, CWD);
        session.set_subagent_messages(TOOL_ID.into(), vec![Message::user(BASE_MSG.into())]);
        let id = session.id;
        writer.seed(Arc::new(session.clone()));
        let checkpoint = writer.checkpoint_child_history(
            id,
            TOOL_ID.into(),
            HistorySnapshot::new(vec![Message::user(REWOUND_MSG.into())]),
        );
        if interleaved {
            session.set_title(TITLE.into());
            writer.send(Arc::new(session.clone()));
        }
        let (warn_tx, _warn_rx) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        for _ in 0..=CHECKPOINT_RETRY_BACKOFFS.len() {
            if let Some(Entry::Save(save)) = lock(&writer.pending).entries.get_mut(&id) {
                save.retry_at = None;
            }
            disk.flush(&writer.pending);
        }
        assert!(
            matches!(smol::block_on(checkpoint), Err(CheckpointError::Save { session_id, .. }) if session_id == id)
        );
        assert!(lock(&writer.pending).coordinator_pending.is_empty());
        let latest = writer.latest_snapshot(id).unwrap();
        assert_eq!(
            serde_json::to_value(&latest.subagent_messages()[TOOL_ID]).unwrap(),
            serde_json::to_value(vec![Message::user(BASE_MSG.into())]).unwrap()
        );
        std::fs::remove_file(dir.path().join(SESSIONS_DIR)).unwrap();
        let mut retry = latest.as_ref().clone();
        retry.set_title(TITLE.into());
        writer.send(Arc::new(retry));
        disk.flush(&writer.pending);
        let stored = AppSession::load(id, &dir).unwrap();
        assert_eq!(stored.title, TITLE);
        assert_eq!(
            serde_json::to_value(&stored.subagent_messages()[TOOL_ID]).unwrap(),
            serde_json::to_value(vec![Message::user(BASE_MSG.into())]).unwrap()
        );
    }

    #[test]
    fn successive_child_replacements_fence_stale_snapshots_and_reconstructed_events() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _warn_rx) = writer(&dir);
            let mut session = AppSession::new(MODEL, CWD);
            session.push_message(Message::user(BASE_MSG.into()));
            session.set_subagent_messages(TOOL_ID.into(), vec![Message::user(BASE_MSG.into())]);
            session.set_subagent_messages(
                SIBLING_AGENT_ID.into(),
                vec![Message::user(BASE_MSG.into())],
            );
            let id = session.id;
            let original = session.clone();
            writer.send(Arc::new(session.clone()));
            let earlier = HistorySnapshot::new(vec![Message::user(REWOUND_MSG.into())]);
            writer
                .checkpoint_child_history(id, TOOL_ID.into(), earlier.clone())
                .await
                .unwrap();
            assert!(session.adopt_subagent_history(TOOL_ID.into(), &earlier));
            let replacement = HistorySnapshot::new(vec![Message::user(RESUMED_MSG.into())]);
            writer
                .checkpoint_child_history(id, TOOL_ID.into(), replacement.clone())
                .await
                .unwrap();
            // A delayed save from B must not resurrect B after the B -> C replacement.
            writer.send(Arc::new(session.clone()));
            // Reallocation cannot turn an old producer epoch into a new one.
            let reconstructed = HistorySnapshot {
                messages: Arc::new(earlier.messages.as_ref().clone()),
                ..earlier
            };
            session.adopt_subagent_history(TOOL_ID.into(), &reconstructed);
            session.set_title(TITLE.into());
            session.set_subagent_messages(
                SIBLING_AGENT_ID.into(),
                vec![Message::user(REWOUND_MSG.into())],
            );
            writer.send(Arc::new(session.clone()));
            assert_eq!(
                writer
                    .latest_snapshot(id)
                    .unwrap()
                    .subagent_history_snapshot(TOOL_ID)
                    .unwrap()
                    .epoch,
                replacement.epoch
            );
            assert!(session.adopt_subagent_history(TOOL_ID.into(), &replacement));
            assert!(!session.adopt_subagent_history(TOOL_ID.into(), &reconstructed));
            writer.send(Arc::new(original));
            writer.send(Arc::new(session));
            writer.shutdown(DRAIN_TIMEOUT);
            let loaded = AppSession::load(id, &dir).unwrap();
            assert_eq!(loaded.title, TITLE);
            assert_eq!(
                serde_json::to_value(loaded.messages()).unwrap(),
                serde_json::to_value(vec![Message::user(BASE_MSG.into())]).unwrap()
            );
            assert_eq!(
                serde_json::to_value(&loaded.subagent_messages()[TOOL_ID]).unwrap(),
                serde_json::to_value(&replacement.messages).unwrap()
            );
            assert_eq!(
                serde_json::to_value(&loaded.subagent_messages()[SIBLING_AGENT_ID]).unwrap(),
                serde_json::to_value(vec![Message::user(REWOUND_MSG.into())]).unwrap()
            );
        });
    }

    #[test]
    fn coordinator_checkpoint_merges_without_losing_tui_state() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _warn_rx) = writer(&dir);
            let mut session = AppSession::new(MODEL, CWD);
            session.set_title(TITLE.into());
            session.meta.mode = Some(StoredMode::Plan);
            session.meta.queued_messages = vec!["queued".into()];
            let id = session.id;
            writer.send(Arc::new(session.clone()));

            let options = SessionOptions::new(
                builtin_option_definitions(
                    "next/model",
                    [Arc::from("next/model")],
                    true,
                    true,
                    true,
                    ThinkingConfig::Effort(Effort::High),
                ),
                &Default::default(),
            )
            .unwrap()
            .snapshot();
            let history = vec![Message::user("coordinator history".into())];
            let version = CheckpointVersion {
                revision: 1,
                epoch: 1,
            };
            let checkpoint = writer.coordinator_checkpoint();
            let ack = checkpoint
                .checkpoint(CheckpointRequest {
                    session_id: id,
                    version,
                    snapshot: Arc::new(SessionCheckpoint {
                        history: Some(Arc::new(history)),
                        model: Arc::from("next/model"),
                        cwd: "/tmp/next".into(),
                        options,
                        config: None,
                    }),
                })
                .await
                .unwrap();

            assert_eq!(ack.version, version);
            let persisted = AppSession::load(id, &dir).unwrap();
            assert_eq!(
                persisted.meta.thinking,
                Some(StoredThinking::Effort {
                    level: Effort::High
                })
            );
            let mut later_ui = session;
            later_ui.set_title("later UI title".into());
            later_ui.meta.mode = Some(StoredMode::Plan);
            later_ui.meta.queued_messages = vec!["queued".into()];
            writer.send(Arc::new(later_ui));
            drop(checkpoint);
            writer.shutdown(DRAIN_TIMEOUT);
            let loaded = AppSession::load(id, &dir).unwrap();
            assert_eq!(loaded.title, "later UI title");
            assert_eq!(loaded.meta.mode, Some(StoredMode::Plan));
            assert_eq!(loaded.meta.queued_messages, ["queued"]);
            assert_eq!(loaded.model, "next/model");
            assert_eq!(loaded.cwd, "/tmp/next");
            assert_eq!(loaded.meta.yolo, Some(true));
            assert!(loaded.meta.fast);
            assert!(loaded.meta.workflow);
            assert_eq!(
                loaded.meta.thinking,
                Some(StoredThinking::Effort {
                    level: Effort::High
                })
            );
            assert_eq!(message_texts(&loaded), ["coordinator history"]);
        });
    }

    #[test]
    fn divergent_tui_history_is_not_overwritten_by_coordinator_history() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            let (writer, _warn_rx) = writer(&dir);
            let mut session = AppSession::new(MODEL, CWD);
            session.push_message(Message::user("base".into()));
            let id = session.id;
            writer.send(Arc::new(session.clone()));

            let options = SessionOptions::new(
                builtin_option_definitions(
                    MODEL,
                    [Arc::from(MODEL)],
                    false,
                    false,
                    false,
                    ThinkingConfig::Off,
                ),
                &Default::default(),
            )
            .unwrap()
            .snapshot();
            writer
                .coordinator_checkpoint()
                .checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision: 1,
                        epoch: 1,
                    },
                    snapshot: Arc::new(SessionCheckpoint {
                        history: Some(Arc::new(vec![Message::user("coordinator".into())])),
                        model: Arc::from(MODEL),
                        cwd: CWD.into(),
                        options,
                        config: None,
                    }),
                })
                .await
                .unwrap();

            session.replace_messages(vec![Message::user("tui".into())]);
            writer.send(Arc::new(session));
            writer.shutdown(DRAIN_TIMEOUT);

            let loaded = AppSession::load(id, &dir).unwrap();
            assert_eq!(message_texts(&loaded), ["tui"]);
        });
    }

    /// A retiring coordinator's history flush can still be pending when the
    /// TUI rewinds the same session. Merging that stale history into every
    /// later TUI snapshot silently undid the rewind on disk.
    #[test_case(true, &[COORDINATOR_MSG] ; "matching_history_adopts_pending_coordinator_history")]
    #[test_case(false, &[REWOUND_MSG] ; "divergent_history_discards_pending_coordinator_history")]
    fn tui_snapshot_merges_pending_coordinator_history_only_from_its_base(
        history_matches_base: bool,
        expected: &[&str],
    ) {
        let (wake, _wake_rx) = flume::unbounded();
        let (_done_tx, done_rx) = flume::bounded(1);
        let writer = StorageWriter {
            pending: Arc::default(),
            wake,
            done_rx,
            stop: Arc::default(),
        };
        let mut session = AppSession::new(MODEL, CWD);
        session.push_message(Message::user(BASE_MSG.into()));
        let id = session.id;
        writer.seed(Arc::new(session.clone()));

        let options = SessionOptions::new(
            builtin_option_definitions(
                MODEL,
                [Arc::from(MODEL)],
                false,
                false,
                false,
                ThinkingConfig::Off,
            ),
            &Default::default(),
        )
        .unwrap()
        .snapshot();
        let pending_ack = writer
            .coordinator_checkpoint()
            .checkpoint(CheckpointRequest {
                session_id: id,
                version: CheckpointVersion {
                    revision: 1,
                    epoch: 1,
                },
                snapshot: Arc::new(SessionCheckpoint {
                    history: Some(Arc::new(vec![Message::user(COORDINATOR_MSG.into())])),
                    model: Arc::from(MODEL),
                    cwd: CWD.into(),
                    options,
                    config: None,
                }),
            });

        if !history_matches_base {
            session.replace_messages(vec![Message::user(REWOUND_MSG.into())]);
        }
        for _ in 0..2 {
            writer.send(Arc::new(session.clone()));
            let state = lock(&writer.pending);
            let Some(Entry::Save(save)) = state.entries.get(&id) else {
                unreachable!()
            };
            assert_eq!(message_texts(&save.session), expected);
            assert_eq!(
                save.coordinator_history_base.is_some(),
                history_matches_base
            );
        }
        let (_tmp, dir) = state_dir();
        let (warn_tx, _warn_rx) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        disk.flush(&writer.pending);
        let ack = smol::block_on(pending_ack);
        if history_matches_base {
            assert!(ack.is_ok());
        } else {
            let Err(CheckpointError::Save { message, .. }) = ack else {
                panic!("superseded history checkpoint must fail")
            };
            assert_eq!(message.as_ref(), CHECKPOINT_REPLACED);
        }
        assert_eq!(
            message_texts(&AppSession::load(id, &dir).unwrap()),
            expected
        );
    }

    #[test]
    fn failed_coordinator_checkpoint_is_not_a_later_tui_merge_base() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            block_sessions_dir(&dir);
            let (writer, warn_rx) = writer(&dir);
            let session = AppSession::new(MODEL, CWD);
            let id = session.id;
            writer.send(Arc::new(session.clone()));
            let warning = warn_rx.recv_async().await.unwrap();
            assert!(warning.starts_with(SAVE_FAILED_PREFIX), "{warning}");

            let options = SessionOptions::new(
                builtin_option_definitions(
                    FAILED_MODEL,
                    [Arc::from(FAILED_MODEL)],
                    true,
                    true,
                    true,
                    ThinkingConfig::Off,
                ),
                &Default::default(),
            )
            .unwrap()
            .snapshot();
            let result = writer
                .coordinator_checkpoint()
                .checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision: 1,
                        epoch: 1,
                    },
                    snapshot: Arc::new(SessionCheckpoint {
                        history: None,
                        model: Arc::from(FAILED_MODEL),
                        cwd: CWD.into(),
                        options,
                        config: None,
                    }),
                })
                .await;
            assert!(matches!(result, Err(CheckpointError::Save { .. })));

            std::fs::remove_file(dir.path().join(SESSIONS_DIR)).unwrap();
            writer.send(Arc::new(session));
            writer.shutdown(DRAIN_TIMEOUT);

            let loaded = AppSession::load(id, &dir).unwrap();
            assert_eq!(loaded.model, MODEL);
            assert!(loaded.meta.yolo.is_none());
            assert!(!loaded.meta.fast);
            assert!(!loaded.meta.workflow);
            assert!(loaded.meta.session_options.is_empty());
        });
    }

    #[test]
    fn failed_checkpoint_retries_without_another_wake() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            block_sessions_dir(&dir);
            let (writer, warn_rx) = writer(&dir);
            let session = AppSession::new(MODEL, CWD);
            let id = session.id;
            let version = CheckpointVersion {
                revision: session.revision(),
                epoch: 1,
            };
            let ack = writer.checkpoint(CheckpointRequest {
                session_id: id,
                version,
                snapshot: Arc::new(session),
            });

            let warning = warn_rx.recv_async().await.unwrap();
            assert!(warning.starts_with(SAVE_FAILED_PREFIX), "{warning}");
            std::fs::remove_file(dir.path().join(SESSIONS_DIR)).unwrap();

            assert_eq!(ack.await.unwrap().version, version);
            assert_eq!(warn_rx.recv_async().await.unwrap(), SAVE_RECOVERED);
            writer.shutdown(DRAIN_TIMEOUT);
            assert!(AppSession::load(id, &dir).is_ok());
        });
    }

    /// Drives the flushes by hand: the threaded writer leaves the gap
    /// between the failure warning, the requeue and the retry deadline to the
    /// scheduler, so asserting on them raced under load.
    #[test]
    fn failed_checkpoint_does_not_delay_another_session() {
        let (_tmp, dir) = state_dir();
        let sessions_dir = dir.path().join(SESSIONS_DIR);
        std::fs::create_dir(&sessions_dir).unwrap();
        let (writer, _wake_rx) = manual_writer();
        let (warn_tx, warn_rx) = flume::unbounded();
        let mut disk = Writer {
            dir: dir.clone(),
            warn_tx,
            logs: HashMap::new(),
            failing: HashSet::new(),
        };
        let retry_state = |id| match lock(&writer.pending).entries.get(&id) {
            Some(Entry::Save(save)) => (save.retry_attempt, save.retry_at),
            _ => panic!("blocked session entry missing"),
        };

        let blocked = AppSession::new(MODEL, CWD);
        let blocked_id = blocked.id;
        let blocked_tmp = sessions_dir.join(format!("{blocked_id}.jsonl.tmp"));
        std::fs::create_dir(&blocked_tmp).unwrap();
        let blocked_ack = writer.checkpoint(CheckpointRequest {
            session_id: blocked_id,
            version: CheckpointVersion {
                revision: blocked.revision(),
                epoch: 1,
            },
            snapshot: Arc::new(blocked),
        });
        disk.flush(&writer.pending);
        let warning = warn_rx.try_recv().unwrap();
        assert!(warning.starts_with(SAVE_FAILED_PREFIX), "{warning}");
        let blocked_retry_at = Instant::now() + DEFERRED_RETRY;
        if let Some(Entry::Save(save)) = lock(&writer.pending).entries.get_mut(&blocked_id) {
            save.retry_at = Some(blocked_retry_at);
        }
        let blocked_retry = retry_state(blocked_id);

        let healthy = AppSession::new(MODEL, CWD);
        let healthy_id = healthy.id;
        let healthy_version = CheckpointVersion {
            revision: healthy.revision(),
            epoch: 1,
        };
        let healthy_ack = writer.checkpoint(CheckpointRequest {
            session_id: healthy_id,
            version: healthy_version,
            snapshot: Arc::new(healthy),
        });
        let next_wake = disk.flush(&writer.pending);
        let ack = smol::block_on(healthy_ack).expect("healthy checkpoint was delayed or failed");
        assert_eq!(ack.version, healthy_version);
        assert_eq!(
            retry_state(blocked_id),
            blocked_retry,
            "the failed session must keep backing off while another session saves"
        );
        assert_eq!(next_wake, Some(blocked_retry_at));

        std::fs::remove_dir(blocked_tmp).unwrap();
        if let Some(Entry::Save(save)) = lock(&writer.pending).entries.get_mut(&blocked_id) {
            save.retry_at = None;
        }
        disk.flush(&writer.pending);
        assert!(smol::block_on(blocked_ack).is_ok());
        assert!(AppSession::load(healthy_id, &dir).is_ok());
        assert!(AppSession::load(blocked_id, &dir).is_ok());
    }

    #[test]
    fn failed_checkpoint_exhausts_retries_without_false_ack() {
        smol::block_on(async {
            let (_tmp, dir) = state_dir();
            block_sessions_dir(&dir);
            let (writer, warn_rx) = writer(&dir);
            let session = AppSession::new(MODEL, CWD);
            let id = session.id;
            let result = writer
                .checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision: session.revision(),
                        epoch: 1,
                    },
                    snapshot: Arc::new(session),
                })
                .await;

            assert!(matches!(
                result,
                Err(CheckpointError::Save { session_id, .. }) if session_id == id
            ));
            let warning = warn_rx.recv_async().await.unwrap();
            assert!(warning.starts_with(SAVE_FAILED_PREFIX), "{warning}");
            writer.shutdown(DRAIN_TIMEOUT);
        });
    }

    #[test]
    fn delete_discards_pending_snapshot() {
        let (_tmp, dir) = state_dir();
        let (writer, _warn_rx) = writer(&dir);
        let session = AppSession::new("test-model", "/tmp/c");
        let id = session.id;
        writer.send(Arc::new(session));
        let (done_tx, done_rx) = flume::bounded(1);
        writer.delete(id, move |res| {
            let _ = done_tx.send(res);
        });
        writer.shutdown(DRAIN_TIMEOUT);

        assert!(done_rx.recv().unwrap().is_ok());
        assert!(AppSession::load(id, &dir).is_err());
    }

    /// A fresh writer over an existing file has no cursor, so it re-opens the
    /// log and gets cursors for the loaded session, not the live one. The first
    /// append must diverge into a full rewrite instead of landing on stale
    /// offsets.
    #[test]
    fn reopened_log_rewrites_diverged_file_instead_of_appending() {
        let (_tmp, dir) = state_dir();
        let mut session = AppSession::new(MODEL, CWD);
        let id = session.id;
        for i in 0..5 {
            session.push_message(user_message(i));
        }
        let (first, _first_warn_rx) = writer(&dir);
        first.send(Arc::new(session.clone()));
        first.shutdown(DRAIN_TIMEOUT);

        session.truncate_messages(2);
        session.push_message(Message::user(RESUMED_MSG.into()));
        session.insert_tool_output(
            TOOL_ID.into(),
            ToolOutput::Plain(TOOL_TEXT.to_string().into()),
        );
        session.set_title(TITLE.into());

        let (second, second_warn_rx) = writer(&dir);
        second.send(Arc::new(session.clone()));
        second.shutdown(DRAIN_TIMEOUT);

        let loaded = AppSession::load(id, &dir).unwrap();
        assert_eq!(
            message_texts(&loaded),
            [msg_text(0), msg_text(1), RESUMED_MSG.to_string()]
        );
        assert_eq!(loaded.title, TITLE);
        match loaded.tool_outputs().get(TOOL_ID).map(Arc::as_ref) {
            Some(ToolOutput::Plain(out)) => assert_eq!(out.text, TOOL_TEXT),
            other => panic!("tool output lost: {other:?}"),
        }
        assert!(second_warn_rx.is_empty());
    }

    /// A disk that keeps failing warns once, not once per frame, and says so
    /// exactly once when writes start working again.
    #[test]
    fn failing_flush_warns_once_and_reports_recovery() {
        let (_tmp, dir) = state_dir();
        block_sessions_dir(&dir);
        let (writer, warn_rx) = writer(&dir);
        let session = Arc::new(AppSession::new(MODEL, CWD));
        let id = session.id;

        writer.send(Arc::clone(&session));
        let warning = warn_rx.recv_timeout(DRAIN_TIMEOUT).unwrap();
        assert!(warning.starts_with(SAVE_FAILED_PREFIX), "{warning}");

        // The save is enqueued before the delete, so the flush that runs the
        // delete has already drained it; a repeat failure must stay silent.
        writer.send(Arc::clone(&session));
        let (done_tx, done_rx) = flume::bounded(1);
        writer.delete(MakiId::generate(), move |res| {
            let _ = done_tx.send(res);
        });
        assert!(done_rx.recv_timeout(DRAIN_TIMEOUT).unwrap().is_err());
        assert!(warn_rx.is_empty(), "second failure warned again");

        std::fs::remove_file(dir.path().join(SESSIONS_DIR)).unwrap();
        writer.send(session);
        let recovered = warn_rx.recv_timeout(DRAIN_TIMEOUT).unwrap();
        assert_eq!(recovered, SAVE_RECOVERED);
        writer.shutdown(DRAIN_TIMEOUT);

        assert!(warn_rx.is_empty());
        assert!(AppSession::load(id, &dir).is_ok());
    }

    /// A save enqueued after a delete must win: clear a draft and retype it
    /// fast enough, and the queued delete used to unlink the file the retype
    /// had just saved.
    #[test]
    fn save_enqueued_after_delete_survives() {
        let (_tmp, dir) = state_dir();
        let (writer, warn_rx) = writer(&dir);
        let mut session = AppSession::new(MODEL, CWD);
        let id = session.id;
        session.push_message(user_message(0));
        writer.send(Arc::new(session.clone()));
        writer.delete(id, |_| {});
        session.push_message(Message::user(RESUMED_MSG.into()));
        writer.send(Arc::new(session));
        writer.shutdown(DRAIN_TIMEOUT);

        let loaded = AppSession::load(id, &dir).unwrap();
        assert_eq!(
            message_texts(&loaded),
            [msg_text(0), RESUMED_MSG.to_string()]
        );
        assert!(warn_rx.is_empty());
    }

    #[test]
    fn failed_write_is_retried_without_an_external_wake() {
        let (_tmp, dir) = state_dir();
        block_sessions_dir(&dir);
        let (writer, warn_rx) = writer(&dir);
        let session = Arc::new(AppSession::new(MODEL, CWD));
        let id = session.id;

        writer.send(session);
        let warning = warn_rx.recv_timeout(DRAIN_TIMEOUT).unwrap();
        assert!(warning.starts_with(SAVE_FAILED_PREFIX), "{warning}");

        std::fs::remove_file(dir.path().join(SESSIONS_DIR)).unwrap();
        assert_eq!(warn_rx.recv_timeout(DRAIN_TIMEOUT).unwrap(), SAVE_RECOVERED);
        assert!(AppSession::load(id, &dir).is_ok());
        writer.shutdown(DRAIN_TIMEOUT);
    }

    /// After a delete the cursor still holds an open handle to the unlinked
    /// file, which still looks unchanged, so an append would write the session
    /// into nothing. Forgetting the cursor makes the next snapshot write a
    /// whole file.
    #[test]
    fn session_recreated_after_delete_is_written_in_full() {
        let (_tmp, dir) = state_dir();
        let (writer, warn_rx) = writer(&dir);
        let mut session = AppSession::new(MODEL, CWD);
        let id = session.id;
        session.push_message(user_message(0));
        writer.send(Arc::new(session.clone()));

        let (done_tx, done_rx) = flume::bounded(1);
        writer.delete(id, move |res| {
            let _ = done_tx.send(res);
        });
        done_rx.recv_timeout(DRAIN_TIMEOUT).unwrap().unwrap();
        assert!(AppSession::load(id, &dir).is_err());

        session.push_message(Message::user(RESUMED_MSG.into()));
        writer.send(Arc::new(session));
        writer.shutdown(DRAIN_TIMEOUT);

        let loaded = AppSession::load(id, &dir).unwrap();
        assert_eq!(
            message_texts(&loaded),
            [msg_text(0), RESUMED_MSG.to_string()]
        );
        assert!(warn_rx.is_empty());
    }
}
