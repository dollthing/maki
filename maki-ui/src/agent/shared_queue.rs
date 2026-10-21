//! Queue of work handed from the UI to the agent actor.
//!
//! This is a thin presentation facade over the actor's single scheduling
//! queue. Production uses the actor-backed variant only: `push` translates a
//! TUI [`QueueItem`] into acknowledged ordinary submissions or compact work,
//! and every read projects the selected actor's queue.
//! There is no second scheduling deque in production.
//!
//! The `#[cfg(test)]` variant is a presentation-only deque used exclusively
//! by App unit tests that need deterministic queue-panel behavior without an
//! actor runner racing their assertions. It never compiles into production.

use std::borrow::Cow;
use std::sync::Arc;

#[cfg(test)]
use std::collections::VecDeque;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(test)]
use std::sync::{Mutex, MutexGuard, PoisonError};

use maki_agent::actor::{
    ActorError, AgentActorHandle, PendingQueueEntry, TurnAdmissionOptions, TurnOrigin,
    TurnProvenance,
};
use maki_agent::{AgentInput, ImageSource, TurnId};
use maki_commands::COMPACT_COMMAND_NAME;
use tracing::warn;

use crate::components::input::Submission;
use crate::components::queue_panel::QueueEntry;
use crate::theme;

pub(crate) struct QueuedMessage {
    pub(crate) text: String,
    pub(crate) images: Vec<ImageSource>,
}

impl From<Submission> for QueuedMessage {
    fn from(sub: Submission) -> Self {
        Self {
            text: sub.text,
            images: sub.images,
        }
    }
}

enum QueueProjection {
    Message { text: String, displayed: bool },
    Compact(Option<String>),
}

pub(crate) enum QueueItem {
    Message {
        text: String,
        input: AgentInput,
        run_id: u64,
        /// `true` when the UI already drew the bubble (immediate dispatch).
        /// Consumption retains this flag so the UI does not draw it twice.
        /// `false` when the user typed while the agent was busy: the UI waits
        /// for `QueueItemConsumed` before drawing.
        displayed: bool,
    },
    #[cfg(test)]
    Compact {
        run_id: u64,
        instructions: Option<String>,
    },
}

/// The actual storage behind a [`QueueSender`]. Production always uses
/// [`QueueBackend::Actor`], delegating every operation to the actor's single
/// queue. The test variant is compiled out of production builds.
#[derive(Clone)]
pub(crate) enum QueueBackend {
    /// The actor's scheduling queue, via its handle.
    Actor(Arc<AgentActorHandle>),
    /// Presentation-only deque for deterministic App unit tests; never
    /// compiled into production and never a scheduling source. It owns its
    /// own presentation counter because there is no actor to own one.
    #[cfg(test)]
    Test {
        items: Arc<Mutex<VecDeque<QueueItem>>>,
        presentation: Arc<AtomicU64>,
    },
}

/// Reserves the next presentation id in the test fixture, mirroring the
/// actor's policy of never reusing an id and never going below the floor.
#[cfg(test)]
fn reserve_test_presentation(presentation: &AtomicU64, displayed: u64) -> u64 {
    let mut allocated = presentation.load(Ordering::Relaxed);
    loop {
        let next = allocated.max(displayed) + 1;
        match presentation.compare_exchange_weak(
            allocated,
            next,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return next,
            Err(current) => allocated = current,
        }
    }
}

/// Actor-backed queue facade shared with the app. Clones all reference the
/// same backend, so every push lands in the same actor queue (or, in tests,
/// the same deterministic deque).
#[derive(Clone)]
pub(crate) struct QueueSender {
    backend: QueueBackend,
}

/// Actor-backed facade used by production `AgentHandles`.
pub(crate) fn actor_queue(actor: Arc<AgentActorHandle>) -> QueueSender {
    QueueSender {
        backend: QueueBackend::Actor(actor),
    }
}

/// Test-only fixture: a deterministic presentation deque with no actor.
/// Returns a sender App tests can drive synchronously.
#[cfg(test)]
pub(crate) fn queue() -> QueueSender {
    let items: Arc<Mutex<VecDeque<QueueItem>>> = Arc::new(Mutex::new(VecDeque::new()));
    QueueSender {
        backend: QueueBackend::Test {
            items,
            presentation: Arc::new(AtomicU64::new(0)),
        },
    }
}

#[cfg(test)]
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Correlation the TUI stamps on actor root/turn admissions. Purely
/// presentation: the actor owns identity and the TUI only correlates events.
pub(crate) fn correlation(run_id: u64) -> String {
    format!("r{run_id}")
}

impl QueueSender {
    pub(crate) fn capture_cancel(&self, displayed: u64) -> (Option<TurnId>, u64) {
        match &self.backend {
            QueueBackend::Actor(actor) => actor.cancel_local(displayed),
            #[cfg(test)]
            QueueBackend::Test {
                items,
                presentation,
            } => {
                lock(items).clear();
                (None, reserve_test_presentation(presentation, displayed))
            }
        }
    }

    /// The presentation the app should treat as current. Production asks the
    /// actor, which owns the counter; the test fixture keeps its own.
    pub(crate) fn current_run_id(&self) -> u64 {
        match &self.backend {
            QueueBackend::Actor(actor) => actor.presentation_id(),
            #[cfg(test)]
            QueueBackend::Test { presentation, .. } => presentation.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn set_run_id(&self, displayed: u64) {
        match &self.backend {
            QueueBackend::Actor(actor) => actor.observe_presentation(displayed),
            #[cfg(test)]
            QueueBackend::Test { presentation, .. } => {
                presentation.fetch_max(displayed, Ordering::Relaxed);
            }
        }
    }

    pub(crate) fn reserve_run_id(&self, displayed: u64) -> u64 {
        match &self.backend {
            QueueBackend::Actor(actor) => actor.reserve_presentation(displayed),
            #[cfg(test)]
            QueueBackend::Test { presentation, .. } => {
                reserve_test_presentation(presentation, displayed)
            }
        }
    }

    pub(crate) fn push(&self, entry: QueueItem) {
        if let Err(error) = self.try_push(entry) {
            warn!(error = %error, "agent queue push failed");
        }
    }

    pub(crate) fn try_push(&self, entry: QueueItem) -> Result<(), ActorError> {
        match &self.backend {
            #[cfg(test)]
            QueueBackend::Test { items, .. } => lock(items).push_back(entry),
            QueueBackend::Actor(actor) => push_to_actor(actor, entry)?,
        }
        Ok(())
    }

    /// Removes the panel-visible item at `index`, returning whether a row
    /// was removed. Production delegates to the actor's queue removal; the
    /// test facade removes directly from its presentation deque.
    pub(crate) fn remove(&self, index: usize) -> bool {
        match &self.backend {
            #[cfg(test)]
            QueueBackend::Test { items, .. } => {
                let mut items = lock(items);
                let raw_index = items
                    .iter()
                    .enumerate()
                    .filter(|(_, item)| visible_in_panel(&(*item).into()))
                    .nth(index)
                    .map(|(index, _)| index);
                raw_index.and_then(|index| items.remove(index)).is_some()
            }
            QueueBackend::Actor(actor) => {
                match actor
                    .pending_queue()
                    .into_iter()
                    .filter(|entry| match entry {
                        PendingQueueEntry::Message(entry) => !entry.already_displayed,
                        PendingQueueEntry::Compact { .. } => true,
                    })
                    .nth(index)
                {
                    Some(PendingQueueEntry::Message(entry)) => {
                        actor.remove_submission(entry.id).is_some()
                    }
                    Some(PendingQueueEntry::Compact { operation_id, .. }) => {
                        actor.remove_pending_compact(operation_id).is_some()
                    }
                    None => false,
                }
            }
        }
    }

    pub(crate) fn len(&self) -> usize {
        match &self.backend {
            #[cfg(test)]
            QueueBackend::Test { items, .. } => lock(items).len(),
            QueueBackend::Actor(_) => self.projections().len(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[cfg(test)]
    pub(crate) fn clear(&self) {
        match &self.backend {
            QueueBackend::Test { items, .. } => lock(items).clear(),
            QueueBackend::Actor(actor) => {
                let _ = actor.clear();
            }
        }
    }

    /// Clears what the user queued while keeping child-result deliveries.
    pub(crate) fn clear_user_work(&self) {
        match &self.backend {
            #[cfg(test)]
            QueueBackend::Test { items, .. } => lock(items).clear(),
            QueueBackend::Actor(actor) => {
                let _ = actor.clear_user_work();
            }
        }
    }

    pub(crate) fn text_messages(&self) -> Vec<String> {
        self.projections()
            .into_iter()
            .filter_map(|entry| match entry {
                QueueProjection::Message {
                    text,
                    displayed: false,
                    ..
                } => Some(text),
                _ => None,
            })
            .collect()
    }

    pub(crate) fn panel_len(&self) -> usize {
        self.projections()
            .into_iter()
            .filter(visible_in_panel)
            .count()
    }

    pub(crate) fn panel_entries(&self) -> Vec<QueueEntry<'static>> {
        self.projections()
            .into_iter()
            .filter(visible_in_panel)
            .map(|entry| as_queue_entry(&entry))
            .collect()
    }

    fn projections(&self) -> Vec<QueueProjection> {
        match &self.backend {
            #[cfg(test)]
            QueueBackend::Test { items, .. } => lock(items).iter().map(Into::into).collect(),
            QueueBackend::Actor(actor) => actor
                .pending_queue()
                .into_iter()
                .map(|entry| match entry {
                    PendingQueueEntry::Message(entry) => QueueProjection::Message {
                        text: entry.text,
                        displayed: entry.already_displayed,
                    },
                    PendingQueueEntry::Compact { instructions, .. } => {
                        QueueProjection::Compact(instructions)
                    }
                })
                .collect(),
        }
    }
}

fn push_to_actor(actor: &AgentActorHandle, entry: QueueItem) -> Result<(), ActorError> {
    match entry {
        QueueItem::Message {
            input,
            run_id,
            displayed,
            ..
        } => {
            actor.submit(
                input,
                None,
                correlation(run_id),
                TurnAdmissionOptions {
                    provenance: TurnProvenance {
                        origin: TurnOrigin::User,
                        ..Default::default()
                    },
                    already_displayed: displayed,
                    // Only a bubble the UI already drew keeps its id; a
                    // deferred item gets a fresh presentation when it runs.
                    displayed_presentation: displayed.then_some(run_id),
                    ..Default::default()
                },
            )?;
            Ok(())
        }
        #[cfg(test)]
        QueueItem::Compact {
            run_id,
            instructions,
        } => actor.push_compact(run_id, instructions),
    }
}

fn visible_in_panel(entry: &QueueProjection) -> bool {
    match entry {
        QueueProjection::Message { displayed, .. } => !displayed,
        QueueProjection::Compact(_) => true,
    }
}

fn as_queue_entry(entry: &QueueProjection) -> QueueEntry<'static> {
    match entry {
        QueueProjection::Message { text, .. } => QueueEntry {
            text: Cow::Owned(text.clone()),
            color: theme::current().foreground,
        },
        QueueProjection::Compact(instructions) => QueueEntry {
            text: match instructions {
                Some(extra) => Cow::Owned(format!("{COMPACT_COMMAND_NAME} {extra}")),
                None => Cow::Borrowed(COMPACT_COMMAND_NAME),
            },
            color: theme::current()
                .queue
                .fg
                .unwrap_or(theme::current().foreground),
        },
    }
}

impl From<&QueueItem> for QueueProjection {
    fn from(item: &QueueItem) -> Self {
        match item {
            QueueItem::Message {
                text, displayed, ..
            } => QueueProjection::Message {
                text: text.clone(),
                displayed: *displayed,
            },
            #[cfg(test)]
            QueueItem::Compact { instructions, .. } => {
                QueueProjection::Compact(instructions.clone())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    fn msg(displayed: bool) -> QueueItem {
        QueueItem::Message {
            text: "t".into(),
            input: AgentInput {
                message: String::new(),
                mode: Default::default(),
                images: Vec::new(),
                preamble: Vec::new(),
                thinking: Default::default(),
                fast: false,
                workflow: false,
                prompt: None,
                cancel: None,
            },
            run_id: 0,
            displayed,
        }
    }

    #[test_case(msg(false), true  ; "deferred_message_visible")]
    #[test_case(msg(true),  false ; "displayed_message_hidden")]
    #[test_case(QueueItem::Compact { run_id: 0, instructions: None }, true  ; "compact_visible")]
    fn panel_visibility(item: QueueItem, visible: bool) {
        let tx = queue();
        tx.push(item);
        let expected = usize::from(visible);
        assert_eq!(tx.panel_len(), expected);
        assert_eq!(tx.panel_entries().len(), expected);
    }

    #[test]
    fn acknowledged_submission_updates_shared_presentation() {
        let tx = queue();
        let mut item = msg(false);
        if let QueueItem::Message { run_id, .. } = &mut item {
            *run_id = 9;
        }
        tx.try_push(item).unwrap();
        // Pushing no longer advances the counter: the app reserves the
        // presentation it will use and pushes with that id.
        assert_eq!(tx.current_run_id(), 0);
        assert_eq!(tx.reserve_run_id(9), 10);
        assert_eq!(tx.current_run_id(), 10);
        assert_eq!(tx.text_messages(), ["t"]);
        assert_eq!(tx.clone().panel_len(), 1);
    }

    #[test]
    fn remove_reports_panel_row_removal() {
        let tx = queue();
        assert!(!tx.remove(0), "empty queue removes nothing");
        tx.push(msg(false));
        assert!(tx.remove(0), "visible row removed");
        assert!(tx.is_empty());
        assert!(!tx.remove(0), "queue is empty again");
    }
}
