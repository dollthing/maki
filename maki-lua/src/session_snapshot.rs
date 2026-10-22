//! The one shape `maki.session.read()` returns. The UI and the headless
//! drivers all serialize this struct, so a plugin sees the same keys wherever
//! it runs and a new field is one edit instead of three `json!` blobs.

use std::sync::{Arc, Mutex, MutexGuard};

use maki_agent::{AgentEvent, AgentRef, Envelope};
use maki_providers::{TokenUsage, add_cost};
use maki_storage::id::MakiId;
use serde::Serialize;

use crate::{EventHandle, agent_autocmd};

const ROOT_CONFIG_UNAVAILABLE: &str = "session root configuration is unavailable";
pub const STATUS_IDLE: &str = "idle";
pub const STATUS_WORKING: &str = "working";
pub const STATUS_NEEDS_INPUT: &str = "needs_input";

#[derive(Serialize)]
pub struct SessionSnapshot {
    pub id: String,
    /// Working directory the session runs against.
    pub cwd: String,
    /// Left out when the host has no title to give. Never filled with the
    /// session id, so a plugin can tell "no title" from a real one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Model spec as shown in the status bar (`provider/id`).
    pub model: String,
    pub fast: bool,
    pub thinking: String,
    /// The committed mode's id: `build`, `plan` or a plugin mode.
    pub mode: String,
    /// [`STATUS_IDLE`], [`STATUS_WORKING`], or [`STATUS_NEEDS_INPUT`].
    pub status: &'static str,
    /// Whether this is the session the user is looking at. Always true
    /// for the single-session headless drivers.
    pub focused: bool,
    /// Unix seconds.
    pub updated_at: u64,
    /// Omitted by hosts that hold no queue of their own. An sdk client can
    /// pipeline messages that wait outside the driver's sight, so leaving
    /// this out beats reporting a zero the plugin would trust.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue: Option<SessionQueueSnapshot>,
    /// Subagent turns included, their tokens are real spend on this session
    /// too.
    pub usage: TokenUsage,
    /// Main session only. A subagent fills its own window, not ours.
    pub context_size: u32,
    pub context_window: u32,
    /// What the session bills, `None` on an unpriced model. No list price
    /// twin here, because `cost` is re-settled from stored usage when a
    /// session resumes and list price is not stored. Per turn list price
    /// lives on the `TurnEnd` autocmd instead.
    pub cost: Option<f64>,
}

#[derive(Serialize)]
pub struct SessionQueueSnapshot {
    /// Pending user text only. A queued `/compact` is housekeeping, not human
    /// input a supervisor should yield to.
    pub count: usize,
}

#[derive(Default)]
struct Totals {
    usage: TokenUsage,
    cost: Option<f64>,
    context_size: u32,
    working: bool,
}

/// A poisoned lock only means some other run panicked mid-update. The
/// numbers are still whole, so read them instead of losing the session's
/// whole accounting over it.
fn lock(totals: &Mutex<Totals>) -> MutexGuard<'_, Totals> {
    totals.lock().unwrap_or_else(|e| e.into_inner())
}

/// What a headless driver (print, SDK, ACP) feeds every envelope of its one
/// session to: the totals `maki.session.read` answers from and the Lua turn
/// autocmds, so plugins see the same events and numbers as under the TUI. The
/// driver writes while the Lua thread reads, hence the lock, though the two
/// rarely meet.
#[derive(Clone)]
pub struct HeadlessObserver {
    handle: EventHandle,
    session_id: MakiId,
    totals: Arc<Mutex<Totals>>,
}

impl HeadlessObserver {
    pub fn new(handle: EventHandle, session_id: MakiId) -> Self {
        Self {
            handle,
            session_id,
            totals: Arc::default(),
        }
    }

    /// Folds one envelope into the totals, then fires its autocmd. Folded
    /// first, so a plugin handling `TurnEnd` finds the finished totals when
    /// it calls `maki.session.read()`.
    pub fn observe(&self, envelope: &Envelope) {
        self.fold(envelope);
        agent_autocmd::dispatch(
            &self.handle,
            &self.session_id,
            envelope,
            envelope.subagent.is_some(),
        );
    }

    fn fold(&self, envelope: &Envelope) {
        let event = &envelope.event;
        let mut totals = lock(&self.totals);
        // Envelopes only flow while a run is alive, so anything that is not
        // an ending means the agent is busy. Sdk mode runs many turns and no
        // driver has to remember to clear a flag between them.
        totals.working = !matches!(
            event,
            AgentEvent::TurnOutcome(_)
                | AgentEvent::ControlComplete { .. }
                | AgentEvent::ControlError { .. }
        );
        match event {
            AgentEvent::TurnOutcome(outcome) => {
                totals.usage += outcome.usage();
                add_cost(&mut totals.cost, outcome.cost());
                totals.context_size = outcome.context_size();
            }
            AgentEvent::TurnComplete(turn) if envelope.subagent.is_none() => {
                totals.context_size = turn.context_size.unwrap_or(totals.context_size);
            }
            _ => {}
        }
    }

    /// Installs the provider `maki.session.read` answers from, for the
    /// session whose root is {root}. Model, thinking, fast, mode, cwd and
    /// context window come from the root's committed config at read time,
    /// the same source the TUI reads, so a `/model` or mode change shows up
    /// at once.
    ///
    /// There is no rename path under the headless drivers, so `title` stays
    /// nil rather than echoing the id back as a fake one.
    pub fn install(&self, root: AgentRef) {
        let totals = Arc::clone(&self.totals);
        let id = self.session_id;
        self.handle.install_session_snapshot(Box::new(move |want| {
            // One session here, so any other id names a tab that is not ours.
            if want != id {
                return Err(format!("session {want} not live"));
            }
            let config = root
                .effective_config()
                .map_err(|error| error.to_string())?
                .ok_or(ROOT_CONFIG_UNAVAILABLE)?;
            let totals = lock(&totals);
            serde_json::to_value(SessionSnapshot {
                id: id.to_string(),
                cwd: config.context.cwd().to_string_lossy().into_owned(),
                title: None,
                model: config.model.spec(),
                fast: config.fast,
                thinking: config.thinking.to_string(),
                mode: config.mode.id().key().to_owned(),
                status: if totals.working {
                    STATUS_WORKING
                } else {
                    STATUS_IDLE
                },
                focused: true,
                updated_at: maki_storage::now_epoch(),
                queue: None,
                usage: totals.usage,
                context_size: totals.context_size,
                context_window: config.model.context_window,
                cost: totals.cost,
            })
            .map_err(|e| e.to_string())
        }));
    }
}

#[cfg(test)]
mod tests {
    use maki_agent::{AgentId, DoneReason, TurnCompleteEvent, TurnId, TurnOutcome};
    use maki_providers::Message;

    use super::*;

    const FIRST_TURN_INPUT: u32 = 1_000;
    const SECOND_TURN_INPUT: u32 = 400;
    const TURN_COST: f64 = 0.25;
    const WINDOW: u32 = 200_000;
    const GROWS_MSG: &str = "session totals add up across turns, they never restart at one turn";
    const WORKING_MSG: &str = "a second turn has to read as working again";

    fn envelope(event: AgentEvent) -> Envelope {
        Envelope {
            event,
            subagent: None,
            identity: None,
        }
    }

    /// Carries no usage or cost of its own, so a fold that summed these instead
    /// of `Done` would leave the totals empty and fail below.
    fn turn_complete(context_size: u32) -> Envelope {
        envelope(AgentEvent::TurnComplete(Box::new(TurnCompleteEvent {
            message: Message::user("go".into()),
            usage: TokenUsage::default(),
            model: String::new(),
            cost: None,
            context_size: Some(context_size),
            context_window: WINDOW,
        })))
    }

    fn done(input: u32) -> Envelope {
        envelope(AgentEvent::TurnOutcome(TurnOutcome::Completed {
            agent_id: AgentId::generate(),
            turn_id: TurnId::generate(),
            usage: TokenUsage {
                input,
                ..Default::default()
            },
            cost: Some(TURN_COST),
            list_cost: None,
            context_size: input,
            context_window: WINDOW,
            num_turns: 1,
            reason: DoneReason::EndTurn,
        }))
    }

    /// Sdk mode serves many turns on one session, so `Done` is a turn boundary
    /// and not the end. Totals have to keep climbing and the status has to come
    /// back to life on turn two, with no driver clearing anything by hand.
    #[test]
    fn totals_grow_and_status_revives_across_turns() {
        let observer =
            HeadlessObserver::new(EventHandle::disconnected_for_test(), MakiId::generate());

        observer.fold(&turn_complete(FIRST_TURN_INPUT));
        assert!(lock(&observer.totals).working);
        observer.fold(&done(FIRST_TURN_INPUT));
        assert!(!lock(&observer.totals).working);

        observer.fold(&turn_complete(SECOND_TURN_INPUT));
        assert!(lock(&observer.totals).working, "{WORKING_MSG}");
        observer.fold(&done(SECOND_TURN_INPUT));

        let totals = lock(&observer.totals);
        assert!(!totals.working);
        assert_eq!(
            totals.usage.input,
            FIRST_TURN_INPUT + SECOND_TURN_INPUT,
            "{GROWS_MSG}"
        );
        assert_eq!(totals.cost, Some(TURN_COST * 2.0), "{GROWS_MSG}");
        assert_eq!(totals.context_size, SECOND_TURN_INPUT);
    }
}
