use std::collections::VecDeque;
use std::sync::{Arc, Weak};

use maki_providers::Message;

use super::{ActorInner, ActorLifecycle, TurnProvenance};
use crate::{AgentInput, TurnId};

pub const MAX_PENDING_INSTRUCTIONS: usize = 64;
pub const MAX_PENDING_INSTRUCTION_BYTES: usize = 256 * 1024;

/// Safe-boundary input source. All methods are synchronous: draining observations
/// and inserting them into history must be one no-await commit path.
pub trait TurnInbox: Send + Sync {
    fn drain_instructions(&self) -> Vec<AgentInput>;
    fn drain_completions(&self) -> Vec<Message>;
    fn commit_completions(&self, commit: &mut dyn FnMut(Message)) {
        for message in self.drain_completions() {
            commit(message);
        }
    }
    /// Closes admission only when every accepted instruction reached a request
    /// and no child completion is waiting to be consumed.
    fn finish(&self) -> bool;
    /// Closes admission once every accepted instruction reached a request,
    /// leaving pending completions for a successor turn.
    fn finish_instructions(&self) -> bool;
    fn close(&self) {}
    /// Acknowledge only previously drained instructions, after request submission.
    fn instructions_submitted(&self);
    fn has_unsubmitted_instructions(&self) -> bool;
}

pub(super) struct InstructionEntry {
    pub input: AgentInput,
    pub provenance: TurnProvenance,
    pub bytes: usize,
}

pub(super) struct InstructionInbox {
    pub open: bool,
    pub ticket: super::TurnTicket,
    pub pending: VecDeque<InstructionEntry>,
    pub drained: Vec<(TurnProvenance, usize)>,
    pub submit_operation: Option<u64>,
    pub policy: Option<Arc<super::EffectiveAgentConfig>>,
}

impl InstructionInbox {
    pub fn new(ticket: super::TurnTicket) -> Self {
        Self {
            open: true,
            ticket,
            pending: VecDeque::new(),
            drained: Vec::new(),
            submit_operation: None,
            policy: None,
        }
    }

    pub fn outstanding(&self) -> (usize, usize) {
        (
            self.pending.len() + self.drained.len(),
            self.pending.iter().map(|entry| entry.bytes).sum::<usize>()
                + self.drained.iter().map(|(_, bytes)| bytes).sum::<usize>(),
        )
    }
}

pub(super) struct ActorTurnInbox {
    pub inner: Weak<ActorInner>,
    pub turn_id: TurnId,
}

impl ActorTurnInbox {
    pub fn source(inner: &Arc<ActorInner>, turn_id: TurnId) -> Arc<dyn TurnInbox> {
        Arc::new(Self {
            inner: Arc::downgrade(inner),
            turn_id,
        })
    }

    fn close_admission(&self, await_completions: bool) -> bool {
        let Some(inner) = self.inner.upgrade() else {
            return true;
        };
        let mut state = inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let pending_completions = await_completions && !state.deliveries.is_empty();
        let Some(inbox) = state.instructions.get_mut(&self.turn_id) else {
            return true;
        };
        if inbox.outstanding().0 != 0 || pending_completions {
            return false;
        }
        inbox.open = false;
        true
    }
}

impl TurnInbox for ActorTurnInbox {
    fn drain_instructions(&self) -> Vec<AgentInput> {
        let Some(inner) = self.inner.upgrade() else {
            return Vec::new();
        };
        let mut state = inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.lifecycle != ActorLifecycle::Open {
            return Vec::new();
        }
        let Some(inbox) = state.instructions.get_mut(&self.turn_id) else {
            return Vec::new();
        };
        if !inbox.open {
            return Vec::new();
        }
        let mut inputs = Vec::with_capacity(inbox.pending.len());
        for entry in inbox.pending.drain(..) {
            inbox.drained.push((entry.provenance, entry.bytes));
            inputs.push(entry.input);
        }
        inputs
    }

    fn drain_completions(&self) -> Vec<Message> {
        let Some(inner) = self.inner.upgrade() else {
            return Vec::new();
        };
        super::delivery::drain(&inner, self.turn_id)
    }

    fn commit_completions(&self, commit: &mut dyn FnMut(Message)) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        super::delivery::commit(&inner, self.turn_id, commit);
    }

    fn finish(&self) -> bool {
        self.close_admission(true)
    }

    fn finish_instructions(&self) -> bool {
        self.close_admission(false)
    }

    fn close(&self) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let mut state = inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(inbox) = state.instructions.get_mut(&self.turn_id) {
            inbox.open = false;
        }
    }

    fn instructions_submitted(&self) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let mut state = inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(inbox) = state.instructions.get_mut(&self.turn_id) {
            inbox.drained.clear();
        }
    }

    fn has_unsubmitted_instructions(&self) -> bool {
        let Some(inner) = self.inner.upgrade() else {
            return false;
        };
        let state = inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state
            .instructions
            .get(&self.turn_id)
            .is_some_and(|inbox| inbox.outstanding().0 != 0)
    }
}
