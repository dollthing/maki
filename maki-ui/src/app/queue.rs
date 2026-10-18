//! Focused actor pending-message presentation.

use crate::theme;
use maki_agent::actor::{
    AgentActorHandle, PendingQueueEntry, SubmissionId, TurnAdmissionOptions, TurnOrigin,
    TurnProvenance,
};
use maki_agent::{AgentInput, TurnId};
use maki_commands::COMPACT_COMMAND_NAME;
use std::borrow::Cow;

use super::{Action, App, Status};

use crate::agent::shared_queue::{QueueItem, QueueSender};
use crate::components::queue_panel::QueueEntry;

pub(crate) use crate::agent::shared_queue::QueuedMessage;

pub(crate) const EMPTY_PROMPT_ERR: &str = "prompt is empty";
pub(crate) const NO_QUEUE_ERR: &str = "session cannot queue messages";
pub(crate) const NO_SUBAGENT_ERR: &str = "subagent is no longer accepting messages";

pub(crate) enum SubmitOutcome {
    Started(Vec<Action>),
    Queued,
    Rejected(String),
}

#[derive(Default)]
pub(crate) struct MessageQueue {
    shared: Option<QueueSender>,
    target: Option<AgentActorHandle>,
    target_id: Option<maki_agent::AgentId>,
    child_selected: bool,
    focused_submission: Option<PendingIdentity>,
    focus: Option<usize>,
    cancelled_through: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PendingIdentity {
    Message(SubmissionId),
    Compact(u64),
}

fn pending_identity(entry: &PendingQueueEntry) -> PendingIdentity {
    match entry {
        PendingQueueEntry::Message(entry) => PendingIdentity::Message(entry.id),
        PendingQueueEntry::Compact { operation_id, .. } => PendingIdentity::Compact(*operation_id),
    }
}

fn visible_pending(actor: &AgentActorHandle) -> Vec<PendingQueueEntry> {
    actor
        .pending_queue()
        .into_iter()
        .filter(|entry| match entry {
            PendingQueueEntry::Message(entry) => !entry.already_displayed,
            PendingQueueEntry::Compact { .. } => true,
        })
        .collect()
}

impl MessageQueue {
    pub(crate) fn select(
        &mut self,
        id: Option<maki_agent::AgentId>,
        child: bool,
        actor: Option<AgentActorHandle>,
    ) {
        if self.target_id != id || self.child_selected != child {
            self.unfocus();
        }
        self.target_id = id;
        self.child_selected = child;
        self.target = actor;
    }
    pub(crate) fn set_shared(&mut self, shared: QueueSender) {
        self.shared = Some(shared);
    }

    pub(crate) fn fence_cancelled_presentations(&mut self, run_id: u64) {
        self.cancelled_through = self.cancelled_through.max(run_id);
    }

    pub(crate) fn accepts_actor_presentation(&self, run_id: u64) -> bool {
        run_id > self.cancelled_through
            && self
                .current_run_id()
                .is_some_and(|allocated| run_id <= allocated)
    }

    pub(crate) fn reserve_run_id(&self, displayed_run_id: u64) -> u64 {
        self.shared.as_ref().map_or(displayed_run_id + 1, |shared| {
            shared.reserve_run_id(displayed_run_id)
        })
    }

    pub(crate) fn capture_cancel(&self, displayed_run_id: u64) -> (Option<TurnId>, u64) {
        self.shared
            .as_ref()
            .map_or((None, displayed_run_id + 1), |shared| {
                shared.capture_cancel(displayed_run_id)
            })
    }

    pub(crate) fn current_run_id(&self) -> Option<u64> {
        self.shared.as_ref().map(QueueSender::current_run_id)
    }

    pub(crate) fn set_run_id(&self, run_id: u64) {
        if let Some(shared) = &self.shared {
            shared.set_run_id(run_id);
        }
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn len(&self) -> usize {
        if self.target.is_some() || self.child_selected {
            return self
                .target
                .as_ref()
                .map_or(0, |actor| visible_pending(actor).len());
        }
        self.shared.as_ref().map_or(0, |s| s.len())
    }

    pub(crate) fn remove(&mut self, index: usize) {
        if self.target.is_some() || self.child_selected {
            if let Some(actor) = &self.target {
                let id = if self.focus() == Some(index) {
                    self.focused_submission
                } else {
                    visible_pending(actor).get(index).map(pending_identity)
                };
                match id {
                    Some(PendingIdentity::Message(id)) => {
                        actor.remove_submission(id);
                    }
                    Some(PendingIdentity::Compact(run_id)) => {
                        actor.remove_pending_compact(run_id);
                    }
                    None => {}
                }
            }
            self.unfocus();
            return;
        }
        // Removal success (or absence) only affects whether focus clamps;
        // the panel must never point past the (possibly unchanged) list.
        if let Some(ref shared) = self.shared {
            shared.remove(index);
        }
        self.clamp_focus();
    }

    #[cfg(test)]
    pub(crate) fn clear(&mut self) {
        if let Some(ref shared) = self.shared {
            shared.clear();
        }
        self.focus = None;
    }

    pub(crate) fn clear_root_user_work(&mut self) {
        if let Some(ref shared) = self.shared {
            shared.clear_user_work();
        }
        if !self.child_selected {
            self.unfocus();
        }
    }

    pub(crate) fn root_text_messages(&self) -> Vec<String> {
        self.shared
            .as_ref()
            .map_or(Vec::new(), QueueSender::text_messages)
    }

    pub(crate) fn focus(&self) -> Option<usize> {
        if let Some(actor) = &self.target {
            let identity = self.focused_submission?;
            return visible_pending(actor)
                .iter()
                .position(|entry| pending_identity(entry) == identity);
        }
        self.focus.filter(|&index| index < self.len())
    }

    pub(crate) fn set_focus(&mut self) {
        self.set_focus_at(0);
    }

    pub(crate) fn unfocus(&mut self) {
        self.focus = None;
        self.focused_submission = None;
    }

    pub(crate) fn move_focus_up(&mut self) {
        if let Some(sel) = self.focus()
            && sel > 0
        {
            self.set_focus_at(sel - 1);
        }
    }

    pub(crate) fn move_focus_down(&mut self) {
        if let Some(sel) = self.focus() {
            let len = self.len();
            if sel + 1 < len {
                self.set_focus_at(sel + 1);
            }
        }
    }

    pub(crate) fn remove_focused(&mut self) {
        if let Some(actor) = &self.target {
            match self.focused_submission {
                Some(PendingIdentity::Message(id)) => {
                    actor.remove_submission(id);
                }
                Some(PendingIdentity::Compact(run_id)) => {
                    actor.remove_pending_compact(run_id);
                }
                None => {}
            }
            self.unfocus();
        } else if let Some(sel) = self.focus() {
            self.remove(sel);
        }
    }

    pub(crate) fn pop_newest(&mut self) {
        if let Some(index) = self.panel_len().checked_sub(1) {
            self.remove(index);
        }
    }

    pub(crate) fn panel_len(&self) -> usize {
        if self.target.is_some() || self.child_selected {
            return self.len();
        }
        self.shared.as_ref().map_or(0, |s| s.panel_len())
    }

    pub(crate) fn panel_entries(&self) -> Vec<QueueEntry<'static>> {
        if self.target.is_some() || self.child_selected {
            return self.target.as_ref().map_or(Vec::new(), |actor| {
                visible_pending(actor)
                    .into_iter()
                    .map(|entry| match entry {
                        PendingQueueEntry::Message(entry) => QueueEntry {
                            text: Cow::Owned(entry.text),
                            color: theme::current().foreground,
                        },
                        PendingQueueEntry::Compact { instructions, .. } => QueueEntry {
                            text: instructions
                                .map_or(Cow::Borrowed(COMPACT_COMMAND_NAME), |extra| {
                                    Cow::Owned(format!("{COMPACT_COMMAND_NAME} {extra}"))
                                }),
                            color: theme::current()
                                .queue
                                .fg
                                .unwrap_or(theme::current().foreground),
                        },
                    })
                    .collect()
            });
        }
        self.shared.as_ref().map_or(vec![], |s| s.panel_entries())
    }

    #[cfg(test)]
    pub(crate) fn text_messages(&self) -> Vec<String> {
        if self.target.is_some() || self.child_selected {
            return self.target.as_ref().map_or(Vec::new(), |actor| {
                visible_pending(actor)
                    .into_iter()
                    .filter_map(|entry| match entry {
                        PendingQueueEntry::Message(entry) => Some(entry.text),
                        PendingQueueEntry::Compact { .. } => None,
                    })
                    .collect()
            });
        }
        self.shared.as_ref().map_or(vec![], |s| s.text_messages())
    }

    fn clamp_focus(&mut self) {
        let len = self.len();
        self.focus = match self.focus {
            Some(_) if len == 0 => None,
            Some(sel) if sel >= len => Some(len - 1),
            other => other,
        };
    }

    pub(crate) fn set_focus_at(&mut self, index: usize) {
        if index < self.len() {
            self.focus = Some(index);
            self.focused_submission = self
                .target
                .as_ref()
                .and_then(|actor| visible_pending(actor).get(index).map(pending_identity));
        }
    }
}

impl App {
    /// The one queue-or-start decision, shared by the keyboard and Lua
    /// paths so they cannot drift. Expects raw text: interpretation (slash
    /// commands, `exit`, `!`) is the caller's job, or skipped on purpose.
    pub(crate) fn submit_prompt(&mut self, msg: QueuedMessage) -> SubmitOutcome {
        if msg.text.trim().is_empty() && msg.images.is_empty() {
            return SubmitOutcome::Rejected(EMPTY_PROMPT_ERR.into());
        }
        // Expand `@prefix:value` tokens once, here, via the registered Lua
        // expanders. A rejected expander flashes and aborts the run. The
        // expanded text is stored so `build_agent_input` reuses it without
        // re-expanding.
        let expanded = match self.lua_event_handle.expand_references(&msg.text) {
            Ok(text) => text,
            Err(e) => return SubmitOutcome::Rejected(e),
        };
        let mut msg = msg;
        msg.text = expanded;
        if !self.is_main_chat() || self.selected_agent_id().is_some() {
            return self.submit_to_focused_actor(msg);
        }
        if self.status == Status::Streaming {
            if self.queue_and_notify(msg) {
                SubmitOutcome::Queued
            } else {
                SubmitOutcome::Rejected(NO_QUEUE_ERR.into())
            }
        } else {
            SubmitOutcome::Started(self.start_from_queue(&msg))
        }
    }

    /// Keyboard path: nobody is around to receive an `Err`, so
    /// rejections flash on screen instead.
    pub(super) fn submit_or_queue(&mut self, msg: QueuedMessage) -> Vec<Action> {
        match self.submit_prompt(msg) {
            SubmitOutcome::Started(actions) => actions,
            SubmitOutcome::Queued => vec![],
            SubmitOutcome::Rejected(e) => {
                self.flash(e);
                vec![]
            }
        }
    }

    fn submit_to_focused_actor(&mut self, msg: QueuedMessage) -> SubmitOutcome {
        if self.active_subagent_closed() {
            return SubmitOutcome::Rejected(NO_SUBAGENT_ERR.into());
        }
        let Some(target) = self
            .command_runtime
            .target_agent(&self.command_target)
            .filter(|target| Some(target.id()) == self.selected_agent_id())
        else {
            return SubmitOutcome::Rejected(NO_SUBAGENT_ERR.into());
        };
        let actor = match target.validate_live().and_then(|()| target.actor()) {
            Ok(actor) => actor,
            Err(error) => return SubmitOutcome::Rejected(error.to_string()),
        };
        let mut input = self.build_agent_input(&msg);
        if let Ok(Some(config)) = target.effective_config() {
            input.mode = config.mode.clone();
            input.thinking = config.settings.thinking;
            input.fast = config.settings.fast;
            input.workflow = config.settings.workflow;
        }
        input.preamble = self.shell.drain_results_for(Some(target.id()));
        if self.is_main_chat() {
            input.preamble.extend(self.shell.drain_results());
        }
        let root = self.is_main_chat();
        let starting = root && !self.focused_streaming();
        let run_id = if starting {
            actor.reserve_presentation(self.run_id)
        } else if root {
            actor.presentation_id()
        } else {
            0
        };
        let correlation = if root {
            crate::agent::shared_queue::correlation(run_id)
        } else {
            String::new()
        };
        match actor.submit(
            input,
            None,
            correlation,
            TurnAdmissionOptions {
                provenance: TurnProvenance {
                    origin: TurnOrigin::User,
                    ..Default::default()
                },
                ..Default::default()
            },
        ) {
            Ok(_) => {
                if starting {
                    self.record_run_start(run_id);
                }
                SubmitOutcome::Queued
            }
            Err(error) => SubmitOutcome::Rejected(error.to_string()),
        }
    }

    /// Deferred path: the agent is busy, so park the message and let
    /// `QueueItemConsumed` draw it once the agent picks it up. Returns
    /// false when there is no shared queue, meaning the message was dropped.
    pub(super) fn queue_and_notify(&mut self, msg: QueuedMessage) -> bool {
        let Some(ref shared) = self.queue.shared else {
            return false;
        };
        let input = self.build_agent_input(&msg);
        shared
            .try_push(QueueItem::Message {
                text: msg.text,
                input,
                run_id: self.run_id,
                displayed: false,
            })
            .is_ok()
    }

    /// Push restored queue items only here, never in `restore_display`: on
    /// load/rewind the display is restored before `respawn` swaps the shared
    /// queue, so pushing earlier would fill a queue that is about to die.
    pub(crate) fn flush_restored_queue(&mut self) {
        // The live queue owns the text from here on, so the recovery
        // snapshot must stop overriding it on save.
        self.recoverable_queue.clear();
        // Read, not taken: the live queue is what the next checkpoint mirrors
        // back into the session, so emptying it here changes nothing on disk.
        for text in self.state.session.meta.queued_messages.clone() {
            self.queue_and_notify(QueuedMessage {
                text,
                images: Vec::new(),
            });
        }
    }

    pub(super) fn queue_compact(&mut self, instructions: Option<String>) -> bool {
        let Some(ref shared) = self.queue.shared else {
            return false;
        };
        shared.push(QueueItem::Compact {
            run_id: self.run_id,
            instructions,
        });
        true
    }

    /// Agent reached a deferred message: time to draw the bubble. Restored
    /// queue items start runs without `start_run`, so this is where the app
    /// learns the agent is busy. Immediate-dispatch items skip this event,
    /// so no dedup needed.
    /// Immediate path: kick off the agent and draw the bubble in the same
    /// frame, so the user sees their message land where it will stay.
    pub(super) fn start_from_queue(&mut self, msg: &QueuedMessage) -> Vec<Action> {
        let input = self.build_agent_input(msg);
        self.start_run(input, msg.text.clone())
    }

    pub(crate) fn start_mailbox_run(
        &mut self,
        preamble: Vec<maki_providers::Message>,
    ) -> Vec<Action> {
        let mut input = self.build_agent_input(&QueuedMessage {
            text: String::new(),
            images: Vec::new(),
        });
        input.preamble = preamble;
        self.start_run(input, String::new())
    }

    /// The one place a fresh run starts: every path that emits
    /// `Action::SendMessage` must go through here so `run_id` bumps exactly
    /// once per run.
    pub(super) fn start_run(&mut self, input: AgentInput, display: String) -> Vec<Action> {
        self.record_run_start(self.queue.reserve_run_id(self.run_id));
        if !display.is_empty() || !input.images.is_empty() {
            self.main_chat()
                .show_user_message(display, input.images.clone());
        }
        vec![Action::SendMessage(Box::new(input))]
    }

    pub(super) fn record_run_start(&mut self, run_id: u64) {
        self.run_id = run_id;
        self.queue.set_run_id(run_id);
        self.clear_exit_request();
        self.stamped_subagent_outcomes.clear();
        // New work supersedes text held for recovery after an agent error.
        self.recoverable_queue.clear();
        self.status = Status::Streaming;
        // The turn runs on whatever the model is now; a change after this
        // point lands on the next turn.
        self.run_model = Some(self.state.session.model.clone());
        self.run_context_window = Some(self.state.model.context_window);
        self.fire_session_autocmd("TurnStart", serde_json::json!({}));
    }
}
