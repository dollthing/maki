use std::collections::HashSet;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;

use futures_lite::FutureExt;

use maki_providers::{ContentBlock, Message, MessageKind, Role};
use maki_storage::checkpoint::CheckpointPublication;
use maki_storage::sessions::HistorySnapshot;

use super::prepared::PreparedOperation;
use super::{
    ActorError, ActorIdleGuard, ActorLifecycle, AgentActorHandle, PreparedCommit,
    PreparedOperationTicket,
};
use crate::{AgentId, History};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryBoundary {
    pub agent_id: AgentId,
    pub epoch: u64,
    pub offset: usize,
}

#[derive(Clone)]
pub struct HistoryChange {
    pub agent_id: AgentId,
    pub epoch: u64,
    pub messages: Arc<Vec<Message>>,
    pub draft: Message,
}

pub type HistoryEditCheckpoint = Arc<
    dyn Fn(HistorySnapshot<Message>) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>>
        + Send
        + Sync,
>;

pub struct HistoryEditWork {
    owner: Arc<()>,
    action: HistoryEditAction,
    checkpoint: HistoryEditCheckpoint,
}

type PreparedEditResult = Result<
    (
        Option<ActorIdleGuard>,
        PreparedCommit,
        Option<HistoryChange>,
    ),
    ActorError,
>;

enum HistoryEditAction {
    Rewind {
        boundary: HistoryBoundary,
        completion: flume::Sender<Result<HistoryChange, ActorError>>,
    },
    Prepared {
        operation: Box<PreparedOperation>,
        clear_history: bool,
        held: bool,
        publication: Option<CheckpointPublication>,
        completion: flume::Sender<PreparedEditResult>,
    },
}

pub(super) enum HistoryEditCompletion {
    Rewind {
        sender: flume::Sender<Result<HistoryChange, ActorError>>,
        result: Result<HistoryChange, ActorError>,
    },
    Prepared {
        sender: flume::Sender<PreparedEditResult>,
        result: PreparedEditResult,
    },
}

impl HistoryEditCompletion {
    pub(super) fn send(self) {
        match self {
            Self::Rewind { sender, result } => {
                let _ = sender.send(result);
            }
            Self::Prepared { sender, result } => {
                let _ = sender.send(result);
            }
        }
    }
}

impl AgentActorHandle {
    pub fn history_boundary_message(
        &self,
        boundary: &HistoryBoundary,
    ) -> Result<Message, ActorError> {
        let history = self.inner.history.load_full();
        if boundary.agent_id != self.agent_id()
            || boundary.epoch != history.epoch
            || !valid_boundary(&history.messages, boundary.offset)
        {
            return Err(ActorError::StaleHistoryBoundary);
        }
        Ok(history.messages[boundary.offset].clone())
    }

    pub fn history_boundaries(&self) -> Result<Vec<HistoryBoundary>, ActorError> {
        Ok(self
            .history_boundary_messages()?
            .into_iter()
            .map(|(boundary, _)| boundary)
            .collect())
    }

    pub fn history_boundary_messages(&self) -> Result<Vec<(HistoryBoundary, Message)>, ActorError> {
        {
            let state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.lifecycle != ActorLifecycle::Open {
                return Err(super::lifecycle_error(state.lifecycle));
            }
        }
        let history = self.inner.history.load_full();
        let mut pending = HashSet::new();
        let mut valid = true;
        let mut boundaries = Vec::new();
        for (offset, message) in history.messages.iter().enumerate() {
            if valid && pending.is_empty() && is_boundary_message(message) {
                boundaries.push((
                    HistoryBoundary {
                        agent_id: self.agent_id(),
                        epoch: history.epoch,
                        offset,
                    },
                    message.clone(),
                ));
            }
            valid &= update_pending_tools(message, &mut pending);
        }
        Ok(boundaries)
    }
}

impl ActorIdleGuard {
    /// Starts an actor-owned commit after the caller has revalidated preparation.
    /// Once enqueued, dropping this future does not cancel the transaction.
    pub async fn commit_prepared(
        self,
        operation: PreparedOperationTicket,
        clear_history: bool,
        checkpoint: HistoryEditCheckpoint,
    ) -> Result<(PreparedCommit, Option<HistoryChange>), ActorError> {
        self.commit_prepared_inner(operation, clear_history, checkpoint, false, None)
            .await
            .map(|(_, commit, history)| (commit, history))
    }

    pub async fn commit_prepared_held(
        self,
        operation: PreparedOperationTicket,
        clear_history: bool,
        checkpoint: HistoryEditCheckpoint,
    ) -> Result<(ActorIdleGuard, PreparedCommit, Option<HistoryChange>), ActorError> {
        let (guard, commit, history) = self
            .commit_prepared_inner(operation, clear_history, checkpoint, true, None)
            .await?;
        Ok((guard.ok_or(ActorError::PolicyCancelled)?, commit, history))
    }

    pub async fn commit_prepared_held_cancellable(
        self,
        operation: PreparedOperationTicket,
        clear_history: bool,
        checkpoint: HistoryEditCheckpoint,
        publication: CheckpointPublication,
    ) -> Result<(ActorIdleGuard, PreparedCommit, Option<HistoryChange>), ActorError> {
        let (guard, commit, history) = self
            .commit_prepared_inner(
                operation,
                clear_history,
                checkpoint,
                true,
                Some(publication),
            )
            .await?;
        Ok((guard.ok_or(ActorError::PolicyCancelled)?, commit, history))
    }

    async fn commit_prepared_inner(
        mut self,
        operation: PreparedOperationTicket,
        clear_history: bool,
        checkpoint: HistoryEditCheckpoint,
        held: bool,
        publication: Option<CheckpointPublication>,
    ) -> PreparedEditResult {
        if !operation.belongs_to(&self.actor)
            || publication
                .as_ref()
                .is_some_and(CheckpointPublication::is_cancelled)
        {
            return Err(ActorError::PolicyCancelled);
        }
        let cancellation = publication.clone();
        let (completion, response) = flume::bounded(1);
        let actor = self.actor.clone();
        {
            let mut state = actor.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.lifecycle != ActorLifecycle::Open {
                return Err(super::lifecycle_error(state.lifecycle));
            }
            state
                .owned_idle(&self.owner)
                .filter(|reservation| {
                    reservation.permission.is_none() && reservation.edit.is_none()
                })
                .ok_or(ActorError::Busy)?;
            let pending = operation.take_ready_locked(&mut state)?;
            state.owned_idle(&self.owner).unwrap().committing = true;
            state.owned_idle(&self.owner).unwrap().edit = Some(HistoryEditWork {
                owner: Arc::clone(&self.owner),
                action: HistoryEditAction::Prepared {
                    operation: Box::new(pending),
                    clear_history,
                    held,
                    publication,
                    completion,
                },
                checkpoint,
            });
            self.release = false;
            actor.inner.queue.notify();
        }
        drop(operation);
        if let Some(publication) = cancellation {
            let result = futures_lite::future::or(
                async {
                    publication.cancelled().await;
                    let edit = {
                        let mut state = actor.inner.state.lock().unwrap_or_else(|e| e.into_inner());
                        state
                            .owned_idle(&self.owner)
                            .and_then(|idle| idle.edit.take())
                    };
                    if let Some(edit) = edit {
                        edit.fail(ActorError::PolicyCancelled);
                        self.release = true;
                    }
                    None
                },
                async { Some(response.recv_async().await) },
            )
            .await;
            if let Some(result) = result {
                return result.map_err(|_| ActorError::Closed)?;
            }
        }
        response
            .recv_async()
            .await
            .map_err(|_| ActorError::Closed)?
    }

    pub async fn rewind(
        mut self,
        boundary: HistoryBoundary,
        checkpoint: HistoryEditCheckpoint,
    ) -> Result<HistoryChange, ActorError> {
        let (completion, response) = flume::bounded(1);
        let actor = self.actor.clone();
        {
            let mut state = actor.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.lifecycle != ActorLifecycle::Open {
                return Err(super::lifecycle_error(state.lifecycle));
            }
            let reservation = state
                .owned_idle(&self.owner)
                .filter(|reservation| {
                    reservation.permission.is_none() && reservation.edit.is_none()
                })
                .ok_or(ActorError::Busy)?;
            self.release = false;
            reservation.edit = Some(HistoryEditWork {
                owner: Arc::clone(&self.owner),
                action: HistoryEditAction::Rewind {
                    boundary,
                    completion,
                },
                checkpoint,
            });
            actor.inner.queue.notify();
        }
        response
            .recv_async()
            .await
            .map_err(|_| ActorError::Closed)?
    }
}

impl HistoryEditWork {
    pub(super) fn fail(self, error: ActorError) {
        match self.action {
            HistoryEditAction::Rewind { completion, .. } => {
                let _ = completion.send(Err(error));
            }
            HistoryEditAction::Prepared {
                operation,
                completion,
                ..
            } => {
                (*operation).fail(error.clone());
                let _ = completion.send(Err(error));
            }
        }
    }

    pub(super) async fn execute(
        self,
        actor: &AgentActorHandle,
        history: &mut History,
    ) -> HistoryEditCompletion {
        let mut guard = ActorIdleGuard {
            actor: actor.clone(),
            owner: Arc::clone(&self.owner),
            release: true,
        };
        match self.action {
            HistoryEditAction::Rewind {
                boundary,
                completion,
            } => {
                let result =
                    Self::apply(&self.owner, &boundary, &self.checkpoint, actor, history).await;
                drop(guard);
                HistoryEditCompletion::Rewind {
                    sender: completion,
                    result,
                }
            }
            HistoryEditAction::Prepared {
                operation,
                clear_history,
                held,
                publication,
                completion,
            } => {
                let snapshot = clear_history.then(|| {
                    let mut snapshot = HistorySnapshot::new(Vec::new());
                    snapshot.compacted = actor.inner.history.load().compacted;
                    snapshot
                });
                let result = if let Some(publication) = &publication {
                    if publication.is_cancelled() {
                        Err(ActorError::PolicyCancelled)
                    } else if let Some(snapshot) = &snapshot {
                        futures_lite::future::or(
                            checkpoint(&self.checkpoint, snapshot.clone()),
                            async {
                                publication.cancelled().await;
                                Err(ActorError::PolicyCancelled)
                            },
                        )
                        .await
                        .and_then(|()| {
                            if publication.finish_commit() {
                                Ok(())
                            } else {
                                Err(ActorError::PolicyCancelled)
                            }
                        })
                    } else if publication.try_publish() {
                        Ok(())
                    } else {
                        Err(ActorError::PolicyCancelled)
                    }
                } else if let Some(snapshot) = &snapshot {
                    checkpoint(&self.checkpoint, snapshot.clone()).await
                } else {
                    Ok(())
                };
                let result = match result {
                    Err(error) => {
                        (*operation).fail(error.clone());
                        let mut state = actor.inner.state.lock().unwrap_or_else(|e| e.into_inner());
                        super::drive_operations(&actor.inner, &mut state);
                        Err(error)
                    }
                    Ok(()) => {
                        let mut state = actor.inner.state.lock().unwrap_or_else(|e| e.into_inner());
                        let change = snapshot.map(|snapshot| {
                            history.replace_snapshot(snapshot.clone());
                            HistoryChange {
                                agent_id: actor.agent_id(),
                                epoch: snapshot.epoch,
                                messages: snapshot.messages,
                                draft: Message::user(String::new()),
                            }
                        });
                        let committed = (*operation).commit_locked(&actor.inner, &mut state);
                        if let Some(reservation) = state.owned_idle(&self.owner) {
                            reservation.committing = false;
                            if held {
                                guard.release = false;
                            } else if let Some(ticket) = &committed.ticket {
                                reservation.permission = Some(ticket.turn_id());
                                guard.release = false;
                            }
                        }
                        super::drive_operations(&actor.inner, &mut state);
                        Ok((committed, change))
                    }
                };
                if let Ok((committed, _)) = &result
                    && let Some(ticket) = &committed.ticket
                {
                    let cancelled = {
                        let state = actor.inner.state.lock().unwrap_or_else(|e| e.into_inner());
                        let reason = match state.lifecycle {
                            ActorLifecycle::Open => None,
                            ActorLifecycle::Closed => Some(crate::TurnCancellationReason::Closed),
                            ActorLifecycle::Shutdown => {
                                Some(crate::TurnCancellationReason::Shutdown)
                            }
                        };
                        if let Some(reason) = reason {
                            actor
                                .inner
                                .queue
                                .remove_turn(ticket.turn_id())
                                .zip(Some(reason))
                        } else {
                            None
                        }
                    };
                    if let Some((admission, reason)) = cancelled {
                        super::terminalize_work(
                            &actor.inner,
                            vec![super::ActorWork::Turn(Box::new(admission))],
                            reason,
                        );
                    }
                }
                let result = result.map(|(commit, history)| {
                    let idle = held.then(|| ActorIdleGuard {
                        actor: actor.clone(),
                        owner: Arc::clone(&self.owner),
                        release: true,
                    });
                    (idle, commit, history)
                });
                drop(guard);
                HistoryEditCompletion::Prepared {
                    sender: completion,
                    result,
                }
            }
        }
    }

    async fn apply(
        owner: &Arc<()>,
        boundary: &HistoryBoundary,
        checkpoint_fn: &HistoryEditCheckpoint,
        actor: &AgentActorHandle,
        history: &mut History,
    ) -> Result<HistoryChange, ActorError> {
        let (draft, snapshot) = {
            let mut state = actor.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.lifecycle != ActorLifecycle::Open {
                return Err(super::lifecycle_error(state.lifecycle));
            }
            if state.owned_idle(owner).is_none() {
                return Err(ActorError::PolicyCancelled);
            }
            let snapshot = actor.inner.history.load_full();
            if boundary.agent_id != actor.agent_id()
                || boundary.epoch != snapshot.epoch
                || !valid_boundary(history.as_slice(), boundary.offset)
            {
                return Err(ActorError::StaleHistoryBoundary);
            }
            let mut replacement =
                HistorySnapshot::new(history.as_slice()[..boundary.offset].to_vec());
            replacement.compacted = snapshot.compacted;
            (history.as_slice()[boundary.offset].clone(), replacement)
        };
        checkpoint(checkpoint_fn, snapshot.clone()).await?;
        let _state = actor.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        history.replace_snapshot(snapshot.clone());
        Ok(HistoryChange {
            agent_id: actor.agent_id(),
            epoch: snapshot.epoch,
            messages: snapshot.messages,
            draft,
        })
    }
}

async fn checkpoint(
    checkpoint: &HistoryEditCheckpoint,
    snapshot: HistorySnapshot<Message>,
) -> Result<(), ActorError> {
    let future = catch_unwind(AssertUnwindSafe(|| checkpoint(snapshot)))
        .map_err(|_| ActorError::HistoryCheckpoint("checkpoint callback panicked".into()))?;
    AssertUnwindSafe(future)
        .catch_unwind()
        .await
        .map_err(|_| ActorError::HistoryCheckpoint("checkpoint future panicked".into()))?
        .map_err(ActorError::HistoryCheckpoint)
}

fn valid_boundary(messages: &[Message], offset: usize) -> bool {
    let Some(message) = messages.get(offset) else {
        return false;
    };
    let mut pending = HashSet::new();
    is_boundary_message(message)
        && messages[..offset]
            .iter()
            .all(|message| update_pending_tools(message, &mut pending))
        && pending.is_empty()
}

fn is_boundary_message(message: &Message) -> bool {
    matches!(message.role, Role::User)
        && matches!(message.kind, MessageKind::Turn)
        && !message.content.is_empty()
        && !message
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::ToolResult { .. }))
}

fn update_pending_tools<'a>(message: &'a Message, pending: &mut HashSet<&'a str>) -> bool {
    for block in &message.content {
        match block {
            ContentBlock::ToolUse { id, .. } => {
                pending.insert(id.as_str());
            }
            ContentBlock::ToolResult { tool_use_id, .. }
                if !pending.remove(tool_use_id.as_str()) =>
            {
                return false;
            }
            _ => {}
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    use futures_lite::future;
    use maki_providers::{ContentBlock, Message, Role, ThinkingConfig, TokenUsage};
    use maki_storage::checkpoint::CheckpointPublication;
    use maki_storage::sessions::HistorySnapshot;
    use test_case::test_case;

    use super::{HistoryEditCheckpoint, valid_boundary};
    use crate::actor::tests::{ScriptedBackend, input, policy};
    use crate::actor::{
        ActorBackend, ActorError, AgentActorHandle, BackendResult, ChildDelivery, ConfigChange,
        ConfigPatch, ControlWork, EffectiveAgentConfig, PreparedTurn, TurnContext, TurnResult,
        WorkKind,
    };
    use crate::{
        AgentId, AgentInput, AgentMode, DoneReason, History, TurnCancellationReason, TurnId,
        TurnOutcome,
    };

    const CHECKPOINT_ERROR: &str = "checkpoint unavailable";

    #[test_case(false, false; "rewind_close")]
    #[test_case(true, false; "prepared_close")]
    #[test_case(false, true; "rewind_shutdown")]
    #[test_case(true, true; "prepared_shutdown")]
    fn queued_history_edit_resolves_when_actor_closes(prepared: bool, shutdown: bool) {
        smol::block_on(async {
            const TEXT: &str = "draft";
            let (_start, rx) = flume::bounded(1);
            let (actor, runner) = AgentActorHandle::spawn_inner(
                AgentId::generate(),
                vec![Message::user(TEXT.into())],
                None,
                Box::new(IdleBackend),
                None,
                None,
                Some(rx),
            );
            actor
                .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
                .unwrap();
            let guard = actor.prepare_idle().unwrap();
            let checkpoint: HistoryEditCheckpoint =
                Arc::new(|_| panic!("closed edit must not checkpoint"));
            let boundary = actor.history_boundaries().unwrap().remove(0);
            let operation = actor.reserve_prepared_operation(None).unwrap();
            operation.resolve(None).unwrap();
            operation.wait_ready().await.unwrap();
            let edit = async move {
                if prepared {
                    guard
                        .commit_prepared(operation, true, checkpoint)
                        .await
                        .map(|_| ())
                } else {
                    drop(operation);
                    guard.rewind(boundary, checkpoint).await.map(|_| ())
                }
            };
            let mut edit = Box::pin(edit);
            assert!(future::poll_once(&mut edit).await.is_none());
            if shutdown {
                actor.shutdown();
            } else {
                actor.close();
            }
            runner.await;
            let result = future::poll_once(edit)
                .await
                .expect("queued edit must resolve after close");
            assert!(matches!(
                (shutdown, result),
                (false, Err(ActorError::Closed)) | (true, Err(ActorError::Shutdown))
            ));
        });
    }

    #[test_case(false; "rewind")]
    #[test_case(true; "prepared_config_only")]
    fn edit_response_waits_for_runner_cleanup(prepared: bool) {
        smol::block_on(async {
            const DRAFT: &str = "draft";
            let (actor, runner) = AgentActorHandle::spawn(
                AgentId::generate(),
                vec![Message::user(DRAFT.into())],
                None,
                Box::new(IdleBackend),
            );
            actor
                .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
                .unwrap();
            let (executed, execution) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            *actor.inner.after_work_execution.lock().unwrap() = Some((executed, released));
            let guard = actor.prepare_idle().unwrap();
            let checkpoint: HistoryEditCheckpoint = Arc::new(|_| Box::pin(async { Ok(()) }));
            let boundary = actor.history_boundaries().unwrap().remove(0);
            let operation = actor.reserve_prepared_operation(None).unwrap();
            operation.resolve(None).unwrap();
            operation.wait_ready().await.unwrap();
            let mut edit = Box::pin(async move {
                if prepared {
                    guard
                        .commit_prepared(operation, false, checkpoint)
                        .await
                        .map(|_| ())
                } else {
                    drop(operation);
                    guard.rewind(boundary, checkpoint).await.map(|_| ())
                }
            });
            assert!(future::poll_once(&mut edit).await.is_none());
            execution.recv_async().await.unwrap();
            let premature = future::poll_once(&mut edit).await;
            let premature_idle = premature.as_ref().map(|_| actor.prepare_idle().is_ok());
            release.send(()).unwrap();
            if premature.is_none() {
                edit.await.unwrap();
            }
            let idle = actor.prepare_idle();
            let idle_after_response = idle.is_ok();
            drop(idle);
            actor.close();
            runner.await;
            assert!(
                premature.is_none(),
                "edit response must not precede runner cleanup: idle acquisition was {premature_idle:?}"
            );
            assert!(idle_after_response);
        });
    }

    #[test]
    fn held_prepared_commit_waits_for_presentation_permission() {
        smol::block_on(async {
            const IMPLEMENT: &str = "implement after presentation";
            let (actor, runner) = AgentActorHandle::spawn(
                AgentId::generate(),
                Vec::new(),
                None,
                Box::new(ScriptedBackend::new()),
            );
            actor
                .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
                .unwrap();
            let idle = actor.prepare_idle().unwrap();
            let operation = actor
                .reserve_prepared_operation(Some(PreparedTurn {
                    input: input(IMPLEMENT),
                    event_sender: None,
                    correlation: IMPLEMENT.into(),
                }))
                .unwrap();
            operation.resolve(None).unwrap();
            operation.wait_ready().await.unwrap();
            let (guard, committed, history) = idle
                .commit_prepared_held(operation, false, Arc::new(|_| Box::pin(async { Ok(()) })))
                .await
                .unwrap();
            let ticket = committed.ticket.unwrap();
            assert!(history.is_none());
            actor.runtime_barrier().await.unwrap();
            assert!(ticket.peek_result().is_none());
            assert_eq!(actor.committed_message_count(), 0);
            guard.allow_turn(&ticket).unwrap();
            assert!(matches!(
                ticket.wait().await,
                crate::TurnOutcome::Completed { .. }
            ));
            assert_eq!(actor.committed_message_count(), 1);
            drop(guard);
            actor.close();
            runner.await;
        });
    }

    struct IdleBackend;

    impl ActorBackend for IdleBackend {
        fn run_turn<'a>(
            &'a mut self,
            history: &'a mut History,
            context: TurnContext,
            _: AgentInput,
            _: WorkKind,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async move {
                let inbox = context.inbox.unwrap();
                inbox.commit_completions(&mut |message| history.push(message));
                assert!(inbox.finish());
                BackendResult::EnteredRun(TurnOutcome::completed(
                    context.agent_id,
                    context.turn_id.unwrap(),
                    TokenUsage::default(),
                    0,
                    DoneReason::EndTurn,
                ))
            })
        }

        fn run_control<'a>(
            &'a mut self,
            _: &'a mut History,
            _: TurnContext,
            _: &'a ControlWork,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async { unreachable!() })
        }

        fn run_compact<'a>(
            &'a mut self,
            history: &'a mut History,
            _: TurnContext,
            _: Option<&'a str>,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async move {
                history.replace_compacted(history.as_slice().to_vec());
                BackendResult::CompactDone
            })
        }
    }

    #[test_case(false, false; "preserve_history")]
    #[test_case(true, false; "clear_history")]
    #[test_case(true, true; "clear_empty_history")]
    fn prepared_commit_preserves_identity(clear: bool, empty: bool) {
        smol::block_on(async {
            let messages = if empty {
                Vec::new()
            } else {
                vec![Message::user("draft".into())]
            };
            let (actor, runner) =
                AgentActorHandle::spawn(AgentId::generate(), messages, None, Box::new(IdleBackend));
            actor
                .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
                .unwrap();
            let original = actor.config_snapshot().unwrap();
            let epoch = actor.inner.history.load().epoch;
            let guard = actor.prepare_idle().unwrap();
            let operation = actor.reserve_prepared_operation(None).unwrap();
            operation.resolve(None).unwrap();
            operation.wait_ready().await.unwrap();
            let (saved, checkpointed) = flume::bounded(1);
            let checkpoint: HistoryEditCheckpoint = Arc::new(move |snapshot| {
                assert!(snapshot.messages.is_empty());
                saved.send(snapshot).unwrap();
                Box::pin(async { Ok(()) })
            });
            let (commit, change) = guard
                .commit_prepared(operation, clear, checkpoint)
                .await
                .unwrap();
            assert!(Arc::ptr_eq(&original.identity, &commit.config.identity));
            assert_eq!(original.generation, commit.config.generation);
            assert_eq!(change.is_some(), clear);
            if let Some(change) = change {
                let snapshot = checkpointed.recv().unwrap();
                assert_eq!(change.epoch, snapshot.epoch);
                assert!(Arc::ptr_eq(&change.messages, &snapshot.messages));
                let published = actor.inner.history.load();
                assert_eq!(published.epoch, snapshot.epoch);
                assert_eq!(published.compacted, snapshot.compacted);
                assert!(Arc::ptr_eq(&published.messages, &snapshot.messages));
                assert_eq!(change.agent_id, actor.agent_id());
                assert_ne!(change.epoch, epoch);
                assert!(change.messages.is_empty());
                assert!(matches!(change.draft.role, Role::User));
            } else {
                assert_eq!(actor.inner.history.load().epoch, epoch);
            }
            actor.runtime_barrier().await.unwrap();
            assert!(actor.prepare_idle().is_ok());
            actor.close();
            runner.await;
        });
    }

    #[test]
    fn prepared_commit_rejects_another_actor() {
        smol::block_on(async {
            let (actor, runner) = AgentActorHandle::spawn(
                AgentId::generate(),
                Vec::new(),
                None,
                Box::new(IdleBackend),
            );
            let (other, other_runner) = AgentActorHandle::spawn(
                AgentId::generate(),
                Vec::new(),
                None,
                Box::new(IdleBackend),
            );
            let operation = other.reserve_prepared_operation(None).unwrap();
            let checkpoint: HistoryEditCheckpoint =
                Arc::new(|_| panic!("foreign operation must not checkpoint"));
            let result = actor
                .prepare_idle()
                .unwrap()
                .commit_prepared(operation, true, checkpoint)
                .await;
            assert!(matches!(result, Err(ActorError::PolicyCancelled)));
            assert!(actor.prepare_idle().is_ok());
            assert!(other.prepare_idle().is_ok());
            actor.close();
            other.close();
            runner.await;
            other_runner.await;
        });
    }

    #[test_case(false, false; "local_cancel")]
    #[test_case(true, false; "local_cancel_and_close")]
    #[test_case(false, true; "local_cancel_and_shutdown")]
    fn prepared_checkpoint_survives_cancel_and_close(close: bool, shutdown: bool) {
        smol::block_on(async {
            let (actor, runner) = AgentActorHandle::spawn(
                AgentId::generate(),
                vec![Message::user("draft".into())],
                None,
                Box::new(IdleBackend),
            );
            actor
                .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
                .unwrap();
            let guard = actor.prepare_idle().unwrap();
            let turn = (close || shutdown).then(|| PreparedTurn {
                input: AgentInput {
                    message: String::new(),
                    mode: AgentMode::Build,
                    images: Vec::new(),
                    preamble: Vec::new(),
                    thinking: ThinkingConfig::Off,
                    fast: false,
                    workflow: false,
                    prompt: None,
                    cancel: None,
                    lease_committer: None,
                },
                event_sender: None,
                correlation: "prepared-close".into(),
            });
            let operation = actor.reserve_prepared_operation(turn).unwrap();
            operation.resolve(Some(ConfigChange::ToggleFast)).unwrap();
            operation.wait_ready().await.unwrap();
            let (entered, entry) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            let checkpoint: HistoryEditCheckpoint = Arc::new(move |messages| {
                assert!(messages.messages.is_empty());
                let entered = entered.clone();
                let released = released.clone();
                Box::pin(async move {
                    entered.send(()).unwrap();
                    released.recv_async().await.unwrap();
                    Ok(())
                })
            });
            let commit = smol::spawn(guard.commit_prepared(operation, true, checkpoint));
            entry.recv_async().await.unwrap();
            assert!(!actor.effective_config().unwrap().fast);
            assert_eq!(actor.inner.history.load().messages.len(), 1);
            assert!(matches!(actor.prepare_idle(), Err(ActorError::Busy)));
            actor.cancel_local(0);
            if shutdown {
                actor.shutdown();
            } else if close {
                actor.close();
            }
            release.send(()).unwrap();
            let (committed, change) = commit.await.unwrap();
            assert!(committed.config.config.fast);
            assert!(actor.effective_config().unwrap().fast);
            assert!(change.unwrap().messages.is_empty());
            assert!(actor.inner.history.load().messages.is_empty());
            if let Some(ticket) = committed.ticket {
                assert!(ticket.peek_result().is_some());
                ticket.wait().await;
                assert!(actor.inner.queue.remove_turn(ticket.turn_id()).is_none());
            }
            actor.close();
            runner.await;
        });
    }

    #[test_case(true, true; "cancel_before_checkpoint")]
    #[test_case(true, false; "cancel_permanently_gated_checkpoint")]
    #[test_case(false, true; "cancel_without_history_checkpoint")]
    fn cancellable_prepared_commit_releases_reservation(clear_history: bool, cancel_before: bool) {
        smol::block_on(async {
            let (actor, runner) = AgentActorHandle::spawn(
                AgentId::generate(),
                vec![Message::user(CHECKPOINT_ERROR.into())],
                None,
                Box::new(IdleBackend),
            );
            actor
                .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
                .unwrap();
            let history = actor.inner.history.load_full();
            let guard = actor.prepare_idle().unwrap();
            let operation = actor.reserve_prepared_operation(None).unwrap();
            operation.resolve(Some(ConfigChange::ToggleFast)).unwrap();
            operation.wait_ready().await.unwrap();
            let publication = CheckpointPublication::default();
            if cancel_before {
                assert!(publication.cancel());
            }
            let (entered, entry) = flume::bounded(1);
            let checkpoint: HistoryEditCheckpoint = Arc::new(move |_| {
                let entered = entered.clone();
                Box::pin(async move {
                    entered.send(()).unwrap();
                    futures_lite::future::pending().await
                })
            });
            let commit = smol::spawn(guard.commit_prepared_held_cancellable(
                operation,
                clear_history,
                checkpoint,
                publication.clone(),
            ));
            if !cancel_before {
                entry.recv_async().await.unwrap();
                assert!(publication.cancel());
            }
            assert!(matches!(commit.await, Err(ActorError::PolicyCancelled)));
            drop(actor.prepare_idle().expect(CHECKPOINT_ERROR));
            assert!(!actor.effective_config().unwrap().fast);
            assert!(Arc::ptr_eq(
                &history.messages,
                &actor.inner.history.load().messages
            ));
            actor.close();
            runner.await;
        });
    }

    #[test_case(true, true; "cancel_before_queue_with_clear")]
    #[test_case(false, true; "cancel_before_queue_without_clear")]
    #[test_case(true, false; "cancel_queued_with_clear")]
    #[test_case(false, false; "cancel_queued_without_clear")]
    fn cancellable_prepared_commit_does_not_need_runner(clear_history: bool, cancel_before: bool) {
        smol::block_on(async {
            let (start, gate) = flume::bounded(1);
            let (actor, runner) = AgentActorHandle::spawn_inner(
                AgentId::generate(),
                vec![Message::user(CHECKPOINT_ERROR.into())],
                None,
                Box::new(IdleBackend),
                None,
                None,
                Some(gate),
            );
            actor
                .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
                .unwrap();
            let guard = actor.prepare_idle().unwrap();
            let operation = actor.reserve_prepared_operation(None).unwrap();
            operation.resolve(Some(ConfigChange::ToggleFast)).unwrap();
            operation.wait_ready().await.unwrap();
            let publication = CheckpointPublication::default();
            if cancel_before {
                assert!(publication.cancel());
            }
            let checkpoint: HistoryEditCheckpoint =
                Arc::new(|_| panic!("cancelled edit must not checkpoint"));
            let mut commit = Box::pin(guard.commit_prepared_held_cancellable(
                operation,
                clear_history,
                checkpoint,
                publication.clone(),
            ));
            if !cancel_before {
                assert!(future::poll_once(commit.as_mut()).await.is_none());
                assert!(publication.cancel());
            }
            let result = future::poll_once(commit.as_mut())
                .await
                .expect(CHECKPOINT_ERROR);
            assert!(matches!(result, Err(ActorError::PolicyCancelled)));
            assert!(!actor.effective_config().unwrap().fast);
            assert_eq!(actor.inner.history.load().messages.len(), 1);
            drop(actor.prepare_idle().expect(CHECKPOINT_ERROR));
            start.send(()).unwrap();
            actor.close();
            runner.await;
        });
    }

    #[test_case(true, false; "ready_checkpoint_loses_to_cancel")]
    #[test_case(false, false; "published_checkpoint_succeeds")]
    #[test_case(false, true; "published_checkpoint_reports_failure")]
    fn cancellable_prepared_commit_observes_publication(cancel: bool, fail: bool) {
        smol::block_on(async {
            let (actor, runner) = AgentActorHandle::spawn(
                AgentId::generate(),
                vec![Message::user(CHECKPOINT_ERROR.into())],
                None,
                Box::new(IdleBackend),
            );
            actor
                .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
                .unwrap();
            let history = actor.inner.history.load_full();
            let guard = actor.prepare_idle().unwrap();
            let operation = actor.reserve_prepared_operation(None).unwrap();
            operation.resolve(Some(ConfigChange::ToggleFast)).unwrap();
            operation.wait_ready().await.unwrap();
            let publication = CheckpointPublication::default();
            let checkpoint: HistoryEditCheckpoint = Arc::new({
                let publication = publication.clone();
                move |_| {
                    let publication = publication.clone();
                    Box::pin(async move {
                        if cancel {
                            assert!(publication.cancel());
                        } else {
                            assert!(publication.try_publish());
                            assert!(!publication.cancel());
                        }
                        if fail {
                            Err(CHECKPOINT_ERROR.into())
                        } else {
                            Ok(())
                        }
                    })
                }
            });
            let result = guard
                .commit_prepared_held_cancellable(operation, true, checkpoint, publication)
                .await;
            if cancel || fail {
                assert!(matches!(
                    result,
                    Err(ActorError::PolicyCancelled | ActorError::HistoryCheckpoint(_))
                ));
                assert!(!actor.effective_config().unwrap().fast);
                assert!(Arc::ptr_eq(
                    &history.messages,
                    &actor.inner.history.load().messages
                ));
            } else {
                let (idle, _, change) = result.unwrap();
                assert!(actor.effective_config().unwrap().fast);
                assert!(change.unwrap().messages.is_empty());
                assert!(matches!(actor.prepare_idle(), Err(ActorError::Busy)));
                drop(idle);
            }
            drop(actor.prepare_idle().expect(CHECKPOINT_ERROR));
            actor.close();
            runner.await;
        });
    }

    #[test]
    fn prepared_checkpoint_failure_preserves_config_and_history() {
        smol::block_on(async {
            let (actor, runner) = AgentActorHandle::spawn(
                AgentId::generate(),
                vec![Message::user("draft".into())],
                None,
                Box::new(IdleBackend),
            );
            actor
                .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
                .unwrap();
            let original = actor.config_snapshot().unwrap();
            let epoch = actor.inner.history.load().epoch;
            let guard = actor.prepare_idle().unwrap();
            let operation = actor.reserve_prepared_operation(None).unwrap();
            operation
                .resolve(Some(ConfigChange::Patch(ConfigPatch {
                    fast: Some(true),
                    ..Default::default()
                })))
                .unwrap();
            operation.wait_ready().await.unwrap();
            let checkpoint: HistoryEditCheckpoint =
                Arc::new(|_| Box::pin(async { Err(CHECKPOINT_ERROR.into()) }));
            let result = guard.commit_prepared(operation, true, checkpoint).await;
            assert!(
                matches!(result, Err(ActorError::HistoryCheckpoint(error)) if error == CHECKPOINT_ERROR)
            );
            assert_eq!(
                actor.config_snapshot().unwrap().generation,
                original.generation
            );
            assert!(!actor.effective_config().unwrap().fast);
            assert_eq!(actor.inner.history.load().epoch, epoch);
            assert_eq!(actor.inner.history.load().messages.len(), 1);
            actor.runtime_barrier().await.unwrap();
            assert!(actor.prepare_idle().is_ok());
            actor.close();
            runner.await;
        });
    }

    #[test]
    fn rewind_checkpoint_is_unpublished_until_ack_and_survives_close() {
        smol::block_on(async {
            let (actor, runner) = AgentActorHandle::spawn(
                AgentId::generate(),
                vec![Message::user("first".into()), Message::user("draft".into())],
                None,
                Box::new(IdleBackend),
            );
            let boundary = actor.history_boundaries().unwrap().remove(1);
            let epoch = boundary.epoch;
            let (entered, entry) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            let checkpoint: HistoryEditCheckpoint = Arc::new(move |messages| {
                let entered = entered.clone();
                let released = released.clone();
                Box::pin(async move {
                    entered.send(messages).unwrap();
                    released.recv_async().await.unwrap();
                    Ok(())
                })
            });
            let guard = actor.prepare_idle().unwrap();
            let edit = smol::spawn(guard.rewind(boundary, checkpoint));
            let checkpointed = entry.recv_async().await.unwrap();
            assert_eq!(checkpointed.messages.len(), 1);
            assert_eq!(actor.inner.history.load().epoch, epoch);
            assert_eq!(actor.inner.history.load().messages.len(), 2);
            assert!(matches!(actor.prepare_idle(), Err(ActorError::Busy)));
            actor.close();
            release.send(()).unwrap();
            let change = edit.await.unwrap();
            assert_eq!(change.epoch, checkpointed.epoch);
            assert!(Arc::ptr_eq(&change.messages, &checkpointed.messages));
            let published = actor.inner.history.load();
            assert_eq!(published.epoch, checkpointed.epoch);
            assert_eq!(published.compacted, checkpointed.compacted);
            assert!(Arc::ptr_eq(&published.messages, &checkpointed.messages));
            assert_eq!(change.messages.len(), 1);
            assert_ne!(change.epoch, epoch);
            runner.await;
        });
    }

    #[test]
    fn rewind_serializes_concurrent_child_report() {
        smol::block_on(async {
            let (actor, runner) = AgentActorHandle::spawn(
                AgentId::generate(),
                vec![Message::user("draft".into())],
                None,
                Box::new(IdleBackend),
            );
            let boundary = actor.history_boundaries().unwrap().remove(0);
            let (entered, entry) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            let checkpoint: HistoryEditCheckpoint = Arc::new(move |_| {
                let entered = entered.clone();
                let released = released.clone();
                Box::pin(async move {
                    entered.send(()).unwrap();
                    released.recv_async().await.unwrap();
                    Ok(())
                })
            });
            let guard = actor.prepare_idle().unwrap();
            let edit = smol::spawn(guard.rewind(boundary, checkpoint));
            entry.recv_async().await.unwrap();
            let turn_id = TurnId::generate();
            let report = ChildDelivery {
                parent_id: actor.agent_id(),
                compatibility_id: None,
                label: None,
                child_config: None,
                correlation: "rewind-report".into(),
                result: TurnResult {
                    outcome: TurnOutcome::cancelled(
                        AgentId::generate(),
                        turn_id,
                        TokenUsage::default(),
                        0,
                        TurnCancellationReason::User,
                    ),
                    text: "child report".into(),
                    output: Vec::new(),
                    provenance: Default::default(),
                    duration_ms: 0,
                },
            };
            super::super::delivery::admit_delivery(&actor.inner, Arc::new(report)).unwrap();
            {
                let mut state = actor.inner.state.lock().unwrap();
                super::super::delivery::schedule(&actor.inner, &mut state);
                assert!(state.deliveries.contains_key(&turn_id));
                assert!(state.pop_work(&actor.inner.queue).is_none());
            }
            assert_eq!(actor.inner.history.load().messages.len(), 1);
            release.send(()).unwrap();
            assert!(edit.await.unwrap().messages.is_empty());
            actor.runtime_barrier().await.unwrap();
            assert_eq!(actor.committed_message_count(), 1);
            assert!(
                actor.inner.history.load().messages[0]
                    .first_text_content()
                    .unwrap()
                    .contains("child report")
            );
            assert!(!actor.withdraw_child_delivery(turn_id));
            actor.close();
            runner.await;
        });
    }

    #[test]
    fn rewind_callback_panic_releases_reservation_without_publication() {
        smol::block_on(async {
            let (actor, runner) = AgentActorHandle::spawn(
                AgentId::generate(),
                vec![Message::user("draft".into())],
                None,
                Box::new(IdleBackend),
            );
            let boundary = actor.history_boundaries().unwrap().remove(0);
            let epoch = boundary.epoch;
            let checkpoint: HistoryEditCheckpoint = Arc::new(|_| panic!("callback panic"));
            let result = actor
                .prepare_idle()
                .unwrap()
                .rewind(boundary, checkpoint)
                .await;
            assert!(matches!(result, Err(ActorError::HistoryCheckpoint(_))));
            assert_eq!(actor.inner.history.load().epoch, epoch);
            assert_eq!(actor.inner.history.load().messages.len(), 1);
            actor.close();
            runner.await;
        });
    }

    #[test]
    fn rewind_checkpoint_failure_restores_history() {
        smol::block_on(async {
            let (actor, runner) = AgentActorHandle::spawn(
                AgentId::generate(),
                vec![Message::user("first".into()), Message::user("draft".into())],
                None,
                Box::new(IdleBackend),
            );
            let boundary = actor.history_boundaries().unwrap().remove(1);
            let original = actor.inner.history.load_full();
            let checkpoint: HistoryEditCheckpoint = Arc::new(|_: HistorySnapshot<Message>| {
                Box::pin(async { Err(CHECKPOINT_ERROR.to_owned()) }) as _
            });
            let result = actor
                .prepare_idle()
                .unwrap()
                .rewind(boundary.clone(), checkpoint)
                .await;
            assert!(matches!(result, Err(ActorError::HistoryCheckpoint(_))));
            assert_eq!(actor.inner.history.load().epoch, original.epoch);
            assert_eq!(actor.inner.history.load().messages.len(), 2);
            let checkpoint: HistoryEditCheckpoint =
                Arc::new(|_: HistorySnapshot<Message>| Box::pin(async { Ok(()) }) as _);
            let guard = actor.prepare_idle().unwrap();
            let changed = guard
                .rewind(boundary.clone(), checkpoint.clone())
                .await
                .unwrap();
            assert_eq!(changed.messages.len(), 1);
            assert_ne!(changed.epoch, original.epoch);
            let guard = actor.prepare_idle().unwrap();
            let stale = guard.rewind(boundary, checkpoint).await;
            assert!(matches!(stale, Err(ActorError::StaleHistoryBoundary)));
            actor.close();
            runner.await;
        });
    }

    #[test_case(false; "foreign_actor")]
    #[test_case(true; "invalidated_by_compaction")]
    fn rewind_rejects_foreign_or_compacted_boundary(compacted: bool) {
        smol::block_on(async {
            const DRAFT: &str = "draft";
            const RUN: u64 = 1;
            let (actor, runner) = AgentActorHandle::spawn(
                AgentId::generate(),
                vec![Message::user(DRAFT.into())],
                None,
                Box::new(IdleBackend),
            );
            let (other, other_runner) = AgentActorHandle::spawn(
                AgentId::generate(),
                vec![Message::user(DRAFT.into())],
                None,
                Box::new(IdleBackend),
            );
            let boundary = if compacted {
                actor.history_boundaries().unwrap().remove(0)
            } else {
                other.history_boundaries().unwrap().remove(0)
            };
            if compacted {
                actor.push_compact(RUN, None).unwrap();
                actor.runtime_barrier().await.unwrap();
                assert!(actor.inner.history.load().compacted);
                assert_ne!(boundary.epoch, actor.inner.history.load().epoch);
            }
            let epoch = actor.inner.history.load().epoch;
            let result = actor
                .prepare_idle()
                .unwrap()
                .rewind(
                    boundary,
                    Arc::new(|_| panic!("stale boundary must not checkpoint")),
                )
                .await;
            assert!(matches!(result, Err(ActorError::StaleHistoryBoundary)));
            assert_eq!(actor.inner.history.load().epoch, epoch);
            assert_eq!(actor.committed_message_count(), 1);
            actor.close();
            other.close();
            runner.await;
            other_runner.await;
        });
    }

    #[test]
    fn boundary_messages_match_one_committed_snapshot() {
        smol::block_on(async {
            const FIRST: &str = "first";
            const NEXT: &str = "next";
            let messages = vec![
                Message::user(FIRST.into()),
                Message {
                    role: Role::Assistant,
                    content: vec![ContentBlock::Text {
                        text: "answer".into(),
                    }],
                    ..Default::default()
                },
                Message::user(NEXT.into()),
            ];
            let (actor, runner) =
                AgentActorHandle::spawn(AgentId::generate(), messages, None, Box::new(IdleBackend));
            let boundaries = actor.history_boundary_messages().unwrap();
            assert_eq!(
                boundaries
                    .iter()
                    .map(|(boundary, message)| (
                        boundary.offset,
                        message.first_text_content().unwrap()
                    ))
                    .collect::<Vec<_>>(),
                vec![(0, FIRST), (2, NEXT)]
            );
            assert!(
                boundaries
                    .iter()
                    .all(|(boundary, _)| boundary.agent_id == actor.agent_id()
                        && boundary.epoch == boundaries[0].0.epoch)
            );
            let boundary = boundaries[1].0.clone();
            actor
                .prepare_idle()
                .unwrap()
                .rewind(boundary.clone(), Arc::new(|_| Box::pin(async { Ok(()) })))
                .await
                .unwrap();
            assert!(actor.prepare_idle().is_ok());
            assert_eq!(boundaries[1].1.first_text_content(), Some(NEXT));
            assert!(matches!(
                actor.history_boundary_message(&boundary),
                Err(ActorError::StaleHistoryBoundary)
            ));
            actor.close();
            runner.await;
        });
    }

    #[test]
    fn rewind_boundaries_preserve_complete_tool_groups() {
        let messages = vec![
            Message::user("first".into()),
            Message {
                role: Role::Assistant,
                content: vec![ContentBlock::tool_use(
                    "tool",
                    "read",
                    serde_json::json!({}),
                )],
                ..Default::default()
            },
            Message::user("would split group".into()),
        ];
        assert!(valid_boundary(&messages, 0));
        assert!(!valid_boundary(&messages, 1));
        assert!(!valid_boundary(&messages, 2));
        assert!(!valid_boundary(&messages, 3));
    }
}
