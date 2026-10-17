use std::collections::HashSet;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;

use futures_lite::FutureExt;
use std::sync::Arc;

use maki_providers::{ContentBlock, Message, MessageKind, Role};
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

enum HistoryEditAction {
    Rewind {
        boundary: HistoryBoundary,
        completion: flume::Sender<Result<HistoryChange, ActorError>>,
    },
    Prepared {
        operation: Box<PreparedOperation>,
        clear_history: bool,
        completion: flume::Sender<Result<(PreparedCommit, Option<HistoryChange>), ActorError>>,
    },
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
        let state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.lifecycle != ActorLifecycle::Open {
            return Err(super::lifecycle_error(state.lifecycle));
        }
        let history = self.inner.history.load_full();
        Ok((0..history.messages.len())
            .filter(|&offset| valid_boundary(&history.messages, offset))
            .map(|offset| HistoryBoundary {
                agent_id: self.agent_id(),
                epoch: history.epoch,
                offset,
            })
            .collect())
    }
}

impl ActorIdleGuard {
    /// Starts an actor-owned commit after the caller has revalidated preparation.
    /// Once enqueued, dropping this future does not cancel the transaction.
    pub async fn commit_prepared(
        mut self,
        operation: PreparedOperationTicket,
        clear_history: bool,
        checkpoint: HistoryEditCheckpoint,
    ) -> Result<(PreparedCommit, Option<HistoryChange>), ActorError> {
        if !operation.belongs_to(&self.actor) {
            return Err(ActorError::PolicyCancelled);
        }
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
                    completion,
                },
                checkpoint,
            });
            self.release = false;
            actor.inner.queue.notify();
        }
        drop(operation);
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
    pub async fn execute(self, actor: &AgentActorHandle, history: &mut History) {
        let mut guard = ActorIdleGuard {
            actor: actor.clone(),
            owner: Arc::clone(&self.owner),
            release: true,
        };
        match self.action {
            HistoryEditAction::Rewind {
                ref boundary,
                ref completion,
            } => {
                let result =
                    Self::apply(&self.owner, boundary, &self.checkpoint, actor, history).await;
                let _ = completion.send(result);
                drop(guard);
            }
            HistoryEditAction::Prepared {
                operation,
                clear_history,
                completion,
            } => {
                let snapshot = clear_history.then(|| {
                    let mut snapshot = HistorySnapshot::new(Vec::new());
                    snapshot.compacted = actor.inner.history.load().compacted;
                    snapshot
                });
                let result = if let Some(snapshot) = &snapshot {
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
                        if let Some(ticket) = &committed.ticket
                            && let Some(reservation) = state.owned_idle(&self.owner)
                        {
                            reservation.permission = Some(ticket.turn_id());
                            reservation.committing = false;
                            guard.release = false;
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
                let _ = completion.send(result);
                drop(guard);
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
    if !matches!(message.role, Role::User)
        || !matches!(message.kind, MessageKind::Turn)
        || message.content.is_empty()
        || message
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::ToolResult { .. }))
    {
        return false;
    }
    let mut pending = HashSet::new();
    for message in &messages[..offset] {
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
    }
    pending.is_empty()
}

#[cfg(test)]
mod tests {
    use super::valid_boundary;
    use maki_providers::{ContentBlock, Message, Role};

    struct IdleBackend;

    impl crate::actor::ActorBackend for IdleBackend {
        fn run_turn<'a>(
            &'a mut self,
            _: &'a mut crate::History,
            _: crate::actor::TurnContext,
            _: crate::AgentInput,
            _: crate::actor::WorkKind,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = crate::actor::BackendResult> + Send + 'a>,
        > {
            Box::pin(async { unreachable!() })
        }

        fn run_control<'a>(
            &'a mut self,
            _: &'a mut crate::History,
            _: crate::actor::TurnContext,
            _: &'a crate::actor::ControlWork,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = crate::actor::BackendResult> + Send + 'a>,
        > {
            Box::pin(async { unreachable!() })
        }

        fn run_compact<'a>(
            &'a mut self,
            _: &'a mut crate::History,
            _: crate::actor::TurnContext,
            _: Option<&'a str>,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = crate::actor::BackendResult> + Send + 'a>,
        > {
            Box::pin(async { unreachable!() })
        }
    }

    #[test_case::test_case(false, false; "preserve_history")]
    #[test_case::test_case(true, false; "clear_history")]
    #[test_case::test_case(true, true; "clear_empty_history")]
    fn prepared_commit_preserves_identity(clear: bool, empty: bool) {
        smol::block_on(async {
            let messages = if empty {
                Vec::new()
            } else {
                vec![Message::user("draft".into())]
            };
            let (actor, runner) = crate::actor::AgentActorHandle::spawn(
                crate::AgentId::generate(),
                messages,
                None,
                Box::new(IdleBackend),
            );
            actor
                .initialize_config(crate::actor::EffectiveAgentConfig::new(
                    super::super::tests::policy(false),
                    crate::AgentMode::Build,
                ))
                .unwrap();
            let original = actor.config_snapshot().unwrap();
            let epoch = actor.inner.history.load().epoch;
            let guard = actor.prepare_idle().unwrap();
            let operation = actor.reserve_prepared_operation(None).unwrap();
            operation.resolve(None).unwrap();
            operation.wait_ready().await.unwrap();
            let (saved, checkpointed) = flume::bounded(1);
            let checkpoint: super::HistoryEditCheckpoint = std::sync::Arc::new(move |snapshot| {
                assert!(snapshot.messages.is_empty());
                saved.send(snapshot).unwrap();
                Box::pin(async { Ok(()) })
            });
            let (commit, change) = guard
                .commit_prepared(operation, clear, checkpoint)
                .await
                .unwrap();
            assert!(std::sync::Arc::ptr_eq(
                &original.identity,
                &commit.config.identity
            ));
            assert_eq!(original.generation, commit.config.generation);
            assert_eq!(change.is_some(), clear);
            if let Some(change) = change {
                let snapshot = checkpointed.recv().unwrap();
                assert_eq!(change.epoch, snapshot.epoch);
                assert!(std::sync::Arc::ptr_eq(&change.messages, &snapshot.messages));
                let published = actor.inner.history.load();
                assert_eq!(published.epoch, snapshot.epoch);
                assert_eq!(published.compacted, snapshot.compacted);
                assert!(std::sync::Arc::ptr_eq(
                    &published.messages,
                    &snapshot.messages
                ));
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
            let (actor, runner) = crate::actor::AgentActorHandle::spawn(
                crate::AgentId::generate(),
                Vec::new(),
                None,
                Box::new(IdleBackend),
            );
            let (other, other_runner) = crate::actor::AgentActorHandle::spawn(
                crate::AgentId::generate(),
                Vec::new(),
                None,
                Box::new(IdleBackend),
            );
            let operation = other.reserve_prepared_operation(None).unwrap();
            let checkpoint: super::HistoryEditCheckpoint =
                std::sync::Arc::new(|_| panic!("foreign operation must not checkpoint"));
            let result = actor
                .prepare_idle()
                .unwrap()
                .commit_prepared(operation, true, checkpoint)
                .await;
            assert!(matches!(
                result,
                Err(crate::actor::ActorError::PolicyCancelled)
            ));
            assert!(actor.prepare_idle().is_ok());
            assert!(other.prepare_idle().is_ok());
            actor.close();
            other.close();
            runner.await;
            other_runner.await;
        });
    }

    #[test_case::test_case(false, false; "local_cancel")]
    #[test_case::test_case(true, false; "local_cancel_and_close")]
    #[test_case::test_case(false, true; "local_cancel_and_shutdown")]
    fn prepared_checkpoint_survives_cancel_and_close(close: bool, shutdown: bool) {
        smol::block_on(async {
            let (actor, runner) = crate::actor::AgentActorHandle::spawn(
                crate::AgentId::generate(),
                vec![Message::user("draft".into())],
                None,
                Box::new(IdleBackend),
            );
            actor
                .initialize_config(crate::actor::EffectiveAgentConfig::new(
                    super::super::tests::policy(false),
                    crate::AgentMode::Build,
                ))
                .unwrap();
            let guard = actor.prepare_idle().unwrap();
            let turn = (close || shutdown).then(|| super::super::PreparedTurn {
                input: crate::AgentInput {
                    message: String::new(),
                    mode: crate::AgentMode::Build,
                    images: Vec::new(),
                    preamble: Vec::new(),
                    thinking: maki_providers::ThinkingConfig::Off,
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
            operation
                .resolve(Some(crate::actor::ConfigChange::ToggleFast))
                .unwrap();
            operation.wait_ready().await.unwrap();
            let (entered, entry) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            let checkpoint: super::HistoryEditCheckpoint = std::sync::Arc::new(move |messages| {
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
            assert!(matches!(
                actor.prepare_idle(),
                Err(crate::actor::ActorError::Busy)
            ));
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

    #[test]
    fn prepared_checkpoint_failure_preserves_config_and_history() {
        smol::block_on(async {
            const CHECKPOINT_ERROR: &str = "checkpoint unavailable";
            let (actor, runner) = crate::actor::AgentActorHandle::spawn(
                crate::AgentId::generate(),
                vec![Message::user("draft".into())],
                None,
                Box::new(IdleBackend),
            );
            actor
                .initialize_config(crate::actor::EffectiveAgentConfig::new(
                    super::super::tests::policy(false),
                    crate::AgentMode::Build,
                ))
                .unwrap();
            let original = actor.config_snapshot().unwrap();
            let epoch = actor.inner.history.load().epoch;
            let guard = actor.prepare_idle().unwrap();
            let operation = actor.reserve_prepared_operation(None).unwrap();
            operation
                .resolve(Some(crate::actor::ConfigChange::Patch(
                    crate::actor::ConfigPatch {
                        fast: Some(true),
                        ..Default::default()
                    },
                )))
                .unwrap();
            operation.wait_ready().await.unwrap();
            let checkpoint: super::HistoryEditCheckpoint =
                std::sync::Arc::new(|_| Box::pin(async { Err(CHECKPOINT_ERROR.into()) }));
            let result = guard.commit_prepared(operation, true, checkpoint).await;
            assert!(
                matches!(result, Err(crate::actor::ActorError::HistoryCheckpoint(error)) if error == CHECKPOINT_ERROR)
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
            let (actor, runner) = crate::actor::AgentActorHandle::spawn(
                crate::AgentId::generate(),
                vec![Message::user("first".into()), Message::user("draft".into())],
                None,
                Box::new(IdleBackend),
            );
            let boundary = actor.history_boundaries().unwrap().remove(1);
            let epoch = boundary.epoch;
            let (entered, entry) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            let checkpoint: super::HistoryEditCheckpoint = std::sync::Arc::new(move |messages| {
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
            assert!(matches!(
                actor.prepare_idle(),
                Err(crate::actor::ActorError::Busy)
            ));
            actor.close();
            release.send(()).unwrap();
            let change = edit.await.unwrap();
            assert_eq!(change.epoch, checkpointed.epoch);
            assert!(std::sync::Arc::ptr_eq(
                &change.messages,
                &checkpointed.messages
            ));
            let published = actor.inner.history.load();
            assert_eq!(published.epoch, checkpointed.epoch);
            assert_eq!(published.compacted, checkpointed.compacted);
            assert!(std::sync::Arc::ptr_eq(
                &published.messages,
                &checkpointed.messages
            ));
            assert_eq!(change.messages.len(), 1);
            assert_ne!(change.epoch, epoch);
            runner.await;
        });
    }

    #[test]
    fn rewind_serializes_concurrent_child_report() {
        smol::block_on(async {
            let (actor, runner) = crate::actor::AgentActorHandle::spawn(
                crate::AgentId::generate(),
                vec![Message::user("draft".into())],
                None,
                Box::new(IdleBackend),
            );
            let boundary = actor.history_boundaries().unwrap().remove(0);
            let (entered, entry) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            let checkpoint: super::HistoryEditCheckpoint = std::sync::Arc::new(move |_| {
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
            let turn_id = crate::TurnId::generate();
            let report = crate::actor::ChildDelivery {
                parent_id: actor.agent_id(),
                compatibility_id: None,
                label: None,
                child_config: None,
                correlation: "rewind-report".into(),
                result: crate::actor::TurnResult {
                    outcome: crate::TurnOutcome::cancelled(
                        crate::AgentId::generate(),
                        turn_id,
                        maki_providers::TokenUsage::default(),
                        0,
                        crate::TurnCancellationReason::User,
                    ),
                    text: "child report".into(),
                    output: Vec::new(),
                    provenance: Default::default(),
                    duration_ms: 0,
                },
            };
            super::super::delivery::admit_delivery(&actor.inner, std::sync::Arc::new(report))
                .unwrap();
            {
                let mut state = actor.inner.state.lock().unwrap();
                super::super::delivery::schedule(&actor.inner, &mut state);
                assert!(state.deliveries.contains_key(&turn_id));
                assert!(state.pop_work(&actor.inner.queue).is_none());
            }
            assert_eq!(actor.inner.history.load().messages.len(), 1);
            assert!(actor.withdraw_child_delivery(turn_id));
            release.send(()).unwrap();
            assert!(edit.await.unwrap().messages.is_empty());
            actor.close();
            runner.await;
        });
    }

    #[test]
    fn rewind_callback_panic_releases_reservation_without_publication() {
        smol::block_on(async {
            let (actor, runner) = crate::actor::AgentActorHandle::spawn(
                crate::AgentId::generate(),
                vec![Message::user("draft".into())],
                None,
                Box::new(IdleBackend),
            );
            let boundary = actor.history_boundaries().unwrap().remove(0);
            let epoch = boundary.epoch;
            let checkpoint: super::HistoryEditCheckpoint =
                std::sync::Arc::new(|_| panic!("callback panic"));
            let result = actor
                .prepare_idle()
                .unwrap()
                .rewind(boundary, checkpoint)
                .await;
            assert!(matches!(
                result,
                Err(crate::actor::ActorError::HistoryCheckpoint(_))
            ));
            assert_eq!(actor.inner.history.load().epoch, epoch);
            assert_eq!(actor.inner.history.load().messages.len(), 1);
            actor.close();
            runner.await;
        });
    }

    #[test]
    fn rewind_checkpoint_failure_restores_history() {
        smol::block_on(async {
            let (actor, runner) = crate::actor::AgentActorHandle::spawn(
                crate::AgentId::generate(),
                vec![Message::user("first".into()), Message::user("draft".into())],
                None,
                Box::new(IdleBackend),
            );
            let boundary = actor.history_boundaries().unwrap().remove(1);
            let original = actor.inner.history.load_full();
            let checkpoint: super::HistoryEditCheckpoint =
                std::sync::Arc::new(|_: maki_storage::sessions::HistorySnapshot<Message>| {
                    Box::pin(async { Err("checkpoint unavailable".to_owned()) }) as _
                });
            let result = actor
                .prepare_idle()
                .unwrap()
                .rewind(boundary.clone(), checkpoint)
                .await;
            assert!(matches!(
                result,
                Err(crate::actor::ActorError::HistoryCheckpoint(_))
            ));
            assert_eq!(actor.inner.history.load().epoch, original.epoch);
            assert_eq!(actor.inner.history.load().messages.len(), 2);
            let checkpoint: super::HistoryEditCheckpoint =
                std::sync::Arc::new(|_: maki_storage::sessions::HistorySnapshot<Message>| {
                    Box::pin(async { Ok(()) }) as _
                });
            actor.runtime_barrier().await.unwrap();
            let guard = actor.prepare_idle().unwrap();
            let changed = guard
                .rewind(boundary.clone(), checkpoint.clone())
                .await
                .unwrap();
            assert_eq!(changed.messages.len(), 1);
            assert_ne!(changed.epoch, original.epoch);
            actor.runtime_barrier().await.unwrap();
            let guard = actor.prepare_idle().unwrap();
            let stale = guard.rewind(boundary, checkpoint).await;
            assert!(matches!(
                stale,
                Err(crate::actor::ActorError::StaleHistoryBoundary)
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
