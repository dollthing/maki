use std::sync::{Arc, Weak};

use maki_providers::{CompleteModelSelection, Message, ObservationSource, RequestOptions};
use serde_json::{Value, json};

use super::{
    ActorEvent, ActorInner, ActorLifecycle, ActorOperation, AgentActorHandle, ConfigCommit,
    TurnOrigin, TurnProvenance, TurnResult, TurnTicket, drive_operations,
};
use crate::{AgentId, AgentInput, AgentMode, TurnCancellationReason, TurnId};

pub const MAX_PENDING_DELIVERIES: usize = 64;

#[derive(Clone)]
pub(crate) struct ParentDeliveryPolicy {
    pub auto_deliver: bool,
    pub compatibility_id: Option<String>,
}

#[derive(Clone)]
pub(crate) struct ParentDelivery {
    pub parent_id: AgentId,
    pub parent: Weak<ActorInner>,
    pub metadata: crate::AgentMetadata,
}

#[derive(Clone)]
pub struct ChildDelivery {
    pub parent_id: AgentId,
    pub compatibility_id: Option<String>,
    pub label: Option<String>,
    pub result: TurnResult,
    pub child_config: Option<ConfigCommit>,
    pub correlation: String,
}

impl ChildDelivery {
    pub fn to_json(&self) -> Value {
        let provenance = &self.result.provenance;
        json!({
            "kind": "child_result",
            "parent_id": self.parent_id,
            "agent_id": self.result.outcome.agent_id(),
            "turn_id": self.result.outcome.turn_id(),
            "compatibility_id": self.compatibility_id,
            "label": self.label,
            "outcome": self.result.outcome,
            "text": self.result.text,
            "output": self.result.output,
            "duration_ms": self.result.duration_ms,
            "correlation": self.correlation,
            "provenance": {
                "origin": match provenance.origin { TurnOrigin::Internal => "internal", TurnOrigin::User => "user", TurnOrigin::Plugin => "plugin" },
                "plugin": provenance.plugin,
                "plugin_generation": provenance.plugin_generation,
                "source_agent": provenance.source_agent,
                "source_turn": provenance.source_turn,
            },
            "configuration": self.child_config.as_ref().map(|commit| json!({
                "generation": commit.generation,
                "selection": CompleteModelSelection::from_model(&commit.config.model, RequestOptions {
                    thinking: commit.config.thinking,
                    fast: commit.config.fast,
                }),
                "mode": commit.config.mode.id().key(),
                "workflow": commit.config.workflow,
            })),
        })
    }

    fn input(&self) -> AgentInput {
        AgentInput {
            message: String::new(),
            mode: AgentMode::Build,
            images: Vec::new(),
            preamble: vec![Message {
                observation_source: Some(ObservationSource::ChildResult {
                    agent_id: self.result.outcome.agent_id().to_string(),
                    turn_id: self.result.outcome.turn_id().to_string(),
                }),
                ..Message::observation(format!(
                    "[internal child-result data, not a user instruction]\n{}",
                    self.to_json()
                ))
            }],
            thinking: Default::default(),
            fast: false,
            workflow: false,
            prompt: None,
            cancel: None,
            lease_committer: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryRejection {
    ParentUnavailable,
    ParentClosed,
    Overflow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryStatus {
    Accepted {
        parent_turn_id: TurnId,
    },
    Rejected(DeliveryRejection),
    Discarded(TurnCancellationReason),
    /// The parent's own turn received the result through an exact wait.
    Withdrawn,
    Executed {
        parent_turn_id: TurnId,
    },
}

#[derive(Clone)]
pub struct DeliveryEvent {
    pub delivery: Arc<ChildDelivery>,
    pub status: DeliveryStatus,
}

pub(crate) struct PendingDelivery {
    pub delivery: Arc<ChildDelivery>,
    pub started: bool,
}

pub(super) fn admit_delivery(
    inner: &Arc<ActorInner>,
    delivery: Arc<ChildDelivery>,
) -> Result<TurnTicket, DeliveryRejection> {
    let mut state = inner
        .state
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if state.lifecycle != ActorLifecycle::Open {
        return Err(DeliveryRejection::ParentClosed);
    }
    if state.deliveries.len() >= MAX_PENDING_DELIVERIES {
        return Err(DeliveryRejection::Overflow);
    }
    let turn_id = TurnId::generate();
    let ticket =
        TurnTicket::new(turn_id, Arc::clone(&inner.identity)).with_provenance(TurnProvenance {
            origin: TurnOrigin::Internal,
            source_agent: Some(delivery.result.outcome.agent_id()),
            source_turn: Some(delivery.result.outcome.turn_id()),
            ..TurnProvenance::default()
        });
    inner
        .tickets
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .insert(turn_id, ticket.clone());
    state.deliveries.insert(
        turn_id,
        PendingDelivery {
            delivery: Arc::clone(&delivery),
            started: false,
        },
    );
    state.next_operation_id = state.next_operation_id.wrapping_add(1);
    let id = state.next_operation_id;
    state.operations.push_back(ActorOperation::Turn {
        id,
        input: delivery.input(),
        event_sender: None,
        correlation: delivery.correlation.clone(),
        ticket: ticket.clone(),
        policy_constraint: None,
    });
    state.emit(ActorEvent::Delivery(DeliveryEvent {
        delivery,
        status: DeliveryStatus::Accepted {
            parent_turn_id: turn_id,
        },
    }));
    drive_operations(inner, &mut state);
    Ok(ticket)
}

pub(super) fn record(
    inner: &ActorInner,
    turn_id: TurnId,
    generation: u64,
    policy: Option<Arc<super::EffectiveAgentConfig>>,
    correlation: String,
) {
    if inner
        .managed_admission
        .as_ref()
        .is_some_and(|managed| managed.parent_delivery.is_some())
    {
        let config = policy.map(|config| ConfigCommit {
            identity: Arc::clone(&inner.identity),
            generation,
            config,
        });
        inner
            .delivery_records
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(turn_id, (config, correlation));
    }
}

pub(super) fn publish(
    inner: &ActorInner,
    ticket: &TurnTicket,
    admission: Option<&super::TurnAdmission>,
) {
    let Some(target) = inner
        .managed_admission
        .as_ref()
        .and_then(|managed| managed.parent_delivery.as_ref())
    else {
        return;
    };
    let recorded = inner
        .delivery_records
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .remove(&ticket.turn_id());
    let policy = {
        let state = inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.deliveries.contains_key(&ticket.turn_id()) {
            return;
        }
        state.parent_delivery_policy.clone()
    };
    let Some(policy) = policy.filter(|policy| policy.auto_deliver) else {
        return;
    };
    let (child_config, correlation) = recorded.unwrap_or_else(|| {
        let config = admission.and_then(|admission| {
            admission.policy.clone().map(|config| ConfigCommit {
                identity: Arc::clone(&inner.identity),
                generation: admission.generation,
                config,
            })
        });
        (
            config,
            admission
                .map(|admission| admission.correlation.clone())
                .unwrap_or_default(),
        )
    });
    let Some(result) = ticket.peek_result() else {
        return;
    };
    let delivery = Arc::new(ChildDelivery {
        parent_id: target.parent_id,
        compatibility_id: policy.compatibility_id,
        label: target.metadata.label.clone(),
        result,
        child_config,
        correlation,
    });
    let result = target
        .parent
        .upgrade()
        .ok_or(DeliveryRejection::ParentUnavailable)
        .and_then(|parent| admit_delivery(&parent, Arc::clone(&delivery)));
    if let Err(reason) = result {
        inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .emit(ActorEvent::Delivery(DeliveryEvent {
                delivery,
                status: DeliveryStatus::Rejected(reason),
            }));
    }
}

impl AgentActorHandle {
    /// Withdraws the queued delivery of `child_turn` once an exact wait in
    /// this agent's own turn has returned that result to the model.
    pub fn withdraw_child_delivery(&self, child_turn: TurnId) -> bool {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(delivery_turn) = state.deliveries.iter().find_map(|(turn_id, pending)| {
            (!pending.started && pending.delivery.result.outcome.turn_id() == child_turn)
                .then_some(*turn_id)
        }) else {
            return false;
        };
        let Ok(cut) =
            self.capture_turn_cancel(&mut state, delivery_turn, TurnCancellationReason::User)
        else {
            return false;
        };
        if let Some(pending) = state.deliveries.remove(&delivery_turn) {
            state.emit(ActorEvent::Delivery(DeliveryEvent {
                delivery: pending.delivery,
                status: DeliveryStatus::Withdrawn,
            }));
        }
        drop(state);
        self.settle_turn_cancel(cut, TurnCancellationReason::User);
        true
    }
}

pub(super) fn settle(inner: &ActorInner, turn_id: TurnId, reason: Option<TurnCancellationReason>) {
    let mut state = inner
        .state
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if let Some(pending) = state.deliveries.remove(&turn_id) {
        let status = if pending.started {
            DeliveryStatus::Executed {
                parent_turn_id: turn_id,
            }
        } else {
            DeliveryStatus::Discarded(reason.unwrap_or(TurnCancellationReason::Closed))
        };
        state.emit(ActorEvent::Delivery(DeliveryEvent {
            delivery: pending.delivery,
            status,
        }));
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    use maki_providers::TokenUsage;
    use test_case::test_case;

    use super::{
        ChildDelivery, DeliveryRejection, DeliveryStatus, MAX_PENDING_DELIVERIES, admit_delivery,
    };
    use crate::actor::{
        ActorBackend, ActorEvent, BackendResult, ControlWork, TurnContext, TurnProvenance,
        TurnResult, WorkKind,
    };
    use crate::{
        AgentActorHandle, AgentId, AgentInput, History, TurnCancellationReason, TurnId, TurnOutcome,
    };

    const CHILD_TEXT: &str = "child result";
    const CORRELATION: &str = "child correlation";

    struct UnusedBackend;

    impl ActorBackend for UnusedBackend {
        fn run_turn<'a>(
            &'a mut self,
            _: &'a mut History,
            _: TurnContext,
            _: AgentInput,
            _: WorkKind,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async { unreachable!("deferred runner must not execute") })
        }
        fn run_control<'a>(
            &'a mut self,
            _: &'a mut History,
            _: TurnContext,
            _: &'a ControlWork,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async { unreachable!("deferred runner must not execute") })
        }
        fn run_compact<'a>(
            &'a mut self,
            _: &'a mut History,
            _: TurnContext,
            _: Option<&'a str>,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async { unreachable!("deferred runner must not execute") })
        }
    }

    fn delivery(parent_id: AgentId) -> Arc<ChildDelivery> {
        Arc::new(ChildDelivery {
            parent_id,
            compatibility_id: None,
            label: None,
            child_config: None,
            correlation: CORRELATION.into(),
            result: TurnResult {
                outcome: TurnOutcome::cancelled(
                    AgentId::generate(),
                    TurnId::generate(),
                    TokenUsage::default(),
                    0,
                    TurnCancellationReason::User,
                ),
                text: CHILD_TEXT.into(),
                output: Vec::new(),
                provenance: TurnProvenance::default(),
                duration_ms: 0,
            },
        })
    }

    #[test]
    fn envelope_configuration_uses_normalized_selection_and_mode_key() {
        let mut envelope = (*delivery(AgentId::generate())).clone();
        let config = super::super::tests::policy(false);
        let expected_spec = config.model.spec();
        envelope.child_config = Some(super::ConfigCommit {
            identity: Arc::new(()),
            generation: 3,
            config: Arc::new(crate::EffectiveAgentConfig::new(
                config,
                crate::AgentMode::Build,
            )),
        });
        let configuration = &envelope.to_json()["configuration"];
        assert_eq!(configuration["generation"], 3);
        assert_eq!(configuration["mode"], "build");
        assert_eq!(configuration["selection"]["spec"], expected_spec);
        assert_eq!(configuration["selection"]["thinking"], "off");
        assert_eq!(configuration["selection"]["fast"], false);
        assert!(configuration.get("provider").is_none());
        assert!(configuration.get("thinking").is_none());
    }

    #[test]
    fn withdrawn_delivery_settles_once_without_running() {
        smol::block_on(async {
            let (_start, rx) = flume::bounded(1);
            let (actor, task) = AgentActorHandle::spawn_inner(
                AgentId::generate(),
                Vec::new(),
                None,
                Box::new(UnusedBackend),
                None,
                None,
                Some(rx),
            );
            let events = actor.subscribe_events();
            let envelope = delivery(actor.agent_id());
            let ticket = admit_delivery(&actor.inner, Arc::clone(&envelope)).unwrap();
            let child_turn = envelope.result.outcome.turn_id();
            assert!(!actor.withdraw_child_delivery(TurnId::generate()));
            assert!(actor.withdraw_child_delivery(child_turn));
            assert!(!actor.withdraw_child_delivery(child_turn));
            assert!(matches!(ticket.wait().await, TurnOutcome::Cancelled { .. }));
            let statuses: Vec<_> = events
                .try_iter()
                .filter_map(|event| match event {
                    ActorEvent::Delivery(event) => Some(event.status),
                    _ => None,
                })
                .collect();
            assert_eq!(
                statuses,
                vec![
                    DeliveryStatus::Accepted {
                        parent_turn_id: ticket.turn_id()
                    },
                    DeliveryStatus::Withdrawn,
                ]
            );
            assert!(actor.inner.state.lock().unwrap().deliveries.is_empty());
            actor.close();
            task.await;
        });
    }

    #[test_case(false; "clear")]
    #[test_case(true; "close")]
    fn queued_delivery_discards_once(close: bool) {
        smol::block_on(async {
            let (_start, rx) = flume::bounded(1);
            let (actor, task) = AgentActorHandle::spawn_inner(
                AgentId::generate(),
                Vec::new(),
                None,
                Box::new(UnusedBackend),
                None,
                None,
                Some(rx),
            );
            let events = actor.subscribe_events();
            let envelope = delivery(actor.agent_id());
            let ticket = admit_delivery(&actor.inner, Arc::clone(&envelope)).unwrap();
            assert_eq!(
                ticket.provenance().source_agent,
                Some(envelope.result.outcome.agent_id())
            );
            if close {
                actor.close();
            } else {
                actor.clear();
            }
            ticket.wait().await;
            let statuses: Vec<_> = events
                .try_iter()
                .filter_map(|event| match event {
                    ActorEvent::Delivery(event) => Some(event.status),
                    _ => None,
                })
                .collect();
            assert_eq!(
                statuses,
                vec![
                    DeliveryStatus::Accepted {
                        parent_turn_id: ticket.turn_id()
                    },
                    DeliveryStatus::Discarded(if close {
                        TurnCancellationReason::Closed
                    } else {
                        TurnCancellationReason::User
                    })
                ]
            );
            assert!(actor.inner.state.lock().unwrap().deliveries.is_empty());
            actor.close();
            task.await;
        });
    }

    #[test_case(false; "clear")]
    #[test_case(true; "close")]
    fn discarded_notification_does_not_notify_ancestor(close: bool) {
        smol::block_on(async {
            let (_start, rx) = flume::bounded(1);
            let (ancestor, ancestor_task) = AgentActorHandle::spawn_inner(
                AgentId::generate(),
                Vec::new(),
                None,
                Box::new(UnusedBackend),
                None,
                None,
                Some(rx),
            );
            let mut managed = crate::actor::types::ManagedTurnAdmission::new(
                std::sync::Weak::new(),
                AgentId::generate(),
            );
            managed.parent_delivery = Some(super::ParentDelivery {
                parent_id: ancestor.agent_id(),
                parent: Arc::downgrade(&ancestor.inner),
                metadata: Default::default(),
            });
            let (_start, rx) = flume::bounded(1);
            let (parent, parent_task) = AgentActorHandle::spawn_inner(
                managed.agent_id,
                Vec::new(),
                None,
                Box::new(UnusedBackend),
                Some(managed),
                None,
                Some(rx),
            );
            parent.configure_parent_delivery(true, None).unwrap();
            let ancestor_events = ancestor.subscribe_events();
            let ticket = admit_delivery(&parent.inner, delivery(parent.agent_id())).unwrap();
            if close {
                parent.close();
            } else {
                parent.clear();
            }
            ticket.wait().await;
            assert!(
                ancestor_events
                    .try_iter()
                    .all(|event| !matches!(event, ActorEvent::Delivery(_)))
            );
            assert_eq!(ancestor.snapshot().queued, 0);
            assert!(ancestor.inner.tickets.lock().unwrap().is_empty());
            parent.close();
            ancestor.close();
            parent_task.await;
            ancestor_task.await;
        });
    }

    #[test]
    fn deferred_cancel_preserves_admission_configuration_and_correlation() {
        smol::block_on(async {
            const CORRELATION: &str = "cancelled before materialization";
            let (_start, rx) = flume::bounded(1);
            let (parent, parent_task) = AgentActorHandle::spawn_inner(
                AgentId::generate(),
                Vec::new(),
                None,
                Box::new(UnusedBackend),
                None,
                None,
                Some(rx),
            );
            let mut managed = crate::actor::types::ManagedTurnAdmission::new(
                std::sync::Weak::new(),
                AgentId::generate(),
            );
            managed.parent_delivery = Some(super::ParentDelivery {
                parent_id: parent.agent_id(),
                parent: Arc::downgrade(&parent.inner),
                metadata: Default::default(),
            });
            let (_start, rx) = flume::bounded(1);
            let config = crate::EffectiveAgentConfig::new(
                super::super::tests::policy(false),
                crate::AgentMode::Build,
            );
            let (child, child_task) = AgentActorHandle::spawn_inner(
                managed.agent_id,
                Vec::new(),
                None,
                Box::new(UnusedBackend),
                Some(managed),
                Some(Arc::new(config)),
                Some(rx),
            );
            child.configure_parent_delivery(true, None).unwrap();
            let barrier = child.reserve_config_update().unwrap();
            let events = parent.subscribe_events();
            let ticket = child
                .admit_turn(
                    delivery(parent.agent_id()).input(),
                    None,
                    CORRELATION.into(),
                )
                .unwrap();
            child.cancel_turn(ticket.turn_id()).unwrap();
            ticket.wait().await;
            let event = events
                .try_iter()
                .find_map(|event| match event {
                    ActorEvent::Delivery(event) => Some(event),
                    _ => None,
                })
                .unwrap();
            assert_eq!(event.delivery.correlation, CORRELATION);
            let config = event.delivery.child_config.as_ref().unwrap();
            assert_eq!(config.generation, 0);
            assert_eq!(config.config.mode, crate::AgentMode::Build);
            assert!(child.inner.delivery_records.lock().unwrap().is_empty());
            drop(barrier);
            child.close();
            parent.close();
            child_task.await;
            parent_task.await;
        });
    }

    #[test_case(true; "enabled")]
    #[test_case(false; "disabled")]
    fn closed_parent_rejection_uses_creation_policy(auto_deliver: bool) {
        smol::block_on(async {
            const COMPATIBILITY_ID: &str = "explicit compatibility";
            const TOOL_ID: &str = "tool metadata is not routing identity";
            let (_parent_start, parent_rx) = flume::bounded(1);
            let (parent, parent_task) = AgentActorHandle::spawn_inner(
                AgentId::generate(),
                Vec::new(),
                None,
                Box::new(UnusedBackend),
                None,
                None,
                Some(parent_rx),
            );
            let mut admission = crate::actor::types::ManagedTurnAdmission::new(
                std::sync::Weak::new(),
                AgentId::generate(),
            );
            admission.parent_delivery = Some(super::ParentDelivery {
                parent_id: parent.agent_id(),
                parent: Arc::downgrade(&parent.inner),
                metadata: crate::AgentMetadata {
                    spawned_by_tool_use_id: Some(TOOL_ID.into()),
                    ..Default::default()
                },
            });
            let (_child_start, child_rx) = flume::bounded(1);
            let (child, child_task) = AgentActorHandle::spawn_inner(
                admission.agent_id,
                Vec::new(),
                None,
                Box::new(UnusedBackend),
                Some(admission),
                None,
                Some(child_rx),
            );
            child
                .configure_parent_delivery(auto_deliver, Some(COMPATIBILITY_ID.into()))
                .unwrap();
            let events = child.subscribe_events();
            parent.close();
            let ticket = crate::actor::TurnTicket::new(TurnId::generate(), child.identity());
            ticket.resolve(TurnOutcome::cancelled(
                child.agent_id(),
                ticket.turn_id(),
                TokenUsage::default(),
                0,
                TurnCancellationReason::User,
            ));
            super::publish(&child.inner, &ticket, None);
            let deliveries: Vec<_> = events
                .try_iter()
                .filter_map(|event| match event {
                    ActorEvent::Delivery(event) => Some(event),
                    _ => None,
                })
                .collect();
            if auto_deliver {
                assert_eq!(deliveries.len(), 1);
                assert_eq!(
                    deliveries[0].status,
                    DeliveryStatus::Rejected(DeliveryRejection::ParentClosed)
                );
                assert_eq!(
                    deliveries[0].delivery.compatibility_id.as_deref(),
                    Some(COMPATIBILITY_ID)
                );
            } else {
                assert!(deliveries.is_empty());
            }
            assert!(parent.inner.state.lock().unwrap().deliveries.is_empty());
            child.close();
            child_task.await;
            parent_task.await;
        });
    }

    #[test]
    fn bounded_admission_and_internal_observation() {
        smol::block_on(async {
            let (_start, rx) = flume::bounded(1);
            let (actor, task) = AgentActorHandle::spawn_inner(
                AgentId::generate(),
                Vec::new(),
                None,
                Box::new(UnusedBackend),
                None,
                None,
                Some(rx),
            );
            let envelope = delivery(actor.agent_id());
            let input = envelope.input();
            assert!(input.message.is_empty());
            assert!(input.preamble[0].is_observation());
            assert_eq!(envelope.to_json()["text"], CHILD_TEXT);
            for _ in 0..MAX_PENDING_DELIVERIES {
                admit_delivery(&actor.inner, Arc::clone(&envelope)).unwrap();
            }
            assert!(matches!(
                admit_delivery(&actor.inner, Arc::clone(&envelope)),
                Err(DeliveryRejection::Overflow)
            ));
            actor.close();
            assert!(matches!(
                admit_delivery(&actor.inner, envelope),
                Err(DeliveryRejection::ParentClosed)
            ));
            task.await;
        });
    }
}
