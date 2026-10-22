//! The one sequence every agent config change commits through.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};

use event_listener::Event;

use maki_config::ModelPolicy;
use maki_providers::Model;
use maki_storage::checkpoint::{CheckpointClaim, CheckpointPublication};
use thiserror::Error;

use crate::actor::{
    ConfigChange, ConfigCommit, ConfigPatch, EffectiveAgentConfig, PreparedModel,
    PreparedOperationTicket,
};
use crate::session_coordinator::{
    ConfigMutation, ModelPreparer, SessionCoordinatorError, SessionCoordinatorHandle,
    publication_claim,
};
use crate::session_options::{MODEL_OPTION_ID, SessionOptionError};
use crate::{ActorError, AgentActorHandle, AgentRef, ManagerError};

#[derive(Debug, Clone, PartialEq, Error)]
pub enum ConfigChangeError {
    #[error(transparent)]
    Target(#[from] ManagerError),
    #[error(transparent)]
    Actor(#[from] ActorError),
    #[error(transparent)]
    Coordinator(#[from] SessionCoordinatorError),
    #[error(transparent)]
    Option(#[from] SessionOptionError),
    #[error("model preparation failed: {0}")]
    ModelPreparation(Arc<str>),
    #[error("directory preparation failed: {0}")]
    DirectoryPreparation(Arc<str>),
    #[error("{0}")]
    Rejected(Arc<str>),
}

impl From<String> for ConfigChangeError {
    fn from(error: String) -> Self {
        Self::Rejected(error.into())
    }
}

impl From<&str> for ConfigChangeError {
    fn from(error: &str) -> Self {
        Self::Rejected(error.into())
    }
}

impl ConfigChangeError {
    /// Refused because an idle guard or the session option lease was held.
    pub fn is_busy(&self) -> bool {
        matches!(
            self,
            Self::Actor(ActorError::Busy)
                | Self::Coordinator(SessionCoordinatorError::SessionBusy(_))
        )
    }

    async fn wait_released(&self, actor: &AgentActorHandle) {
        match self {
            Self::Actor(ActorError::Busy) => actor.wait_unguarded().await,
            Self::Coordinator(SessionCoordinatorError::SessionBusy(id)) => {
                if let Ok(coordinator) = SessionCoordinatorHandle::resolve(*id) {
                    coordinator.read().wait_unreserved().await;
                }
            }
            _ => {}
        }
    }
}

type MutationReserver<'a> =
    Box<dyn FnMut(CheckpointPublication) -> Result<ConfigMutation, ConfigChangeError> + Send + 'a>;

/// Changes that met a busy target, in dispatch order. Only the head retries,
/// so a later change never overtakes an earlier waiting one.
#[derive(Default)]
pub(crate) struct ConfigChangeQueue {
    waiting: Mutex<VecDeque<Arc<()>>>,
    advanced: Event,
}

impl ConfigChangeQueue {
    fn lock(&self) -> MutexGuard<'_, VecDeque<Arc<()>>> {
        self.waiting.lock().unwrap_or_else(|e| e.into_inner())
    }
}

struct QueuedChange {
    actor: AgentActorHandle,
    place: Arc<()>,
}

impl QueuedChange {
    fn enqueue(actor: AgentActorHandle) -> Self {
        let place = Arc::new(());
        actor
            .inner
            .config_changes
            .lock()
            .push_back(Arc::clone(&place));
        Self { actor, place }
    }

    async fn wait_turn(&self) {
        let queue = &self.actor.inner.config_changes;
        loop {
            let listener = queue.advanced.listen();
            if queue
                .lock()
                .front()
                .is_some_and(|head| Arc::ptr_eq(head, &self.place))
            {
                return;
            }
            listener.await;
        }
    }
}

impl Drop for QueuedChange {
    fn drop(&mut self) {
        let queue = &self.actor.inner.config_changes;
        queue
            .lock()
            .retain(|place| !Arc::ptr_eq(place, &self.place));
        queue.advanced.notify(usize::MAX);
    }
}

enum Slot<'a> {
    Reserved {
        mutation: ConfigMutation,
        operation: PreparedOperationTicket,
    },
    Queued {
        place: QueuedChange,
        reserve: MutationReserver<'a>,
    },
}

/// A slot in an agent's config FIFO. Reserving takes the durable mutation and
/// then the actor operation synchronously, so changes hold the session lease
/// in dispatch order. A change that meets a held lease or idle guard queues
/// and takes its slot once the holder lets go; it fails only if the target
/// closes or the change is rejected.
pub struct ConfigChangeTicket<'a> {
    publication: CheckpointPublication,
    slot: Slot<'a>,
}

impl<'a> ConfigChangeTicket<'a> {
    pub fn reserve(
        target: &AgentRef,
        mut publication: CheckpointPublication,
        mut reserve: impl FnMut(CheckpointPublication) -> Result<ConfigMutation, ConfigChangeError>
        + Send
        + 'a,
    ) -> Result<Self, ConfigChangeError> {
        target.validate_live()?;
        let actor = target.actor()?;
        let queued = !actor.inner.config_changes.lock().is_empty();
        if !queued {
            match try_reserve(&actor, &publication, &mut reserve) {
                Ok(slot) => return Ok(Self { publication, slot }),
                Err(error) if error.is_busy() => publication = CheckpointPublication::default(),
                Err(error) => return Err(error),
            }
        }
        Ok(Self {
            publication,
            slot: Slot::Queued {
                place: QueuedChange::enqueue(actor),
                reserve: Box::new(reserve),
            },
        })
    }

    async fn reserved(&mut self) -> Result<&PreparedOperationTicket, ConfigChangeError> {
        if let Slot::Queued { place, reserve } = &mut self.slot {
            place.wait_turn().await;
            let reserved = loop {
                match try_reserve(&place.actor, &self.publication, reserve) {
                    Err(error) if error.is_busy() => {
                        self.publication = CheckpointPublication::default();
                        error.wait_released(&place.actor).await;
                    }
                    result => break result?,
                }
            };
            self.slot = reserved;
        }
        match &self.slot {
            Slot::Reserved { operation, .. } => Ok(operation),
            Slot::Queued { .. } => unreachable!("a queued change is reserved above"),
        }
    }

    pub async fn wait_predecessor(
        &mut self,
    ) -> Result<Arc<EffectiveAgentConfig>, ConfigChangeError> {
        Ok(self.reserved().await?.wait_predecessor().await?)
    }

    /// Cancelling it before the claim rolls the change back. A refused
    /// attempt cancels its publication, so a change that queued publishes
    /// through a fresh one, taken here once it is reserved.
    pub fn publication(&self) -> CheckpointPublication {
        self.publication.clone()
    }

    /// Once the predecessor settles, `prepare` derives the change from its
    /// config and returns the claim the durable publication must pass.
    pub async fn commit(
        mut self,
        prepare: impl AsyncFnOnce(
            CheckpointPublication,
            Arc<EffectiveAgentConfig>,
        ) -> Result<(CheckpointClaim, ConfigChange), ConfigChangeError>,
    ) -> Result<ConfigCommit, ConfigChangeError> {
        let predecessor = self.wait_predecessor().await?;
        let Slot::Reserved {
            mutation,
            operation,
        } = self.slot
        else {
            unreachable!("waiting for the predecessor reserves the change")
        };
        let (claim, change) = prepare(self.publication, predecessor).await?;
        operation.resolve(Some(change))?;
        operation.wait_ready().await?;
        let transaction = mutation(operation.ready_config()?, claim)?;
        Ok(operation.commit_durable(transaction).await?.config)
    }

    /// Commits a change fixed at dispatch under the default claim.
    pub async fn commit_change(
        self,
        change: ConfigChange,
    ) -> Result<ConfigCommit, ConfigChangeError> {
        self.commit(async |_, _| Ok((publication_claim(), change)))
            .await
    }
}

fn try_reserve<'a>(
    actor: &AgentActorHandle,
    publication: &CheckpointPublication,
    reserve: &mut dyn FnMut(CheckpointPublication) -> Result<ConfigMutation, ConfigChangeError>,
) -> Result<Slot<'a>, ConfigChangeError> {
    let mutation = reserve(publication.clone())?;
    let operation = actor.reserve_prepared_operation(None)?;
    operation.register_publication(publication)?;
    Ok(Slot::Reserved {
        mutation,
        operation,
    })
}

/// Resolves {spec} under {policy} and prepares its provider.
pub async fn prepare_model(
    spec: &str,
    policy: &ModelPolicy,
    prepare: &ModelPreparer,
) -> Result<PreparedModel, ConfigChangeError> {
    if !policy.allows(spec) {
        return Err(SessionOptionError::PolicyRejected(spec.into()).into());
    }
    let model = Model::from_spec(spec).map_err(|_| SessionOptionError::InvalidValue {
        id: MODEL_OPTION_ID.into(),
        value: spec.into(),
    })?;
    prepare(model)
        .await
        .map_err(ConfigChangeError::ModelPreparation)
}

/// Fills {patch}'s model from {spec}, when one is given.
pub async fn prepare_patch(
    mut patch: ConfigPatch,
    spec: Option<&str>,
    policy: &ModelPolicy,
    prepare: &ModelPreparer,
) -> Result<ConfigPatch, ConfigChangeError> {
    if let Some(spec) = spec {
        patch.model = Some(prepare_model(spec, policy, prepare).await?);
    }
    Ok(patch)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use maki_config::ModelPolicy;

    use super::{ConfigChangeError, ModelPreparer, SessionOptionError, prepare_model};

    const ALLOWED_SPEC: &str = "anthropic/claude-sonnet-4-20250514";
    const DISALLOWED_SPEC: &str = "openai/gpt-5";

    #[test]
    fn prepare_model_rejects_disallowed_spec_before_preparing() {
        let policy = ModelPolicy::new(&[ALLOWED_SPEC.into()], &[]).unwrap();
        let prepare: ModelPreparer = Arc::new(|_| unreachable!());
        let result = smol::block_on(prepare_model(DISALLOWED_SPEC, &policy, &prepare));
        assert!(matches!(
            result,
            Err(ConfigChangeError::Option(SessionOptionError::PolicyRejected(spec)))
                if *spec == *DISALLOWED_SPEC
        ));
    }
}
