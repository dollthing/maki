//! The one sequence every agent config change commits through.

use std::future::Future;
use std::sync::Arc;

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
use crate::{ActorError, AgentRef, ManagerError};

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

    /// Waits until the holder that refused this change as busy lets go of
    /// {target}. Returns at once for any other error.
    pub async fn wait_released(&self, target: &AgentRef) {
        match self {
            Self::Actor(ActorError::Busy) => {
                if let Ok(actor) = target.actor() {
                    actor.wait_unguarded().await;
                }
            }
            Self::Coordinator(SessionCoordinatorError::SessionBusy(id)) => {
                if let Ok(coordinator) = SessionCoordinatorHandle::resolve(*id) {
                    coordinator.read().wait_unreserved().await;
                }
            }
            _ => {}
        }
    }
}

/// A reserved slot in an agent's config FIFO. Reserving takes the durable
/// mutation and then the actor operation synchronously, so changes hold the
/// session lease in the order they were dispatched and a refused reservation
/// leaves nothing queued.
pub struct ConfigChangeTicket {
    mutation: ConfigMutation,
    operation: PreparedOperationTicket,
    publication: CheckpointPublication,
}

impl ConfigChangeTicket {
    pub fn reserve(
        target: &AgentRef,
        publication: CheckpointPublication,
        mutation: impl FnOnce(CheckpointPublication) -> Result<ConfigMutation, ConfigChangeError>,
    ) -> Result<Self, ConfigChangeError> {
        target.validate_live()?;
        let mutation = mutation(publication.clone())?;
        let operation = target.actor()?.reserve_prepared_operation(None)?;
        operation.register_publication(&publication)?;
        Ok(Self {
            mutation,
            operation,
            publication,
        })
    }

    pub fn wait_predecessor(
        &self,
    ) -> impl Future<Output = Result<Arc<EffectiveAgentConfig>, ConfigChangeError>> + Send + '_
    {
        let operation = &self.operation;
        async move { Ok(operation.wait_predecessor().await?) }
    }

    /// Cancelling it before the claim rolls the change back.
    pub fn publication(&self) -> CheckpointPublication {
        self.publication.clone()
    }

    /// Once the predecessor settles, `prepare` derives the change from its
    /// config and returns the claim the durable publication must pass.
    pub async fn commit(
        self,
        prepare: impl AsyncFnOnce(
            CheckpointPublication,
            Arc<EffectiveAgentConfig>,
        ) -> Result<(CheckpointClaim, ConfigChange), ConfigChangeError>,
    ) -> Result<ConfigCommit, ConfigChangeError> {
        let predecessor = self.operation.wait_predecessor().await?;
        let (claim, change) = prepare(self.publication, predecessor).await?;
        self.operation.resolve(Some(change))?;
        self.operation.wait_ready().await?;
        let transaction = (self.mutation)(self.operation.ready_config()?, claim)?;
        Ok(self.operation.commit_durable(transaction).await?.config)
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
