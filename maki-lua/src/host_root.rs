use maki_agent::{
    AgentId,
    cancel::{CancelToken, CancelTrigger, PublicationRegistration},
    session_coordinator::SessionCoordinatorHandle,
    session_options::{
        DISABLED_VALUE, ENABLED_VALUE, FAST_OPTION_ID, MODEL_OPTION_ID, SessionOptionsSnapshot,
        THINKING_OPTION_ID,
    },
    tools::ToolContext,
};
use maki_config::ModelPolicy;
use maki_providers::{
    CompleteModelSelection, Model, ModelSelection, RequestOptions, provider::available_model_specs,
};
use maki_storage::{
    checkpoint::{CheckpointClaim, CheckpointPublication},
    id::MakiId,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use thiserror::Error;

pub const FIXED_MODEL_ERROR: &str = "model changes are unavailable for this fixed host root";
pub const UNSUPPORTED_OPERATION_ERROR: &str = "operation is unavailable for this host root";

#[derive(Debug, Error)]
pub enum HostRootError {
    #[error("host root reference is stale")]
    Stale,
    #[error("host root reference belongs to another runtime")]
    WrongRuntime,
    #[error("{FIXED_MODEL_ERROR}")]
    FixedModel,
    #[error("{UNSUPPORTED_OPERATION_ERROR}")]
    UnsupportedOperation,
    #[error("invalid host model selection: {0}")]
    InvalidSelection(String),
    #[error(transparent)]
    Coordinator(#[from] maki_agent::session_coordinator::SessionCoordinatorError),
}

struct Identity {
    id: AgentId,
    alive: AtomicBool,
    publication_cancel: CancelToken,
    publication_trigger: Mutex<Option<CancelTrigger>>,
}

#[derive(Clone)]
pub struct HostRootRef {
    identity: Arc<Identity>,
}

impl HostRootRef {
    pub fn id(&self) -> AgentId {
        self.identity.id
    }

    pub fn same_runtime(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.identity, &other.identity)
    }
}

enum ModelSource {
    Coordinator(SessionCoordinatorHandle),
    Fixed(CompleteModelSelection),
}

/// Non-TUI roots support model reads and discovery, with coordinator-backed
/// model mutations when available. Graph, turn, and lifecycle operations are
/// unsupported; this adapter does not manufacture managed agent identities.
pub struct HostRootServices {
    reference: HostRootRef,
    source: ModelSource,
    policy: Arc<ModelPolicy>,
}

pub struct HostRootRetirementGuard {
    services: Arc<HostRootServices>,
}

impl Drop for HostRootRetirementGuard {
    fn drop(&mut self) {
        self.services.retire();
    }
}

impl HostRootServices {
    pub fn retirement_guard(self: &Arc<Self>) -> HostRootRetirementGuard {
        HostRootRetirementGuard {
            services: Arc::clone(self),
        }
    }

    pub fn coordinated(
        id: AgentId,
        coordinator: SessionCoordinatorHandle,
        policy: Arc<ModelPolicy>,
    ) -> Self {
        Self::new(id, ModelSource::Coordinator(coordinator), policy)
    }

    pub fn fixed(
        id: AgentId,
        model: &Model,
        options: RequestOptions,
        policy: Arc<ModelPolicy>,
    ) -> Self {
        Self::new(
            id,
            ModelSource::Fixed(CompleteModelSelection::from_model(model, options)),
            policy,
        )
    }

    fn new(id: AgentId, source: ModelSource, policy: Arc<ModelPolicy>) -> Self {
        let (trigger, cancel) = CancelToken::new();
        Self {
            reference: HostRootRef {
                identity: Arc::new(Identity {
                    id,
                    alive: AtomicBool::new(true),
                    publication_cancel: cancel,
                    publication_trigger: Mutex::new(Some(trigger)),
                }),
            },
            source,
            policy,
        }
    }

    pub fn reference(&self) -> HostRootRef {
        self.reference.clone()
    }

    pub fn session_id(&self) -> Option<MakiId> {
        match &self.source {
            ModelSource::Coordinator(coordinator) => Some(coordinator.session_id()),
            ModelSource::Fixed(_) => None,
        }
    }

    pub fn validate(&self, reference: &HostRootRef) -> Result<(), HostRootError> {
        if !self.reference.same_runtime(reference) {
            return Err(HostRootError::WrongRuntime);
        }
        if !reference.identity.alive.load(Ordering::Acquire) {
            return Err(HostRootError::Stale);
        }
        if let ModelSource::Coordinator(coordinator) = &self.source {
            let current = SessionCoordinatorHandle::resolve(coordinator.session_id())?;
            if current.runtime_epoch() != coordinator.runtime_epoch() {
                return Err(HostRootError::Stale);
            }
        }
        Ok(())
    }

    pub fn register_publication(
        &self,
        reference: &HostRootRef,
        publication: &CheckpointPublication,
    ) -> Result<(PublicationRegistration, Option<PublicationRegistration>), HostRootError> {
        self.validate(reference)?;
        let coordinator = match &self.source {
            ModelSource::Coordinator(coordinator) => {
                Some(coordinator.register_publication(publication)?)
            }
            ModelSource::Fixed(_) => None,
        };
        let trigger = reference
            .identity
            .publication_trigger
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if trigger.is_none() {
            publication.cancel();
            return Err(HostRootError::Stale);
        }
        Ok((
            reference
                .identity
                .publication_cancel
                .register_publication(publication),
            coordinator,
        ))
    }

    /// Retires authority only; session shutdown remains the host's responsibility.
    pub fn retire(&self) {
        let identity = &self.reference.identity;
        let mut trigger = identity
            .publication_trigger
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        trigger.take();
        identity.alive.store(false, Ordering::Release);
    }

    pub fn model(&self, reference: &HostRootRef) -> Result<CompleteModelSelection, HostRootError> {
        self.validate(reference)?;
        match &self.source {
            ModelSource::Coordinator(coordinator) => {
                selection_from_options(&coordinator.read().options())
            }
            ModelSource::Fixed(selection) => Ok(selection.clone()),
        }
    }

    pub fn available_models(&self, reference: &HostRootRef) -> Result<Vec<String>, HostRootError> {
        self.validate(reference)?;
        Ok(available_model_specs(&self.policy))
    }

    pub async fn set_model(
        &self,
        reference: &HostRootRef,
        patch: ModelSelection,
    ) -> Result<CompleteModelSelection, HostRootError> {
        let publication = CheckpointPublication::default();
        let registrations = self.register_publication(reference, &publication)?;
        let request = publication.clone();
        let claim: CheckpointClaim = Arc::new(move |publication| {
            let _registrations = &registrations;
            if request.same_request(publication) && publication.try_publish() {
                Ok(())
            } else {
                Err(Arc::from(HostRootError::Stale.to_string()))
            }
        });
        self.set_model_with_claim(reference, patch, publication, claim)
            .await
    }

    pub async fn set_model_with_claim(
        &self,
        reference: &HostRootRef,
        patch: ModelSelection,
        publication: CheckpointPublication,
        claim: CheckpointClaim,
    ) -> Result<CompleteModelSelection, HostRootError> {
        self.validate(reference)?;
        let ModelSource::Coordinator(coordinator) = &self.source else {
            return Err(HostRootError::FixedModel);
        };
        let snapshot = coordinator
            .set_model_with_claim(patch.spec, patch.fast, patch.thinking, publication, claim)
            .await?;
        selection_from_options(&snapshot)
    }

    pub fn unsupported(&self, reference: &HostRootRef) -> Result<(), HostRootError> {
        self.validate(reference)?;
        Err(HostRootError::UnsupportedOperation)
    }
}

impl Drop for HostRootServices {
    fn drop(&mut self) {
        self.retire();
    }
}

/// Reads the provider configuration captured at tool admission, not live options.
pub fn admitted_model(context: &ToolContext) -> CompleteModelSelection {
    CompleteModelSelection::from_model(&context.model, context.opts)
}

pub fn selection_from_options(
    snapshot: &SessionOptionsSnapshot,
) -> Result<CompleteModelSelection, HostRootError> {
    let value = |id: &str| {
        snapshot
            .options
            .iter()
            .find(|option| option.definition.id.as_ref() == id)
            .map(|option| option.current_value.as_ref())
            .ok_or_else(|| HostRootError::InvalidSelection(format!("missing {id} option")))
    };
    let model = Model::from_spec(value(MODEL_OPTION_ID)?)
        .map_err(|error| HostRootError::InvalidSelection(error.to_string()))?;
    let thinking = maki_providers::parse_thinking(value(THINKING_OPTION_ID)?)
        .map_err(|error| HostRootError::InvalidSelection(error.to_string()))?;
    let fast = match value(FAST_OPTION_ID)? {
        ENABLED_VALUE => true,
        DISABLED_VALUE => false,
        value => {
            return Err(HostRootError::InvalidSelection(format!(
                "invalid fast value: {value}"
            )));
        }
    };
    Ok(CompleteModelSelection::from_model(
        &model,
        RequestOptions { thinking, fast },
    ))
}

#[cfg(test)]
mod tests {
    use super::{HostRootError, HostRootServices};
    use maki_config::ModelPolicy;
    use maki_providers::{Model, ModelSelection, RequestOptions, ThinkingConfig};
    use std::sync::Arc;
    use test_case::test_case;

    fn fixed() -> HostRootServices {
        HostRootServices::fixed(
            maki_agent::AgentId::generate(),
            &Model::from_spec("anthropic/sonnet").unwrap(),
            RequestOptions::default(),
            Arc::new(ModelPolicy::default()),
        )
    }

    #[test_case(ModelSelection::default(); "empty")]
    #[test_case(ModelSelection { spec: Some(Arc::from("anthropic/sonnet")), ..ModelSelection::default() }; "same_spec")]
    #[test_case(ModelSelection { fast: Some(false), thinking: Some(ThinkingConfig::Off), ..ModelSelection::default() }; "disabled_options")]
    fn fixed_root_rejects_every_patch(patch: ModelSelection) {
        let services = fixed();
        let reference = services.reference();
        let before = services.model(&reference).unwrap();
        assert!(matches!(
            smol::block_on(services.set_model(&reference, patch)),
            Err(HostRootError::FixedModel)
        ));
        assert_eq!(services.model(&reference).unwrap(), before);
    }

    #[test]
    fn host_shutdown_retires_services_held_by_lua() {
        let services = Arc::new(fixed());
        let reference = services.reference();
        let guard = services.retirement_guard();
        let lua_services = Arc::clone(&services);
        drop(services);
        assert!(lua_services.model(&reference).is_ok());
        drop(guard);
        assert!(matches!(
            lua_services.model(&reference),
            Err(HostRootError::Stale)
        ));
    }

    #[test]
    fn references_do_not_retarget_or_survive_retirement() {
        let services = fixed();
        let other = fixed();
        let reference = services.reference();
        assert!(reference.same_runtime(&services.reference()));
        assert!(matches!(
            other.model(&reference),
            Err(HostRootError::WrongRuntime)
        ));
        services.retire();
        assert!(matches!(
            services.model(&reference),
            Err(HostRootError::Stale)
        ));
    }
}
