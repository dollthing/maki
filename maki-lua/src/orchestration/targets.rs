use arc_swap::ArcSwapOption;
use maki_agent::{
    AgentRef,
    agent::LoadedInstructions,
    cancel::CancelToken,
    tools::{Deadline, ToolContext, TurnToolBindings},
};
use maki_commands::CommandInvocation;
use maki_providers::Message;
use maki_storage::checkpoint::CheckpointPublication;
use maki_storage::sessions::HistorySnapshot;
use std::sync::{Arc, Mutex};

use crate::agent_errors::FIXED_MODEL_ERROR;

use super::{
    ConfigMutationReservation, ConfigMutationReserver, OrchestrationServices, TrustedTarget,
    TurnCompletion, TurnCompletionFuture, nonpersistent_config_mutation,
};

struct RootTarget {
    target: TrustedTarget,
    config_mutations: Option<ConfigMutationReserver>,
}

struct ChildTarget {
    target: TrustedTarget,
    history_key: Option<String>,
}

#[derive(Default)]
struct Targets {
    roots: Vec<RootTarget>,
    children: Vec<ChildTarget>,
    selected: Option<AgentRef>,
}

pub struct AgentOrchestration {
    targets: Mutex<Targets>,
    models: Arc<ArcSwapOption<Vec<String>>>,
    fixed_root: Option<AgentRef>,
    config_mutations: Option<ConfigMutationReserver>,
    completion: Option<TurnCompletion>,
}

impl AgentOrchestration {
    pub fn new(models: Arc<ArcSwapOption<Vec<String>>>) -> Self {
        Self {
            targets: Mutex::new(Targets::default()),
            models,
            fixed_root: None,
            config_mutations: None,
            completion: None,
        }
    }

    pub fn bind_root(models: Arc<ArcSwapOption<Vec<String>>>, root: TrustedTarget) -> Self {
        let target = root.target.clone();
        let services = Self::new(models);
        services.register_root(root);
        services.select(Some(target));
        services
    }

    pub fn bind_fixed_root(models: Arc<ArcSwapOption<Vec<String>>>, root: TrustedTarget) -> Self {
        let fixed = root.target.clone();
        let mut services = Self::bind_root(models, root);
        services.fixed_root = Some(fixed);
        services
    }

    pub fn with_config_mutations(mut self, reserve: ConfigMutationReserver) -> Self {
        self.config_mutations = Some(reserve);
        self
    }

    pub fn with_completion(mut self, completion: TurnCompletion) -> Self {
        self.completion = Some(completion);
        self
    }

    pub fn register_root(&self, target: TrustedTarget) {
        self.register_root_binding(target, None);
    }

    pub fn register_root_with_config_mutations(
        &self,
        target: TrustedTarget,
        reserve: ConfigMutationReserver,
    ) {
        self.register_root_binding(target, Some(reserve));
    }

    fn register_root_binding(
        &self,
        target: TrustedTarget,
        config_mutations: Option<ConfigMutationReserver>,
    ) {
        let mut targets = self
            .targets
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        targets.roots.retain(|entry| {
            entry.target.target.validate_live().is_ok()
                && !entry.target.target.same_manager(&target.target)
        });
        targets.roots.push(RootTarget {
            target,
            config_mutations,
        });
    }

    pub fn history_key(&self, target: &AgentRef) -> Result<String, String> {
        self.validate_target(target)?;
        let targets = self
            .targets
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        targets
            .children
            .iter()
            .find(|entry| {
                entry.target.target.same_manager(target) && entry.target.target.id() == target.id()
            })
            .and_then(|entry| entry.history_key.clone())
            .ok_or_else(|| "child agent history key is unavailable".into())
    }

    fn register_child_binding(
        &self,
        target: TrustedTarget,
        history_key: Option<String>,
    ) -> Result<(), String> {
        self.validate_target(&target.target)?;
        let mut targets = self
            .targets
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !targets.roots.iter().any(|root| {
            root.target.target.same_manager(&target.target)
                && root.target.target.validate_live().is_ok()
        }) {
            return Err("agent does not belong to this plugin host".into());
        }
        if targets.roots.iter().any(|root| {
            root.target.target.same_manager(&target.target)
                && root.target.target.id() == target.target.id()
        }) {
            return Err("root template registration is owned by the host".into());
        }
        targets.children.retain(|entry| {
            entry.target.target.validate_live().is_ok()
                && !(entry.target.target.same_manager(&target.target)
                    && entry.target.target.id() == target.target.id())
        });
        targets.children.push(ChildTarget {
            target,
            history_key,
        });
        Ok(())
    }

    pub fn select(&self, selected: Option<AgentRef>) {
        let mut targets = self
            .targets
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        targets.selected = selected;
        targets
            .roots
            .retain(|entry| entry.target.target.validate_live().is_ok());
        let roots = targets
            .roots
            .iter()
            .map(|entry| entry.target.target.clone())
            .collect::<Vec<_>>();
        targets.children.retain(|entry| {
            entry.target.target.validate_live().is_ok()
                && roots
                    .iter()
                    .any(|root| root.same_manager(&entry.target.target))
        });
    }
}

impl OrchestrationServices for AgentOrchestration {
    fn resolve_target(
        &self,
        invocation: Option<&CommandInvocation>,
    ) -> Result<Option<TrustedTarget>, String> {
        if let Some(invocation) = invocation {
            let Some(pinned) = invocation.admission_context::<TrustedTarget>() else {
                return Ok(None);
            };
            if pinned.target.validate_live().is_err() {
                return Ok(None);
            }
            self.validate_target(&pinned.target)?;
            return Ok(Some(pinned.clone()));
        }
        let target = {
            let targets = self
                .targets
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            targets.selected.clone()
        };
        target
            .map(|target| {
                self.template(&target)
                    .map(|template| TrustedTarget { target, template })
            })
            .transpose()
    }

    fn register_target(&self, target: TrustedTarget) -> Result<(), String> {
        self.register_child_binding(target, None)
    }

    fn register_child_target(
        &self,
        target: TrustedTarget,
        history_key: String,
    ) -> Result<(), String> {
        self.register_child_binding(target, Some(history_key))
    }

    fn template(&self, target: &AgentRef) -> Result<ToolContext, String> {
        target.validate_live().map_err(|error| error.to_string())?;
        let targets = self
            .targets
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let root = targets
            .roots
            .iter()
            .find(|entry| {
                entry.target.target.same_manager(target)
                    && entry.target.target.validate_live().is_ok()
            })
            .ok_or("agent does not belong to this plugin host")?;
        let source = if root.target.target.id() == target.id() {
            &root.target
        } else {
            targets
                .children
                .iter()
                .find(|entry| {
                    entry.target.target.same_manager(target)
                        && entry.target.target.id() == target.id()
                })
                .map(|entry| &entry.target)
                .ok_or("child agent template is unavailable")?
        };
        let mut template = source.template.clone();
        let config = target
            .effective_config()
            .map_err(|error| error.to_string())?
            .ok_or("agent configuration is unavailable")?;
        template.apply_effective_config(&config);
        template.cwd = config.context.cwd().to_path_buf();
        template.managed_turn = None;
        template.tool_use_id = None;
        template.live_sink = None;
        template.cancel = CancelToken::none();
        template.deadline = Deadline::None;
        template.loaded_instructions = LoadedInstructions::new();
        template.pending_output_limits = None;
        template.turn_bindings = Arc::new(TurnToolBindings::default());
        Ok(template)
    }

    fn validate_target(&self, target: &AgentRef) -> Result<(), String> {
        target.validate_live().map_err(|error| error.to_string())?;
        let targets = self
            .targets
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if targets.roots.iter().any(|entry| {
            entry.target.target.same_manager(target) && entry.target.target.validate_live().is_ok()
        }) {
            Ok(())
        } else {
            Err("agent does not belong to this plugin host".into())
        }
    }

    fn validate_config_mutation(&self, target: &AgentRef) -> Result<(), String> {
        self.validate_target(target)?;
        if self
            .fixed_root
            .as_ref()
            .is_some_and(|root| root.same_manager(target) && root.id() == target.id())
        {
            return Err(FIXED_MODEL_ERROR.into());
        }
        Ok(())
    }

    fn reserve_config_mutation(
        &self,
        target: &AgentRef,
        publication: CheckpointPublication,
    ) -> Result<ConfigMutationReservation, String> {
        self.validate_target(target)?;
        let reserve = {
            let targets = self
                .targets
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            targets
                .roots
                .iter()
                .find(|root| root.target.target.same_manager(target))
                .and_then(|root| root.config_mutations.clone())
        }
        .or_else(|| self.config_mutations.clone());
        if let Some(reserve) = reserve {
            return reserve(target, publication);
        }
        if target
            .snapshot()
            .map_err(|error| error.to_string())?
            .parent_id
            .is_none()
        {
            return Err("durable root configuration adapter is unavailable".into());
        }
        Ok(nonpersistent_config_mutation(publication))
    }

    fn complete_turn(
        &self,
        target: &AgentRef,
        history: HistorySnapshot<Message>,
    ) -> TurnCompletionFuture {
        if let Err(error) = self.validate_target(target) {
            return Box::pin(async move { Err(error) });
        }
        if let Some(completion) = &self.completion {
            return completion(target, history);
        }
        Box::pin(async { Ok(()) })
    }

    fn available_models(&self, target: &AgentRef) -> Result<Vec<String>, String> {
        let template = self.template(target)?;
        let models = self
            .models
            .load_full()
            .ok_or("host model catalog is unavailable")?;
        Ok(models
            .iter()
            .filter(|spec| template.model_policy.allows(spec))
            .cloned()
            .collect())
    }
}
