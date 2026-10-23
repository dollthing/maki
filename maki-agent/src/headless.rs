use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::Duration;

use async_lock::Mutex;
use event_listener::Event;
use flume::Receiver;
use futures_lite::future;
use maki_config::{ModelPolicy, ProjectConfig, SessionDefaults};
use maki_providers::Message;
use maki_providers::model::Model;
use maki_providers::provider::{self, BoxFuture, Provider};
use maki_providers::{RequestOptions, Timeouts};
use maki_storage::id::{MakiId, SessionRef};
use serde_json::Value;
use tracing::warn;

use crate::actor::{AdmissionPreparation, AgentContextSnapshot, PreparedModel};
use crate::agent::isolated_turn::IsolatedTurnEvent;
use crate::agent::{self, History, ResolvedPromptInputs, TurnPromptInputs};
use crate::cancel::{CancelMap, CancelToken};
use crate::permissions::{PermissionManager, PluginRuleStore};
use crate::prompt::ResolvedSlots;
use crate::session_coordinator::{ModelPreparer, SessionCoordinatorHandle};
use crate::template;
use crate::tools::offload::{OffloadCleanup, OffloadStore};
use crate::tools::{
    Deadline, FileReadTracker, LocalTools, RequestTools, ToolAudience, ToolContext, ToolRegistry,
    TurnToolBindings,
};
use crate::{
    ActorBackend, Agent, AgentConfig, AgentEvent, AgentInput, AgentLimits, AgentManagerHandle,
    AgentMode, AgentParams, AgentRef, AgentRunParams, BackendResult, ControlWork,
    EffectiveAgentConfig, Envelope, EventSender, McpHandle, McpSession, PermissionsConfig,
    RunLedger, RunSettings, SessionMailbox, ToolOutputLines, TurnContext, TurnFailure,
    TurnFailureKind, TurnId, TurnTicket, WorkKind,
};

const GRAPH_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const INITIALIZATION_CLOSED: &str = "session ended before initialization";
const TERMINAL_CHECKPOINT_TIMEOUT: &str = "checkpoint timed out";
const OPERATION_CANCELLED: &str = "operation cancelled";
#[cfg(any(test, feature = "test-util"))]
const SESSION_ENDED: &str = "session ended";
const HEADLESS_CONFIG_UNAVAILABLE: &str = "managed headless configuration unavailable";
const COMPACTION_CANCELLED: &str = "compaction cancelled";

#[derive(Clone, Default)]
pub struct RootReadiness(Arc<RootInitialization>);

#[derive(Default)]
struct RootInitialization {
    result: OnceLock<Result<AgentRef, String>>,
    changed: Event,
}

impl RootReadiness {
    pub async fn get(&self) -> Result<AgentRef, String> {
        loop {
            let listener = self.0.changed.listen();
            if let Some(result) = self.0.result.get() {
                return result.clone();
            }
            listener.await;
        }
    }

    fn finish(&self, result: Result<AgentRef, String>) {
        let _ = self.0.result.set(result);
        self.0.changed.notify(usize::MAX);
    }
}

/// Called after provider preparation and root creation, before admissions can run.
pub type RootBinding = Box<
    dyn FnOnce(AgentManagerHandle, AgentRef, ToolContext) -> BoxFuture<'static, Result<(), String>>
        + Send,
>;

pub struct InteractiveParams {
    pub model: Model,
    pub config: AgentConfig,
    pub permissions_config: PermissionsConfig,
    pub timeouts: Timeouts,
    pub prompt_slots: Arc<ResolvedSlots>,
    pub excluded_tools: Vec<&'static str>,
    pub mcp_handle: Option<McpHandle>,
    pub initial_wd: PathBuf,
    pub session_id: Option<SessionRef>,
    pub initial_history: Vec<Message>,
    pub yolo: bool,
    pub system_prompt_override: Option<String>,
    pub append_system_prompt: Option<String>,
    pub defaults: SessionDefaults,
    pub model_policy: Arc<ModelPolicy>,
    pub modes: Arc<crate::ModeRegistry>,
    pub question_mode: crate::tools::QuestionMode,
    pub plugin_rules: Arc<PluginRuleStore>,
    pub project_config: ProjectConfig,
    pub local_tools: LocalTools,
    pub state_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManualCompactionEvent {
    Started,
    Completed,
    Cancelled,
    Failed(String),
}

pub enum InteractiveControl {
    Compact(flume::Sender<Result<(), String>>),
    ManualCompaction {
        instructions: Option<String>,
        output: flume::Sender<ManualCompactionEvent>,
        cancel: CancelToken,
    },
    Reset(flume::Sender<Result<(), String>>),
    IsolatedTurn {
        question: String,
        images: Vec<maki_providers::ImageSource>,
        output: flume::Sender<agent::isolated_turn::IsolatedTurnEvent>,
        cancel: CancelToken,
    },
}

#[derive(Clone)]
pub struct InteractiveControlSender {
    root: RootReadiness,
    next: Arc<AtomicU64>,
    #[cfg(any(test, feature = "test-util"))]
    channel: Option<flume::Sender<InteractiveControl>>,
}

#[cfg(any(test, feature = "test-util"))]
impl From<flume::Sender<InteractiveControl>> for InteractiveControlSender {
    fn from(sender: flume::Sender<InteractiveControl>) -> Self {
        Self {
            root: RootReadiness::default(),
            next: Arc::default(),
            channel: Some(sender),
        }
    }
}

/// A control the actor owns until it runs. Dropping it unrun, because it was
/// cancelled or the actor closed, reports the cancellation to its caller; once
/// the actor drops its work, the task forwarding the caller's cancel ends.
struct PendingControl {
    control: Option<InteractiveControl>,
    _done: flume::Sender<()>,
}

impl PendingControl {
    fn cancel(&mut self) {
        match self.control.take() {
            Some(InteractiveControl::Compact(reply) | InteractiveControl::Reset(reply)) => {
                let _ = reply.send(Err(OPERATION_CANCELLED.into()));
            }
            Some(InteractiveControl::ManualCompaction { output, .. }) => {
                let _ = output.send(ManualCompactionEvent::Cancelled);
            }
            Some(InteractiveControl::IsolatedTurn { output, .. }) => {
                let _ = output.send(agent::isolated_turn::IsolatedTurnEvent::Cancelled);
            }
            None => {}
        }
    }
}

impl Drop for PendingControl {
    fn drop(&mut self) {
        self.cancel();
    }
}

impl InteractiveControlSender {
    pub async fn send(&self, control: InteractiveControl) -> Result<(), String> {
        #[cfg(any(test, feature = "test-util"))]
        if let Some(channel) = &self.channel {
            return channel
                .send_async(control)
                .await
                .map_err(|_| SESSION_ENDED.to_owned());
        }
        let actor = self
            .root
            .get()
            .await?
            .actor()
            .map_err(|error| error.to_string())?;
        let key = format!(
            "headless-control:{}",
            self.next.fetch_add(1, Ordering::Relaxed)
        );
        let cancel = match &control {
            InteractiveControl::ManualCompaction { cancel, .. }
            | InteractiveControl::IsolatedTurn { cancel, .. } => Some(cancel.clone()),
            _ => None,
        };
        let (done, finished) = flume::bounded(0);
        let pending = Arc::new(StdMutex::new(PendingControl {
            control: Some(control),
            _done: done,
        }));
        let queued = Arc::downgrade(&pending);
        actor
            .push_control(key.clone(), key.clone(), Some(pending))
            .map_err(|error| error.to_string())?;
        if let Some(cancel) = cancel {
            smol::spawn(async move {
                future::or(
                    async {
                        cancel.cancelled().await;
                        actor.cancel_correlation(&key, crate::TurnCancellationReason::User);
                        if let Some(pending) = queued.upgrade() {
                            pending
                                .lock()
                                .unwrap_or_else(|error| error.into_inner())
                                .cancel();
                        }
                    },
                    async {
                        let _ = finished.recv_async().await;
                    },
                )
                .await;
            })
            .detach();
        }
        Ok(())
    }

    /// Compacts the root's history and waits for it to settle.
    pub async fn compact(
        &self,
        instructions: Option<String>,
        cancel: CancelToken,
    ) -> Result<(), String> {
        let (output, events) = flume::unbounded();
        self.send(InteractiveControl::ManualCompaction {
            instructions,
            output,
            cancel,
        })
        .await?;
        loop {
            match events.recv_async().await {
                Ok(ManualCompactionEvent::Started) => {}
                Ok(event) => return compaction_result(event),
                Err(_) => return Err(OPERATION_CANCELLED.into()),
            }
        }
    }

    /// Asks {question} beside the root's history, leaving it unchanged, and
    /// returns the answer.
    pub async fn ask_isolated(
        &self,
        question: String,
        images: Vec<maki_providers::ImageSource>,
        cancel: CancelToken,
    ) -> Result<String, String> {
        let (output, events) = flume::unbounded();
        self.send(InteractiveControl::IsolatedTurn {
            question,
            images,
            output,
            cancel,
        })
        .await?;
        let mut answer = String::new();
        loop {
            match events.recv_async().await {
                Ok(IsolatedTurnEvent::TextDelta(text)) => answer.push_str(&text),
                Ok(IsolatedTurnEvent::ThinkingDelta(_)) => {}
                Ok(IsolatedTurnEvent::Done) => return Ok(answer),
                Ok(IsolatedTurnEvent::Error(error)) => return Err(error),
                Ok(IsolatedTurnEvent::Cancelled) | Err(_) => {
                    return Err(OPERATION_CANCELLED.into());
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct InteractiveInputSender {
    root: RootReadiness,
    events: flume::Sender<Envelope>,
    next: Arc<AtomicU64>,
    _lifetime: flume::Sender<()>,
    #[cfg(any(test, feature = "test-util"))]
    mirror: Option<flume::Sender<AgentInput>>,
}

impl InteractiveInputSender {
    /// Diverts sent input to {mirror} instead of admitting it.
    #[cfg(any(test, feature = "test-util"))]
    pub fn with_input_mirror(mut self, mirror: flume::Sender<AgentInput>) -> Self {
        self.mirror = Some(mirror);
        self
    }

    pub async fn send(&self, input: AgentInput) -> Result<(), String> {
        #[cfg(any(test, feature = "test-util"))]
        if let Some(mirror) = &self.mirror {
            return mirror
                .send_async(input)
                .await
                .map_err(|_| SESSION_ENDED.to_owned());
        }
        self.admit(input).await.map(|_| ())
    }

    pub async fn admit(&self, input: AgentInput) -> Result<TurnTicket, String> {
        let root = self.root.get().await?;
        let actor = root.actor().map_err(|error| error.to_string())?;
        let run = self.next.fetch_add(1, Ordering::Relaxed);
        let cancel = input.cancel.clone();
        let ticket = actor
            .admit_turn(
                input,
                Some(EventSender::new(self.events.clone())),
                format!("run:{run}"),
            )
            .map_err(|error| error.to_string())?;
        if let Some(cancel) = cancel {
            let ticket = ticket.clone();
            smol::spawn(async move {
                future::or(
                    async {
                        cancel.cancelled().await;
                        let _ = actor.cancel_turn(ticket.turn_id());
                    },
                    async {
                        ticket.wait().await;
                    },
                )
                .await;
            })
            .detach();
        }
        Ok(ticket)
    }
}

pub struct InteractiveHandle {
    pub manager: AgentManagerHandle,
    pub root: RootReadiness,
    pub event_rx: Receiver<Envelope>,
    pub tool_names: Vec<String>,
    pub input_tx: InteractiveInputSender,
    pub answer_tx: flume::Sender<String>,
    pub cancel_tx: flume::Sender<()>,
    pub control_tx: InteractiveControlSender,
    pub model: crate::SharedModel,
    pub session_id: SessionRef,
    pub mailbox: SessionMailbox,
    pub permissions: Arc<PermissionManager>,
    pub offload_cleanup: Option<OffloadCleanup>,
    pub task: smol::Task<()>,
}

impl InteractiveHandle {
    pub async fn initialized_root(&self) -> Result<AgentRef, String> {
        self.root.get().await
    }
}

pub type ProviderPreparation = Box<
    dyn FnOnce(
            Model,
            Timeouts,
        ) -> BoxFuture<'static, Result<(Model, Arc<dyn Provider>), crate::AgentError>>
        + Send,
>;

pub fn default_preparation() -> ProviderPreparation {
    Box::new(|mut model, timeouts| {
        Box::pin(async move {
            let provider = provider::from_model_async(&mut model, timeouts).await?;
            Ok((model, Arc::from(provider)))
        })
    })
}

/// Prepares the provider for a model a session coordinator commits.
pub fn default_model_preparer(timeouts: Timeouts) -> ModelPreparer {
    Arc::new(move |mut model| {
        Box::pin(async move {
            let provider = provider::from_model_async(&mut model, timeouts)
                .await
                .map_err(|error| Arc::from(error.user_message()))?;
            Ok(PreparedModel {
                model,
                provider: Arc::from(provider),
            })
        })
    })
}

struct HeadlessBackend {
    params: AgentParams,
    excluded_tools: Vec<&'static str>,
    system_override: Option<String>,
    append_system: Option<String>,
    local_tools: LocalTools,
    mcp: Option<McpSession>,
    answer_rx: Arc<Mutex<Receiver<String>>>,
    raw_tx: flume::Sender<Envelope>,
    root: RootReadiness,
}

impl HeadlessBackend {
    fn event_sender(&self, context: &TurnContext) -> EventSender {
        EventSender::with_identity(self.raw_tx.clone(), Some(context.work))
    }

    async fn config(&self, context: &TurnContext) -> Result<Arc<EffectiveAgentConfig>, String> {
        if let Some(config) = &context.policy {
            return Ok(Arc::clone(config));
        }
        self.root
            .get()
            .await?
            .effective_config()
            .map_err(|error| error.to_string())?
            .ok_or_else(|| HEADLESS_CONFIG_UNAVAILABLE.to_owned())
    }

    fn system(&self, config: &EffectiveAgentConfig, instructions: &agent::Instructions) -> String {
        let vars = template::env_vars_for(config.context.cwd());
        let definition = config
            .mode_def
            .clone()
            .unwrap_or_else(|| self.params.modes.current(&config.mode));
        let mut system = self.system_override.clone().unwrap_or_else(|| {
            agent::build_system_prompt_with_def(
                &vars,
                &definition,
                &config.mode,
                &instructions.text,
                &self.params.prompt_slots,
            )
        });
        if let Some(append) = &self.append_system {
            system.push('\n');
            system.push_str(append);
        }
        system
    }

    fn tools(&self, config: &EffectiveAgentConfig) -> RequestTools {
        RequestTools::build(
            &self.params.registry,
            &template::env_vars_for(config.context.cwd()),
            &config.model,
            &self.params.config,
            &self.excluded_tools,
            config.workflow,
            self.mcp.is_some(),
        )
    }

    fn template(&self, config: &EffectiveAgentConfig) -> ToolContext {
        let tools = self.tools(config);
        ToolContext {
            execution_agent_id: Some(self.params.agent_id),
            provider: Arc::clone(&config.provider),
            model: Arc::new(config.model.clone()),
            event_tx: EventSender::new(self.raw_tx.clone()),
            mode: config.mode.clone(),
            mode_def: config.mode_def.clone().map(Arc::new),
            question_mode: self.params.question_mode,
            session_id: self.params.session_id.clone(),
            cwd: config.context.cwd().to_path_buf(),
            tool_use_id: None,
            user_response_rx: Some(Arc::clone(&self.answer_rx)),
            loaded_instructions: Default::default(),
            cancel: CancelToken::none(),
            mcp: self.mcp.clone(),
            deadline: Deadline::None,
            config: self.params.config.clone(),
            tool_filter: Arc::clone(tools.filter()),
            request_tools: Some(tools),
            tool_output_lines: self.params.tool_output_lines,
            permissions: Arc::clone(&self.params.permissions),
            timeouts: self.params.timeouts,
            file_tracker: Arc::clone(&self.params.file_tracker),
            prompt_slots: Arc::clone(&self.params.prompt_slots),
            modes: Arc::clone(&self.params.modes),
            opts: RequestOptions {
                thinking: config.thinking,
                fast: config.fast,
            },
            subagent_cancels: Arc::clone(&self.params.subagent_cancels),
            ledger: Arc::new(RunLedger::default()),
            registry: Arc::clone(&self.params.registry),
            workflow: config.workflow,
            audience: self.params.audience,
            local_tools: Arc::clone(&self.local_tools),
            local_tool_capture: None,
            turn_bindings: Arc::new(TurnToolBindings::capture(
                &self.params.registry,
                &self.local_tools,
                self.mcp.as_ref(),
            )),
            live_sink: None,
            model_policy: Arc::clone(&self.params.model_policy),
            file_write_locks: Arc::clone(&self.params.file_write_locks),
            offload: self.params.offload.clone(),
            pending_output_limits: None,
            write_lock_chain: Arc::new(Vec::new()),
            managed_turn: None,
        }
    }

    async fn compact(
        &self,
        history: &mut History,
        context: &TurnContext,
        instructions: Option<&str>,
        cancel: &CancelToken,
    ) -> ManualCompactionEvent {
        let config = match self.config(context).await {
            Ok(config) => config,
            Err(error) => return ManualCompactionEvent::Failed(error),
        };
        let previous = history.snapshot().clone();
        let result = agent::compact(
            &*config.provider,
            &config.model,
            history,
            &self.event_sender(context),
            cancel,
            &self.params.config,
            instructions,
            self.params.session_id.as_ref(),
        )
        .await
        .map_err(|error| error.to_string());
        let snapshot = history.snapshot().clone();
        settle_manual_compaction(result, history, previous, cancel, |_| {
            persist_history(self.params.session_id.as_ref().unwrap().id(), snapshot)
        })
        .await
    }
}

impl ActorBackend for HeadlessBackend {
    fn offload_store(&self) -> Option<Arc<OffloadStore>> {
        self.params.offload.clone()
    }

    fn admission_preparation(&self) -> Option<AdmissionPreparation> {
        let params = self.params.clone();
        let local_tools = Arc::clone(&self.local_tools);
        let mcp = self.mcp.clone();
        Some(Arc::new(move |_, mode, config| {
            agent::TurnAdmissionSnapshot {
                mode_def: config
                    .and_then(|config| config.mode_def.clone())
                    .or_else(|| match mode {
                        AgentMode::Custom(id) => params.modes.get(id),
                        _ => Some(params.modes.current(mode)),
                    })
                    .map(Arc::new),
                prompt_inputs: config.map(|config| {
                    let cwd = config.context.cwd().to_path_buf();
                    Arc::new(TurnPromptInputs {
                        instructions: agent::load_instructions(&cwd.to_string_lossy()),
                        cwd,
                        mcp_prompt: None,
                        resolved: None,
                        ready: Some(ResolvedPromptInputs {
                            slots: Arc::clone(&params.prompt_slots),
                            slots_error: None,
                            mcp_messages: None,
                        }),
                    })
                }),
                bindings: Arc::new(TurnToolBindings::capture(
                    &params.registry,
                    &local_tools,
                    mcp.as_ref(),
                )),
                mcp_startup_notice: mcp.as_ref().and_then(|mcp| {
                    let count = mcp.connecting_count();
                    (count > 0).then_some(count)
                }),
            }
        }))
    }

    fn run_turn<'a>(
        &'a mut self,
        history: &'a mut History,
        context: TurnContext,
        mut input: AgentInput,
        _work: WorkKind,
    ) -> BoxFuture<'a, BackendResult> {
        Box::pin(async move {
            let turn_id = context.turn_id.unwrap_or_else(TurnId::generate);
            let config = match self.config(&context).await {
                Ok(config) => config,
                Err(error) => {
                    warn!(%turn_id, %error, "headless turn setup failed");
                    return BackendResult::SetupFailed(Some(TurnFailure {
                        kind: TurnFailureKind::Internal,
                        diagnostic: error.clone(),
                        user_message: error,
                        retryable: true,
                    }));
                }
            };
            let instructions = context
                .admission
                .as_ref()
                .and_then(|admission| admission.prompt_inputs.as_ref())
                .map(|inputs| inputs.instructions.clone())
                .unwrap_or_else(|| {
                    agent::load_instructions(&config.context.cwd().to_string_lossy())
                });
            let event_tx = self.event_sender(&context);
            context.output.set_terminal_sender(event_tx.clone());
            while self.answer_rx.lock().await.try_recv().is_ok() {}
            let mut agent = Agent::for_actor_turn(
                self.params.clone(),
                AgentRunParams {
                    history,
                    system: self.system(&config, &instructions),
                    event_tx: event_tx.clone(),
                    tools: self.tools(&config),
                },
                &context,
                &mut input,
                instructions.loaded,
            )
            .with_user_response_rx(Arc::clone(&self.answer_rx))
            .with_local_tools(Arc::clone(&self.local_tools))
            .with_mcp(self.mcp.clone());
            let outcome = agent.run(turn_id, input).await;
            drop(agent);
            let persisted = future::or(
                persist_history(
                    self.params.session_id.as_ref().unwrap().id(),
                    history.snapshot().clone(),
                ),
                async {
                    smol::Timer::after(self.params.timeouts.low_speed).await;
                    Err(TERMINAL_CHECKPOINT_TIMEOUT.to_owned())
                },
            )
            .await;
            BackendResult::EnteredRun(match persisted {
                Ok(()) => outcome,
                Err(error) => outcome.checkpoint_failed(error),
            })
        })
    }

    fn run_control<'a>(
        &'a mut self,
        history: &'a mut History,
        context: TurnContext,
        control: &'a ControlWork,
    ) -> BoxFuture<'a, BackendResult> {
        Box::pin(async move {
            let pending = control
                .payload
                .clone()
                .and_then(|payload| payload.downcast::<StdMutex<PendingControl>>().ok());
            let control = pending.as_ref().and_then(|pending| {
                pending
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .control
                    .take()
            });
            match control {
                Some(InteractiveControl::Compact(reply)) => {
                    let result = self.compact(history, &context, None, &context.cancel).await;
                    let _ = reply.send(compaction_result(result));
                }
                Some(InteractiveControl::ManualCompaction {
                    instructions,
                    output,
                    cancel,
                }) => {
                    let _ = output.send(ManualCompactionEvent::Started);
                    let (trigger, combined) = CancelToken::new();
                    let cancellation = smol::spawn({
                        let actor_cancel = context.cancel.clone();
                        async move {
                            future::or(cancel.cancelled(), actor_cancel.cancelled()).await;
                            trigger.cancel();
                        }
                    });
                    let result = self
                        .compact(history, &context, instructions.as_deref(), &combined)
                        .await;
                    cancellation.cancel().await;
                    let _ = output.send(result);
                }
                Some(InteractiveControl::Reset(reply)) => {
                    let previous = history.snapshot().clone();
                    history.replace(Vec::new());
                    let result = persist_history(
                        self.params.session_id.as_ref().unwrap().id(),
                        history.snapshot().clone(),
                    )
                    .await;
                    if result.is_err() {
                        history.replace_snapshot(previous);
                    }
                    let _ = reply.send(result);
                }
                Some(InteractiveControl::IsolatedTurn {
                    question,
                    images,
                    output,
                    cancel,
                }) => {
                    let config = match self.config(&context).await {
                        Ok(config) => config,
                        Err(error) => {
                            let _ =
                                output.send(agent::isolated_turn::IsolatedTurnEvent::Error(error));
                            return BackendResult::ControlFailed;
                        }
                    };
                    let instructions =
                        agent::load_instructions(&config.context.cwd().to_string_lossy());
                    let (trigger, combined) = CancelToken::new();
                    let cancellation = smol::spawn({
                        let actor_cancel = context.cancel.clone();
                        async move {
                            future::or(cancel.cancelled(), actor_cancel.cancelled()).await;
                            trigger.cancel();
                        }
                    });
                    agent::isolated_turn::run_isolated_turn(
                        agent::isolated_turn::IsolatedTurnRequest {
                            provider: Arc::clone(&config.provider),
                            model: config.model.clone(),
                            history: history.as_slice().to_vec(),
                            system: self.system(&config, &instructions),
                            question,
                            images,
                            session_id: self.params.session_id.clone(),
                            cancel: combined,
                        },
                        output,
                    )
                    .await;
                    cancellation.cancel().await;
                }
                None => return BackendResult::ControlFailed,
            }
            BackendResult::ControlDone
        })
    }

    fn run_compact<'a>(
        &'a mut self,
        history: &'a mut History,
        context: TurnContext,
        instructions: Option<&'a str>,
    ) -> BoxFuture<'a, BackendResult> {
        Box::pin(async move {
            let result = self
                .compact(history, &context, instructions, &context.cancel)
                .await;
            match compaction_result(result) {
                Ok(()) => BackendResult::CompactDone,
                Err(message) => {
                    let _ = self
                        .event_sender(&context)
                        .send(AgentEvent::ControlError { message });
                    BackendResult::ControlFailed
                }
            }
        })
    }
}

fn compaction_result(event: ManualCompactionEvent) -> Result<(), String> {
    match event {
        ManualCompactionEvent::Completed => Ok(()),
        ManualCompactionEvent::Failed(error) => Err(error),
        ManualCompactionEvent::Cancelled => Err(COMPACTION_CANCELLED.into()),
        ManualCompactionEvent::Started => unreachable!(),
    }
}

async fn settle_manual_compaction<P, F>(
    compacted: Result<(), String>,
    history: &mut History,
    previous: crate::agent::HistorySnapshot,
    cancel: &CancelToken,
    persist: P,
) -> ManualCompactionEvent
where
    P: FnOnce(Vec<Message>) -> F,
    F: Future<Output = Result<(), String>>,
{
    let result = match compacted {
        Ok(()) => persist(history.as_slice().to_vec()).await,
        Err(error) => Err(error),
    };
    match result {
        Ok(()) => ManualCompactionEvent::Completed,
        Err(error) => {
            history.replace_snapshot(previous);
            if cancel.is_cancelled() {
                ManualCompactionEvent::Cancelled
            } else {
                ManualCompactionEvent::Failed(error)
            }
        }
    }
}

async fn persist_history(
    session_id: MakiId,
    history: crate::agent::HistorySnapshot,
) -> Result<(), String> {
    SessionCoordinatorHandle::resolve(session_id)
        .map_err(|error| error.to_string())?
        .persist_history(history)
        .await
        .map_err(|error| error.to_string())
}

struct ManagedLifetime {
    manager: AgentManagerHandle,
    root: RootReadiness,
    cleanup: Option<OffloadCleanup>,
    children: Arc<CancelMap<String>>,
}

impl Drop for ManagedLifetime {
    fn drop(&mut self) {
        self.root.finish(Err(INITIALIZATION_CLOSED.into()));
        self.children.cancel_all();
        if let Some(cleanup) = &self.cleanup {
            cleanup.request();
        }
        let manager = self.manager.clone();
        smol::spawn(async move {
            manager.shutdown(GRAPH_SHUTDOWN_TIMEOUT).await;
        })
        .detach();
    }
}

#[cfg(test)]
struct RemoveOffloadOnDrop(OffloadCleanup);

#[cfg(test)]
impl Drop for RemoveOffloadOnDrop {
    fn drop(&mut self) {
        self.0.request();
    }
}

async fn drain_offload(cleanup: Option<&OffloadCleanup>) {
    if let Some(cleanup) = cleanup {
        cleanup.request();
        let _ = cleanup.wait().await;
    }
}

fn session_offload(state_dir: Option<&Path>, session: &SessionRef) -> Option<Arc<OffloadStore>> {
    state_dir.map(|root| Arc::new(OffloadStore::for_session(root, session)))
}

pub fn spawn_interactive_with_binding(
    params: InteractiveParams,
    prepare: ProviderPreparation,
    history: Option<crate::agent::HistorySnapshot>,
    binding: Option<RootBinding>,
) -> InteractiveHandle {
    let session_ref = params
        .session_id
        .clone()
        .unwrap_or_else(|| SessionRef::from(MakiId::generate()));
    let mailbox = SessionMailbox::new(session_ref.id());
    let manager = AgentManagerHandle::new(AgentLimits {
        max_concurrent_agent_turns: params.config.max_concurrent_agent_turns.max(1),
        ..AgentLimits::default()
    })
    .expect("headless agent limits are valid");
    let root = RootReadiness::default();
    let model = crate::SharedModel::default();
    let (guard, events) = crate::types::event_stream();
    let raw_tx = guard.tx().clone();
    let (lifetime_tx, lifetime_rx) = flume::unbounded();
    let input_tx = InteractiveInputSender {
        root: root.clone(),
        events: raw_tx.clone(),
        next: Arc::new(AtomicU64::new(0)),
        _lifetime: lifetime_tx,
        #[cfg(any(test, feature = "test-util"))]
        mirror: None,
    };
    let control_tx = InteractiveControlSender {
        root: root.clone(),
        next: Arc::new(AtomicU64::new(0)),
        #[cfg(any(test, feature = "test-util"))]
        channel: None,
    };
    let (answer_tx, answer_rx) = flume::unbounded();
    let (cancel_tx, cancel_rx) = flume::bounded(1);
    let mut permission_config = params.permissions_config.clone();
    permission_config.yolo |= params.yolo;
    let permissions = Arc::new(PermissionManager::new(
        permission_config,
        params.initial_wd.clone(),
        params.project_config.clone(),
        Arc::clone(&params.plugin_rules),
    ));
    let offload = session_offload(params.state_dir.as_deref(), &session_ref);
    let cleanup = offload
        .as_ref()
        .map(|store| OffloadCleanup::new(Arc::clone(store)));
    let children = Arc::new(CancelMap::new());
    let lifetime = ManagedLifetime {
        manager: manager.clone(),
        root: root.clone(),
        cleanup: cleanup.clone(),
        children: Arc::clone(&children),
    };
    let tools = RequestTools::build(
        ToolRegistry::global(),
        &template::env_vars_for(&params.initial_wd),
        &params.model,
        &params.config,
        &params.excluded_tools,
        params.defaults.workflow,
        params.mcp_handle.is_some(),
    );
    let mcp = params
        .mcp_handle
        .clone()
        .map(|handle| McpSession::new(handle, &params.initial_history));
    let tool_names = advertised_tool_names(tools.definitions(), mcp.as_ref());
    let offload_cleanup = cleanup.clone();
    let task = smol::spawn({
        let manager = manager.clone();
        let root = root.clone();
        let model_source = model.clone();
        let permissions = Arc::clone(&permissions);
        let mailbox = mailbox.clone();
        let session_ref = session_ref.clone();
        async move {
            let _lifetime = lifetime;
            let initialized = async {
                let (model, provider) = prepare(params.model.clone(), params.timeouts)
                    .await
                    .map_err(|error| error.user_message())?;
                model_source.install(Arc::clone(&provider), model.clone());
                let config = EffectiveAgentConfig::new(
                    RunSettings {
                        provider: Arc::clone(&provider),
                        model: model.clone(),
                        fast: params.defaults.fast,
                        workflow: params.defaults.workflow,
                        thinking: params.defaults.thinking.into(),
                    },
                    AgentMode::Build,
                )
                .with_context(AgentContextSnapshot::new(params.initial_wd.clone()))
                .with_mode_def(Some(params.modes.current(&AgentMode::Build)));
                let backend = HeadlessBackend {
                    params: AgentParams {
                        agent_id: crate::AgentId::generate(),
                        provider,
                        model,
                        config: params.config,
                        tool_output_lines: ToolOutputLines::default(),
                        permissions,
                        session_id: Some(session_ref.clone()),
                        mailbox: Some(mailbox),
                        timeouts: params.timeouts,
                        file_tracker: FileReadTracker::fresh(),
                        prompt_slots: params.prompt_slots,
                        modes: params.modes,
                        subagent_cancels: children,
                        registry: Arc::clone(ToolRegistry::global_arc()),
                        audience: ToolAudience::MAIN,
                        question_mode: params.question_mode,
                        model_policy: params.model_policy,
                        file_write_locks: Arc::new(crate::tools::FileWriteLocks::new()),
                        offload,
                        managed_turn: None,
                    },
                    excluded_tools: params.excluded_tools,
                    system_override: params.system_prompt_override,
                    append_system: params.append_system_prompt,
                    local_tools: params.local_tools,
                    mcp,
                    answer_rx: Arc::new(Mutex::new(answer_rx)),
                    raw_tx: raw_tx.clone(),
                    root: root.clone(),
                };
                let mut template = backend.template(&config);
                let mirror =
                    history.map(|snapshot| Arc::new(arc_swap::ArcSwap::from_pointee(snapshot)));
                let initial_history = mirror
                    .as_ref()
                    .map(|mirror| mirror.load().messages.as_ref().clone())
                    .unwrap_or(params.initial_history);
                let (start, started) = flume::bounded(1);
                let reference = manager
                    .create_root_deferred_with_config(
                        Some(config),
                        initial_history,
                        mirror,
                        |agent_id| {
                            let mut backend = backend;
                            backend.params.agent_id = agent_id;
                            Ok::<Box<dyn ActorBackend>, String>(Box::new(backend))
                        },
                        started,
                    )
                    .map_err(|error| error.to_string())?;
                template.execution_agent_id = Some(reference.id());
                if let Some(binding) = binding {
                    binding(manager.clone(), reference.clone(), template).await?;
                }
                root.finish(Ok(reference.clone()));
                start
                    .send(())
                    .map_err(|_| INITIALIZATION_CLOSED.to_owned())?;
                Ok::<_, String>(reference)
            }
            .await;
            match initialized {
                Ok(reference) => {
                    let cancellation = async {
                        while cancel_rx.recv_async().await.is_ok() {
                            let _ = reference.cancel();
                        }
                    };
                    future::or(
                        async {
                            let _ = lifetime_rx.recv_async().await;
                        },
                        cancellation,
                    )
                    .await;
                }
                Err(message) => {
                    root.finish(Err(message.clone()));
                    let _ = EventSender::new(raw_tx).send(AgentEvent::ControlError { message });
                }
            }
            manager.shutdown(GRAPH_SHUTDOWN_TIMEOUT).await;
            if let Some(handle) = params.mcp_handle {
                handle.shutdown().await;
            }
            drain_offload(cleanup.as_ref()).await;
            drop(guard);
        }
    });
    InteractiveHandle {
        manager,
        root,
        event_rx: events.into_receiver(),
        tool_names,
        input_tx,
        answer_tx,
        cancel_tx,
        control_tx,
        model,
        session_id: session_ref,
        mailbox,
        permissions,
        offload_cleanup,
        task,
    }
}

fn advertised_tool_names(tools: &Value, mcp: Option<&McpSession>) -> Vec<String> {
    let mut probe = tools.clone();
    if let Some(mcp) = mcp {
        mcp.extend_tools(&mut probe);
    }
    extract_tool_names(&probe)
}

fn extract_tool_names(tools: &Value) -> Vec<String> {
    tools
        .as_array()
        .map(|array| {
            array
                .iter()
                .filter_map(|tool| tool.get("name").and_then(Value::as_str).map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::actor::{ConfigChange, ConfigPatch, PreparedModel};
    use crate::session_coordinator::{SessionCoordinatorParams, builtin_option_definitions};
    use crate::tools::offload::{OffloadBackend, OffloadError};
    use crate::{ModeRegistry, TurnOutcome};
    use maki_providers::{ContentBlock, Role, StopReason, StreamResponse, TokenUsage};
    use maki_storage::checkpoint::{
        CheckpointAck, CheckpointError, CheckpointFuture, CheckpointRequest, CheckpointWriter,
        accept_all,
    };
    use maki_storage::sessions::{SESSIONS_DIR, offload_dir};
    use std::io;
    use test_case::test_case;

    const GATE_TIMEOUT: Duration = Duration::from_secs(10);
    const HEADLESS_PROVIDER_ERROR: &str = "headless provider failed";
    const SAVED_OUTPUT: &str = "saved output";
    const SAVE_ERROR: &str = "save failed";
    const FIRST: &str = "first provider";
    const SECOND: &str = "second provider";
    const TEST_MODEL: &str = "anthropic/claude-sonnet-4-20250514";
    const TEST_CWD: &str = "/tmp";
    type Checkpoint = Arc<dyn CheckpointWriter<crate::session_coordinator::SessionCheckpoint>>;
    type RecordedRequest = (&'static str, String, String, bool);

    #[derive(Clone, Copy)]
    enum ProviderExit {
        Success,
        Pending,
    }

    struct GatedHeadlessProvider {
        entered: flume::Sender<()>,
        release: Receiver<()>,
        exit: ProviderExit,
    }

    fn response(text: &str) -> StreamResponse {
        StreamResponse {
            message: Message {
                role: Role::Assistant,
                content: vec![ContentBlock::Text { text: text.into() }],
                ..Default::default()
            },
            usage: TokenUsage::default(),
            stop_reason: Some(StopReason::EndTurn),
        }
    }

    impl Provider for GatedHeadlessProvider {
        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            _: &'a [Message],
            _: &'a str,
            _: &'a Value,
            _: &'a flume::Sender<maki_providers::ProviderEvent>,
            _: RequestOptions,
            _: Option<&'a SessionRef>,
        ) -> BoxFuture<'a, Result<StreamResponse, crate::AgentError>> {
            Box::pin(async move {
                self.entered.send_async(()).await.unwrap();
                self.release.recv_async().await.unwrap();
                match self.exit {
                    ProviderExit::Success => Ok(response(SAVED_OUTPUT)),
                    ProviderExit::Pending => std::future::pending().await,
                }
            })
        }

        fn list_models(
            &self,
        ) -> BoxFuture<'_, Result<Vec<maki_providers::ModelInfo>, crate::AgentError>> {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    struct RecordingProvider {
        name: &'static str,
        requests: flume::Sender<(&'static str, String, String, bool)>,
    }

    impl Provider for RecordingProvider {
        fn stream_message<'a>(
            &'a self,
            model: &'a Model,
            _: &'a [Message],
            system: &'a str,
            _: &'a Value,
            _: &'a flume::Sender<maki_providers::ProviderEvent>,
            options: RequestOptions,
            _: Option<&'a SessionRef>,
        ) -> BoxFuture<'a, Result<StreamResponse, crate::AgentError>> {
            Box::pin(async move {
                let _ = self
                    .requests
                    .send((self.name, model.spec(), system.into(), options.fast));
                Ok(response(self.name))
            })
        }

        fn list_models(
            &self,
        ) -> BoxFuture<'_, Result<Vec<maki_providers::ModelInfo>, crate::AgentError>> {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    fn first_input() -> AgentInput {
        AgentInput::from_defaults("hello".into(), Vec::new(), SessionDefaults::default())
    }

    /// Spawns a headless session on {provider} and admits its first turn.
    fn spawn_started(
        params: InteractiveParams,
        provider: Result<Arc<dyn Provider>, crate::AgentError>,
    ) -> InteractiveHandle {
        let handle = spawn_interactive_with_binding(
            params,
            Box::new(move |model, _| {
                Box::pin(async move { provider.map(|provider| (model, provider)) })
            }),
            None,
            None,
        );
        let _ = smol::block_on(handle.input_tx.admit(first_input()));
        handle
    }

    fn interactive_params(session_id: MakiId, history: Vec<Message>) -> InteractiveParams {
        InteractiveParams {
            model: Model::from_spec(TEST_MODEL).unwrap(),
            config: AgentConfig::default(),
            permissions_config: PermissionsConfig::default(),
            timeouts: Timeouts::default(),
            prompt_slots: Arc::default(),
            excluded_tools: Vec::new(),
            mcp_handle: None,
            initial_wd: PathBuf::from(TEST_CWD),
            session_id: Some(SessionRef::from(session_id)),
            initial_history: history,
            yolo: false,
            system_prompt_override: Some("admitted system".into()),
            append_system_prompt: None,
            defaults: Default::default(),
            model_policy: Arc::default(),
            modes: Arc::new(ModeRegistry::builtin()),
            question_mode: crate::tools::QuestionMode::Headless,
            plugin_rules: Arc::default(),
            project_config: ProjectConfig::for_project(Path::new(TEST_CWD)),
            local_tools: Arc::default(),
            state_dir: None,
        }
    }

    pub(crate) fn command_fixture(
        model: Model,
        coordinator: SessionCoordinatorHandle,
    ) -> InteractiveHandle {
        let (provider, _) = recording();
        let mut params = interactive_params(coordinator.read().session_id(), Vec::new());
        params.model = model;
        let binding = coordinator_binding(coordinator, Arc::clone(&provider));
        spawn_interactive_with_binding(
            params,
            Box::new(move |model, _| Box::pin(async move { Ok((model, provider)) })),
            None,
            Some(binding),
        )
    }

    fn coordinator_binding(
        coordinator: SessionCoordinatorHandle,
        provider: Arc<dyn Provider>,
    ) -> RootBinding {
        Box::new(move |_, root, _| {
            Box::pin(async move {
                coordinator
                    .bind_actor(
                        root,
                        Arc::new(move |model| {
                            let provider = Arc::clone(&provider);
                            Box::pin(async move { Ok(PreparedModel { provider, model }) })
                        }),
                        Arc::new(|_| {}),
                    )
                    .map_err(|error| error.to_string())
            })
        })
    }

    fn checkpoint() -> Checkpoint {
        accept_all()
    }

    fn coordinator(session_id: MakiId, checkpoint: Checkpoint) -> SessionCoordinatorHandle {
        SessionCoordinatorHandle::register(SessionCoordinatorParams {
            session_id,
            catalog: Default::default(),
            definitions: builtin_option_definitions(
                Arc::from(TEST_MODEL),
                [Arc::from(TEST_MODEL)],
                false,
                false,
                false,
                Default::default(),
            ),
            persisted_options: Default::default(),
            model_policy: Arc::default(),
            checkpoint,
            mailbox: SessionMailbox::new(session_id),
        })
        .unwrap()
    }

    fn fixture(
        history: Vec<Message>,
        checkpoint: Checkpoint,
        provider: Arc<dyn Provider>,
    ) -> (InteractiveHandle, SessionCoordinatorHandle) {
        let session_id = MakiId::generate();
        let coordinator = coordinator(session_id, checkpoint);
        let binding = coordinator_binding(coordinator.clone(), Arc::clone(&provider));
        let handle = spawn_interactive_with_binding(
            interactive_params(session_id, history),
            Box::new(move |model, _| Box::pin(async move { Ok((model, provider)) })),
            None,
            Some(binding),
        );
        (handle, coordinator)
    }

    async fn bounded<F: Future>(future: F) -> F::Output {
        future::or(future, async {
            smol::Timer::after(GATE_TIMEOUT).await;
            panic!("headless operation did not settle");
        })
        .await
    }

    fn recording() -> (Arc<dyn Provider>, Receiver<RecordedRequest>) {
        let (requests, received) = flume::unbounded();
        (
            Arc::new(RecordingProvider {
                name: FIRST,
                requests,
            }),
            received,
        )
    }

    #[test]
    fn terminal_waits_for_checkpoint_then_forwards() {
        smol::block_on(async {
            let (seen, received) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            let checkpoint: Checkpoint = Arc::new(
                move |request: CheckpointRequest<crate::session_coordinator::SessionCheckpoint>| {
                    let seen = seen.clone();
                    let released = released.clone();
                    Box::pin(async move {
                        seen.send_async(request.snapshot.history.clone().unwrap())
                            .await
                            .unwrap();
                        released.recv_async().await.unwrap();
                        Ok(CheckpointAck::for_request(&request))
                    }) as CheckpointFuture
                },
            );
            let (provider, _) = recording();
            let (handle, coordinator) = fixture(Vec::new(), checkpoint, provider);
            let root = handle.initialized_root().await.unwrap();
            let ticket = handle.input_tx.admit(first_input()).await.unwrap();
            let saved = bounded(received.recv_async()).await.unwrap();
            assert!(saved.messages.len() >= 2);
            assert!(ticket.peek_result().is_none());
            assert!(
                !handle
                    .event_rx
                    .try_iter()
                    .any(|event| matches!(event.event, AgentEvent::TurnOutcome(_)))
            );
            release.send_async(()).await.unwrap();
            assert!(matches!(
                bounded(ticket.wait()).await,
                TurnOutcome::Completed { .. }
            ));
            let mut terminals = 0;
            while let Ok(event) = handle.event_rx.try_recv() {
                if let AgentEvent::TurnOutcome(outcome) = event.event {
                    assert_eq!(outcome.agent_id(), root.id());
                    terminals += 1;
                }
            }
            assert_eq!(terminals, 1);
            assert!(ticket.peek_result().is_some());
            handle.task.cancel().await;
            coordinator.close().await.unwrap();
        });
    }

    #[test_case(false, SAVE_ERROR ; "checkpoint_failure_fails_the_turn_once")]
    #[test_case(true, TERMINAL_CHECKPOINT_TIMEOUT ; "hung_checkpoint_times_out_into_one_failure")]
    fn checkpoint_failure_fails_the_turn_with_its_usage(hang: bool, expected: &str) {
        smol::block_on(async {
            let checkpoint: Checkpoint = Arc::new(
                move |request: CheckpointRequest<crate::session_coordinator::SessionCheckpoint>| {
                    Box::pin(async move {
                        if hang {
                            future::pending::<()>().await;
                        }
                        Err(CheckpointError::Save {
                            session_id: request.session_id,
                            message: Arc::from(SAVE_ERROR),
                        })
                    }) as CheckpointFuture
                },
            );
            let session_id = MakiId::generate();
            let coordinator = coordinator(session_id, checkpoint);
            let (provider, _) = recording();
            let binding = coordinator_binding(coordinator.clone(), Arc::clone(&provider));
            let mut params = interactive_params(session_id, Vec::new());
            if hang {
                params.timeouts.low_speed = Duration::ZERO;
            }
            let handle = spawn_interactive_with_binding(
                params,
                Box::new(move |model, _| Box::pin(async move { Ok((model, provider)) })),
                None,
                Some(binding),
            );
            let ticket = handle.input_tx.admit(first_input()).await.unwrap();
            let settled = bounded(ticket.wait()).await;
            assert!(
                matches!(&settled, TurnOutcome::Failed { failure, num_turns, .. }
                    if failure.user_message.contains(expected) && *num_turns > 0),
                "{settled:?}"
            );
            let terminals: Vec<_> = handle
                .event_rx
                .try_iter()
                .filter(|envelope| {
                    matches!(
                        envelope.event,
                        AgentEvent::TurnOutcome(_) | AgentEvent::ControlError { .. }
                    )
                })
                .collect();
            assert!(
                matches!(
                    terminals.as_slice(),
                    [Envelope { event: AgentEvent::TurnOutcome(outcome), .. }] if *outcome == settled
                ),
                "{terminals:?}"
            );
            handle.task.cancel().await;
            coordinator.close().await.unwrap();
        });
    }

    #[test_case(false ; "manual_compaction_persists_before_reporting_success")]
    #[test_case(true ; "failed_persistence_reports_failure_and_rolls_history_back")]
    fn managed_manual_compaction(fail: bool) {
        smol::block_on(async {
            let original = vec![Message::user("original".into())];
            let (seen, received) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            let checkpoint: Checkpoint = Arc::new(
                move |request: CheckpointRequest<crate::session_coordinator::SessionCheckpoint>| {
                    let seen = seen.clone();
                    let released = released.clone();
                    Box::pin(async move {
                        seen.send_async(()).await.unwrap();
                        released.recv_async().await.unwrap();
                        if fail {
                            Err(CheckpointError::Save {
                                session_id: request.session_id,
                                message: Arc::from(SAVE_ERROR),
                            })
                        } else {
                            Ok(CheckpointAck::for_request(&request))
                        }
                    }) as CheckpointFuture
                },
            );
            let (provider, _) = recording();
            let (handle, coordinator) = fixture(original.clone(), checkpoint, provider);
            let (output, results) = flume::unbounded();
            handle
                .control_tx
                .send(InteractiveControl::ManualCompaction {
                    instructions: None,
                    output,
                    cancel: CancelToken::none(),
                })
                .await
                .unwrap();
            assert_eq!(
                bounded(results.recv_async()).await.unwrap(),
                ManualCompactionEvent::Started
            );
            bounded(received.recv_async()).await.unwrap();
            assert!(results.is_empty());
            release.send_async(()).await.unwrap();
            let terminal = bounded(results.recv_async()).await.unwrap();
            if fail {
                assert!(
                    matches!(terminal, ManualCompactionEvent::Failed(error) if error.contains(SAVE_ERROR))
                );
            } else {
                assert_eq!(terminal, ManualCompactionEvent::Completed);
            }
            let root = handle.initialized_root().await.unwrap();
            let actor = root.actor().unwrap();
            bounded(actor.runtime_barrier()).await.unwrap();
            let transcript = actor
                .transcript(crate::actor::TranscriptRequest {
                    through_turn: None,
                    last_messages: original.len() + 4,
                    max_bytes: 4096,
                })
                .unwrap();
            assert_eq!(transcript.compacted, !fail);
            if fail {
                assert_eq!(actor.committed_message_count(), original.len());
            }
            let ticket = handle.input_tx.admit(first_input()).await.unwrap();
            bounded(received.recv_async()).await.unwrap();
            release.send_async(()).await.unwrap();
            bounded(ticket.wait()).await;
            handle.task.cancel().await;
            coordinator.close().await.unwrap();
        });
    }

    #[test_case(InteractiveControl::Compact ; "compact")]
    #[test_case(InteractiveControl::Reset ; "reset")]
    fn control_dropped_on_close_replies_cancelled(
        control: fn(flume::Sender<Result<(), String>>) -> InteractiveControl,
    ) {
        smol::block_on(async {
            let (seen, received) = flume::bounded(1);
            let (release, released) = flume::bounded::<()>(1);
            let checkpoint: Checkpoint = Arc::new(
                move |request: CheckpointRequest<crate::session_coordinator::SessionCheckpoint>| {
                    let seen = seen.clone();
                    let released = released.clone();
                    Box::pin(async move {
                        let _ = seen.send_async(()).await;
                        let _ = released.recv_async().await;
                        Ok(CheckpointAck::for_request(&request))
                    }) as CheckpointFuture
                },
            );
            let (provider, _) = recording();
            let (handle, coordinator) = fixture(Vec::new(), checkpoint, provider);
            let root = handle.initialized_root().await.unwrap();
            let _turn = handle.input_tx.admit(first_input()).await.unwrap();
            bounded(received.recv_async()).await.unwrap();
            let (reply, replies) = flume::bounded(1);
            handle.control_tx.send(control(reply)).await.unwrap();
            root.actor().unwrap().close();
            assert_eq!(
                bounded(replies.recv_async()).await.unwrap(),
                Err(OPERATION_CANCELLED.to_owned())
            );
            drop(release);
            handle.task.cancel().await;
            coordinator.close().await.unwrap();
        });
    }

    #[test_case(false ; "a_failed_compaction_is_never_persisted")]
    #[test_case(true ; "cancelled_persistence_reports_cancellation_not_failure")]
    fn unsuccessful_compaction_rolls_back(cancelled: bool) {
        smol::block_on(async {
            let previous =
                crate::agent::HistorySnapshot::new(vec![Message::user("original".into())]);
            let mut history = History::new(vec![Message::user("compacted".into())]);
            let (trigger, cancel) = CancelToken::new();
            if cancelled {
                trigger.cancel();
            }
            let compacted = if cancelled {
                Ok(())
            } else {
                Err(SAVE_ERROR.into())
            };
            let event = settle_manual_compaction(
                compacted,
                &mut history,
                previous.clone(),
                &cancel,
                |_| async move {
                    assert!(cancelled, "failed compaction must not persist");
                    Err(SAVE_ERROR.into())
                },
            )
            .await;
            assert_eq!(
                serde_json::to_value(history.as_slice()).unwrap(),
                serde_json::to_value(previous.messages.as_ref()).unwrap()
            );
            assert_eq!(history.snapshot().epoch, previous.epoch);
            assert_eq!(
                event,
                if cancelled {
                    ManualCompactionEvent::Cancelled
                } else {
                    ManualCompactionEvent::Failed(SAVE_ERROR.into())
                }
            );
        });
    }

    #[test]
    fn headless_queued_settings_survive_config_change() {
        smol::block_on(async {
            let (requests, received) = flume::unbounded();
            let first: Arc<dyn Provider> = Arc::new(RecordingProvider {
                name: FIRST,
                requests: requests.clone(),
            });
            let (handle, coordinator) = fixture(Vec::new(), checkpoint(), first);
            let root = handle.initialized_root().await.unwrap();
            let actor = root.actor().unwrap();
            let first = handle.input_tx.admit(first_input()).await.unwrap();
            let update = actor.reserve_prepared_operation(None).unwrap();
            update
                .commit_when_ready(Some(ConfigChange::Patch(ConfigPatch {
                    model: Some(PreparedModel {
                        provider: Arc::new(RecordingProvider {
                            name: SECOND,
                            requests,
                        }),
                        model: Model::from_spec("anthropic/claude-opus-4-8").unwrap(),
                    }),
                    fast: Some(true),
                    workflow: Some(true),
                    ..Default::default()
                })))
                .await
                .unwrap();
            let second = handle.input_tx.admit(first_input()).await.unwrap();
            bounded(first.wait()).await;
            bounded(second.wait()).await;
            let before = bounded(received.recv_async()).await.unwrap();
            let after = bounded(received.recv_async()).await.unwrap();
            assert_eq!(
                before,
                (
                    FIRST,
                    TEST_MODEL.to_owned(),
                    "admitted system".into(),
                    false
                )
            );
            assert_eq!(
                after,
                (
                    SECOND,
                    "anthropic/claude-opus-4-8".into(),
                    "admitted system".into(),
                    true
                )
            );
            assert!(received.is_empty());
            handle.task.cancel().await;
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn reset_is_ordered_after_active_turn() {
        smol::block_on(async {
            let (entered, received) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            let provider = Arc::new(GatedHeadlessProvider {
                entered,
                release: released,
                exit: ProviderExit::Success,
            });
            let (handle, coordinator) = fixture(Vec::new(), checkpoint(), provider);
            let primary = handle.input_tx.admit(first_input()).await.unwrap();
            bounded(received.recv_async()).await.unwrap();
            let (reply, response) = flume::bounded(1);
            handle
                .control_tx
                .send(InteractiveControl::Reset(reply))
                .await
                .unwrap();
            assert!(response.is_empty());
            release.send_async(()).await.unwrap();
            bounded(primary.wait()).await;
            bounded(response.recv_async()).await.unwrap().unwrap();
            let root = handle.initialized_root().await.unwrap();
            assert_eq!(root.actor().unwrap().committed_message_count(), 0);
            handle.task.cancel().await;
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn queued_compaction_cancellation_does_not_wait_for_active_turn() {
        smol::block_on(async {
            let (entered, received) = flume::bounded(1);
            let (_release, released) = flume::bounded(1);
            let provider = Arc::new(GatedHeadlessProvider {
                entered,
                release: released,
                exit: ProviderExit::Pending,
            });
            let (handle, coordinator) = fixture(Vec::new(), checkpoint(), provider);
            let primary = handle.input_tx.admit(first_input()).await.unwrap();
            bounded(received.recv_async()).await.unwrap();
            let (trigger, cancel) = CancelToken::new();
            let (output, events) = flume::unbounded();
            handle
                .control_tx
                .send(InteractiveControl::ManualCompaction {
                    instructions: None,
                    output,
                    cancel,
                })
                .await
                .unwrap();
            trigger.cancel();
            assert_eq!(
                bounded(events.recv_async()).await.unwrap(),
                ManualCompactionEvent::Cancelled
            );
            assert!(primary.peek().is_none());
            let root = handle.initialized_root().await.unwrap();
            root.cancel().unwrap();
            assert!(matches!(
                bounded(primary.wait()).await,
                TurnOutcome::Cancelled { .. }
            ));
            assert!(received.is_empty());
            handle.task.cancel().await;
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn binding_precedes_first_admission() {
        const DISPATCHED_INPUT: &str = "expanded command prompt";
        smol::block_on(async {
            let (provider, received) = recording();
            let (bound, binding_seen) = flume::bounded(1);
            let session_id = MakiId::generate();
            let coordinator = coordinator(session_id, checkpoint());
            let bind_actor = coordinator_binding(coordinator.clone(), Arc::clone(&provider));
            let mut params = interactive_params(session_id, Vec::new());
            params.config.max_concurrent_agent_turns = 1;
            let handle = spawn_interactive_with_binding(
                params,
                Box::new(move |model, _| Box::pin(async move { Ok((model, provider)) })),
                None,
                Some(Box::new(move |manager, root, template| {
                    Box::pin(async move {
                        assert_eq!(manager.root_id().unwrap(), root.id());
                        assert_eq!(manager.limits().max_concurrent_agent_turns, 1);
                        assert_eq!(template.mode, AgentMode::Build);
                        assert_eq!(template.execution_agent_id, Some(root.id()));
                        assert_eq!(template.cwd, PathBuf::from(TEST_CWD));
                        assert!(template.managed_turn.is_none());
                        bound.send_async(root.id()).await.unwrap();
                        bind_actor(manager, root, template).await
                    })
                })),
            );
            let root = handle.initialized_root().await.unwrap();
            assert_eq!(binding_seen.recv_async().await.unwrap(), root.id());
            assert!(received.is_empty());
            let InteractiveHandle {
                input_tx,
                event_rx,
                manager,
                task,
                ..
            } = handle;
            let ticket = input_tx
                .admit(AgentInput::from_defaults(
                    DISPATCHED_INPUT.into(),
                    Vec::new(),
                    SessionDefaults::default(),
                ))
                .await
                .unwrap();
            assert!(matches!(
                bounded(ticket.wait()).await,
                TurnOutcome::Completed { .. }
            ));
            assert_eq!(received.len(), 1);
            drop(input_tx);
            bounded(task).await;
            let inputs = event_rx
                .try_iter()
                .filter_map(|envelope| match envelope.event {
                    AgentEvent::QueueItemConsumed { text, .. } => Some(text),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(inputs, [DISPATCHED_INPUT]);
            assert!(
                !manager
                    .snapshot()
                    .iter()
                    .any(|node| node.graph_lifecycle == crate::GraphLifecycle::Live)
            );
            coordinator.close().await.unwrap();
        });
    }

    #[test_case(ProviderExit::Success ; "successful_completion")]
    #[test_case(ProviderExit::Pending ; "cancelled_task")]
    fn headless_offload_cleanup_covers_task_exit(exit: ProviderExit) {
        let state = tempfile::tempdir().unwrap();
        let session_id = MakiId::generate();
        let mut params = interactive_params(session_id, Vec::new());
        params.state_dir = Some(state.path().to_path_buf());
        let (entered, received) = flume::bounded(1);
        let (release, released) = flume::bounded(1);
        let handle = spawn_started(
            params,
            Ok(Arc::new(GatedHeadlessProvider {
                entered,
                release: released,
                exit,
            })),
        );
        let InteractiveHandle {
            input_tx,
            event_rx,
            task,
            offload_cleanup,
            ..
        } = handle;
        received.recv_timeout(GATE_TIMEOUT).unwrap();
        let dir = offload_dir(&state.path().join(SESSIONS_DIR), session_id);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("saved.txt"), SAVED_OUTPUT).unwrap();
        let cleanup = offload_cleanup.unwrap();
        match exit {
            ProviderExit::Pending => {
                smol::block_on(task.cancel());
            }
            ProviderExit::Success => {
                release.send(()).unwrap();
                let mut input_tx = Some(input_tx);
                while let Ok(event) = event_rx.recv() {
                    match event.event {
                        AgentEvent::TurnOutcome(_) => drop(input_tx.take()),
                        AgentEvent::StreamClosed => {
                            assert!(!dir.exists());
                            break;
                        }
                        _ => {}
                    }
                }
                smol::block_on(task);
            }
        }
        smol::block_on(cleanup.wait()).unwrap();
        assert!(!dir.exists());
    }

    struct GatedGuardRemoval {
        entered: flume::Sender<()>,
        release: Receiver<()>,
    }

    impl OffloadBackend for GatedGuardRemoval {
        fn create_new(&self, _: &str, _: &[u8]) -> io::Result<bool> {
            Ok(true)
        }
        fn total_bytes(&self) -> io::Result<u64> {
            Ok(0)
        }
        fn remove_all(&self) -> io::Result<()> {
            self.entered.send(()).unwrap();
            self.release.recv_timeout(GATE_TIMEOUT).unwrap();
            Ok(())
        }
        fn path(&self, name: &str) -> PathBuf {
            PathBuf::from("/headless-offload-test").join(name)
        }
    }

    #[test]
    fn headless_dropped_task_requests_cleanup_without_waiting_for_removal() {
        let (entered, received) = flume::bounded(1);
        let (release, released) = flume::bounded(1);
        let store = Arc::new(OffloadStore::new(Box::new(GatedGuardRemoval {
            entered,
            release: released,
        })));
        let cleanup = OffloadCleanup::new(Arc::clone(&store));
        let guard = RemoveOffloadOnDrop(cleanup.clone());
        let task = smol::spawn(async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        });
        drop(task);
        received.recv_timeout(GATE_TIMEOUT).unwrap();
        assert!(matches!(store.put(SAVED_OUTPUT), Err(OffloadError::Closed)));
        let mut wait = Box::pin(cleanup.wait());
        assert!(smol::block_on(future::poll_once(&mut wait)).is_none());
        release.send(()).unwrap();
        smol::block_on(wait).unwrap();
    }

    #[test]
    fn headless_initialization_error_drains_cleanup_before_stream_close() {
        let state = tempfile::tempdir().unwrap();
        let session_id = MakiId::generate();
        let dir = offload_dir(&state.path().join(SESSIONS_DIR), session_id);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("saved.txt"), SAVED_OUTPUT).unwrap();
        let mut params = interactive_params(session_id, Vec::new());
        params.state_dir = Some(state.path().to_path_buf());
        let handle = spawn_started(
            params,
            Err(crate::AgentError::Config {
                message: HEADLESS_PROVIDER_ERROR.into(),
            }),
        );
        let mut saw_error = false;
        let mut saw_close = false;
        while let Ok(event) = handle.event_rx.recv() {
            match event.event {
                AgentEvent::ControlError { message } => {
                    assert!(message.contains(HEADLESS_PROVIDER_ERROR));
                    saw_error = true;
                }
                AgentEvent::StreamClosed => {
                    assert!(!dir.exists());
                    saw_close = true;
                    break;
                }
                _ => {}
            }
        }
        assert!(saw_error && saw_close);
        smol::block_on(handle.task);
        smol::block_on(handle.offload_cleanup.unwrap().wait()).unwrap();
    }

    #[test]
    fn extract_tool_names_filters_valid_entries() {
        assert_eq!(
            extract_tool_names(
                &serde_json::json!([{"name": "read"}, {"type": "function"}, {"name": "bash"}])
            ),
            vec!["read", "bash"]
        );
    }

    #[test]
    fn advertised_names_show_tool_search_not_deferred_tools() {
        let base = serde_json::json!([{"name": "read"}]);
        let mcp =
            crate::mcp::test_support::stub_session(&[("srv.fetch_issue", "Fetch a GitHub issue")]);
        assert_eq!(
            advertised_tool_names(&base, Some(&mcp)),
            vec!["read", crate::mcp::TOOL_SEARCH_TOOL_NAME]
        );
        assert_eq!(advertised_tool_names(&base, None), vec!["read"]);
    }
}
