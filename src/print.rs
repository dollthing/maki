//! Non-interactive (headless) mode: `makima --print -p "prompt"`.
//!
//! Wire format intentionally matches Claude Code so existing scripts work
//! unchanged. Keep `PrintResult` fields a strict subset of theirs. `StreamJson`
//! is JSONL with the same shape, `Text` prints the raw response only.
//!
//! We adopt new fields when Claude Code adds them but never invent our own.
//! Check their docs before changing anything here.

use std::any::Any;
use std::io::{self, Read};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::ValueEnum;
use color_eyre::Result;
use color_eyre::eyre::{Context, eyre};
use maki_agent::command::{self, StandardCommands, StandardCompletions};
use maki_agent::headless::{HeadlessHandle, HeadlessParams, RootBinding};
use maki_agent::permissions::PluginRuleStore;
use maki_agent::session_coordinator::SessionCoordinatorHandle;
use maki_agent::tools::QUESTION_TOOL_NAME;
use maki_agent::tools::offload::OffloadCleanup;
use maki_agent::{
    AgentConfig, AgentEvent, AgentInput, AgentMode, DoneReason, Envelope, ImageSource,
    ModeRegistry, PermissionsConfig, TurnOutcome,
};
use maki_commands::{
    BuiltinOperation, CommandContent, CommandError, CommandFuture, CommandHost, CommandOutcome,
    CommandRegistry, HostRequest, HostResponse, InputDispatch, TargetCapabilities,
    TargetCapability,
};
use maki_config::{ModelPolicy, ProjectConfig, SessionDefaults};
use maki_lua::EventHandle;
use maki_lua::orchestration::{AgentOrchestration, TrustedTarget};
use maki_lua::session_snapshot::{HeadlessMeta, HeadlessSnapshot, MODE_BUILD};
use maki_providers::model::Model;
use maki_providers::{CompleteModelSelection, TokenUsage, add_cost};
use maki_storage::id::{MakiId, SessionRef};
use serde::Serialize;
use serde_json::Value;

use crate::command_attachments;

const AGENT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

// Fails fast: silently dropping an image the caller explicitly attached
// would be worse than erroring.
fn load_images(paths: &[PathBuf]) -> Result<Vec<ImageSource>> {
    paths
        .iter()
        .map(|path| {
            let media_type = maki_ui::image::media_type_for(path)
                .ok_or_else(|| eyre!("unsupported image type: {}", path.display()))?;
            maki_ui::image::load_file_image(path, media_type)
                .map_err(|e| eyre!("failed to load image: {e}"))
        })
        .collect()
}

#[derive(Clone, ValueEnum)]
pub enum OutputFormat {
    Text,
    Json,
    StreamJson,
}

#[derive(Serialize)]
struct PrintResult {
    #[serde(rename = "type")]
    result_type: &'static str,
    subtype: &'static str,
    is_error: bool,
    duration_ms: u128,
    num_turns: u32,
    result: String,
    stop_reason: Option<DoneReason>,
    session_id: SessionRef,
    total_cost_usd: f64,
    usage: TokenUsage,
}

#[derive(Default)]
struct PrintTurnState {
    result_text: String,
    is_error: bool,
    num_turns: u32,
    usage: TokenUsage,
    stop_reason: Option<DoneReason>,
}

impl PrintTurnState {
    fn accept_outcome(&mut self, envelope: &Envelope) -> bool {
        let AgentEvent::TurnOutcome(outcome) = &envelope.event else {
            return false;
        };
        if envelope.subagent.is_some() {
            return false;
        }
        self.num_turns = outcome.num_turns();
        self.usage = outcome.usage();
        match outcome {
            TurnOutcome::Completed { reason, .. } => self.stop_reason = Some(*reason),
            TurnOutcome::Failed { failure, .. } => {
                self.is_error = true;
                self.result_text = failure.user_message.clone();
            }
            TurnOutcome::Cancelled { .. } => self.is_error = true,
        }
        true
    }
}

#[derive(Serialize)]
struct InitEvent<'a> {
    #[serde(rename = "type")]
    event_type: &'static str,
    subtype: &'static str,
    cwd: &'a str,
    session_id: &'a SessionRef,
    tools: &'a [String],
    model: &'a str,
}

#[derive(Serialize)]
struct AssistantEvent<'a> {
    #[serde(rename = "type")]
    event_type: &'static str,
    message: AssistantMessage<'a>,
    session_id: &'a SessionRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_tool_use_id: Option<&'a str>,
}

#[derive(Serialize)]
struct AssistantMessage<'a> {
    model: &'a str,
    role: &'static str,
    content: &'a Value,
    usage: &'a TokenUsage,
}

#[derive(Serialize)]
struct UserEvent<'a> {
    #[serde(rename = "type")]
    event_type: &'static str,
    message: UserMessage<'a>,
    session_id: &'a SessionRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_tool_use_id: Option<&'a str>,
}

#[derive(Serialize)]
struct UserMessage<'a> {
    role: &'static str,
    content: &'a Value,
}

#[derive(Serialize)]
struct RetryEvent<'a> {
    #[serde(rename = "type")]
    event_type: &'static str,
    subtype: &'static str,
    attempt: u32,
    retry_delay_ms: u64,
    error: &'a str,
    session_id: &'a SessionRef,
}

enum VerboseOutput {
    StreamJson,
    Json(Vec<Value>),
}

trait PrintRunner {
    fn run(self, input: AgentInput) -> Result<()>;
}

impl<F> PrintRunner for F
where
    F: FnOnce(AgentInput) -> Result<()>,
{
    fn run(self, input: AgentInput) -> Result<()> {
        self(input)
    }
}

#[derive(Default)]
struct CommandTurnMarker;

#[derive(Default)]
struct PrintCommandHost(Option<TrustedTarget>);

fn print_capabilities() -> TargetCapabilities {
    TargetCapabilities::from_slice(&[
        TargetCapability::AgentTurns,
        TargetCapability::ApplicationLifecycle,
    ])
}

impl CommandHost for PrintCommandHost {
    fn admission_context(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        self.0
            .clone()
            .map(|target| Arc::new(target) as Arc<dyn Any + Send + Sync>)
    }

    fn request(&self, request: HostRequest) -> CommandFuture<Result<HostResponse, CommandError>> {
        Box::pin(async move {
            match request {
                HostRequest::Context(_) => Ok(HostResponse::Context(
                    maki_commands::HostContextResponse::Unavailable,
                )),
                HostRequest::Builtin(BuiltinOperation::Exit) => Ok(HostResponse::Completed),
                HostRequest::Builtin(BuiltinOperation::QuickQuestion {
                    question,
                    attachments,
                }) => Ok(HostResponse::AgentTurn(maki_commands::AgentTurn {
                    content: CommandContent {
                        text: question,
                        attachments,
                    },
                    prompt: None,
                })),
                HostRequest::Builtin(operation) => Err(CommandError::Producer(Arc::from(format!(
                    "unsupported command operation: {operation:?}"
                )))),
            }
        })
    }
}

fn drive_print(
    registry: &CommandRegistry,
    target: &maki_commands::TargetHandle,
    _marker: &CommandTurnMarker,
    literal: AgentInput,
    runner: impl PrintRunner,
) -> Result<()> {
    let content = CommandContent {
        text: Arc::from(literal.message.as_str()),
        attachments: command_attachments::from_images(&literal.images),
    };
    let input = match smol::block_on(registry.dispatch_input(target, content)) {
        InputDispatch::Dispatched(CommandOutcome::AgentTurn(turn)) => {
            let defaults = SessionDefaults {
                fast: literal.fast,
                workflow: literal.workflow,
                thinking: Some(literal.thinking.into()),
            };
            command_attachments::agent_input(turn, literal.mode.clone(), defaults)?
        }
        InputDispatch::Dispatched(
            CommandOutcome::Completed | CommandOutcome::FrontendFeedback(_),
        ) => {
            return Ok(());
        }
        InputDispatch::Dispatched(CommandOutcome::IsolatedTurn(_)) => {
            return Err(color_eyre::eyre::eyre!(
                "isolated turns are unavailable in print mode"
            ));
        }
        InputDispatch::Dispatched(CommandOutcome::ManualCompaction(_)) => {
            return Err(color_eyre::eyre::eyre!(
                "manual compaction is unavailable in print mode"
            ));
        }
        InputDispatch::Dispatched(CommandOutcome::Failed(error)) => return Err(error.into()),
        InputDispatch::LiteralInput(content) => AgentInput {
            message: content.text.to_string(),
            mode: literal.mode.clone(),
            images: command_attachments::into_images(&content.attachments)?,
            preamble: Vec::new(),
            thinking: Default::default(),
            fast: literal.fast,
            workflow: literal.workflow,
            prompt: None,
            cancel: None,
        },
    };
    runner.run(input)
}

impl VerboseOutput {
    fn emit(&mut self, value: &impl Serialize) -> Result<()> {
        match self {
            Self::StreamJson => println!("{}", serde_json::to_string(value)?),
            Self::Json(events) => events.push(serde_json::to_value(value)?),
        }
        Ok(())
    }
}

fn run_and_settle<T>(
    task: smol::Task<()>,
    cleanup: Option<OffloadCleanup>,
    on_cancel: impl AsyncFnOnce(Instant),
    body: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let body_result = catch_unwind(AssertUnwindSafe(body));
    let deadline = Instant::now() + AGENT_SHUTDOWN_TIMEOUT;
    let settlement = catch_unwind(AssertUnwindSafe(|| {
        smol::block_on(async {
            let mut task = task;
            if matches!(&body_result, Ok(Ok(_)))
                && futures_lite::future::or(
                    async {
                        (&mut task).await;
                        true
                    },
                    async {
                        smol::Timer::at(deadline).await;
                        false
                    },
                )
                .await
            {
                return;
            }
            if let Some(cleanup) = &cleanup {
                cleanup.request();
            }
            futures_lite::future::or(
                async {
                    task.cancel().await;
                },
                async {
                    smol::Timer::at(deadline).await;
                },
            )
            .await;
            futures_lite::future::or(on_cancel(deadline), async {
                smol::Timer::at(deadline).await;
            })
            .await;
        });
    }));

    if let Some(cleanup) = cleanup {
        cleanup.request();
        smol::block_on(futures_lite::future::or(
            async {
                let _ = cleanup.wait().await;
            },
            async {
                smol::Timer::at(deadline).await;
            },
        ));
    }

    match (body_result, settlement) {
        (Err(body_panic), _) => resume_unwind(body_panic),
        (Ok(_), Err(task_panic)) => resume_unwind(task_panic),
        (Ok(result), Ok(())) => result,
    }
}

fn print_initialization_input(literal: &AgentInput) -> AgentInput {
    AgentInput::from_defaults(
        String::new(),
        literal.mode.clone(),
        Vec::new(),
        SessionDefaults {
            fast: literal.fast,
            workflow: literal.workflow,
            thinking: Some(literal.thinking.into()),
        },
    )
}

fn with_print_handle<T>(
    handle: HeadlessHandle,
    body: impl FnOnce(&HeadlessHandle) -> Result<T>,
) -> Result<T> {
    let cleanup = handle.offload_cleanup.clone();
    let manager = handle.manager.clone();
    let coordinator = SessionCoordinatorHandle::resolve(handle.session_id.id()).ok();
    let result = catch_unwind(AssertUnwindSafe(|| body(&handle)));
    let task = {
        let handle = handle;
        handle.task
    };
    run_and_settle(
        task,
        cleanup,
        async move |deadline| {
            futures_lite::future::zip(
                manager.shutdown(deadline.saturating_duration_since(Instant::now())),
                async {
                    if let Some(coordinator) = coordinator {
                        let _ = coordinator.close().await;
                    }
                },
            )
            .await;
        },
        || match result {
            Ok(result) => result,
            Err(panic) => resume_unwind(panic),
        },
    )
}

fn bind_print_root(lua_handle: EventHandle) -> (RootBinding, flume::Receiver<TrustedTarget>) {
    let (installed, services) = flume::bounded(1);
    let binding: RootBinding = Box::new(move |_, root, template| {
        Box::pin(async move {
            let target = TrustedTarget {
                target: root,
                template,
            };
            lua_handle.install_orchestration_services(Arc::new(
                AgentOrchestration::bind_fixed_root(Arc::default(), target.clone())
                    .with_completion(Arc::new(|_, _| Box::pin(async { Ok(()) }))),
            ));
            installed.send(target).map_err(|error| error.to_string())
        })
    });
    (binding, services)
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    model: &Model,
    prompt_arg: Option<String>,
    image_paths: Vec<PathBuf>,
    format: OutputFormat,
    verbose: bool,
    config: AgentConfig,
    permissions_config: PermissionsConfig,
    timeouts: maki_providers::Timeouts,
    lua_handle: EventHandle,
    defaults: SessionDefaults,
    model_policy: Arc<ModelPolicy>,
    system_prompt_override: Option<String>,
    append_system_prompt: Option<String>,
    plugin_rules: Arc<PluginRuleStore>,
    commands: &[command::CustomCommand],
    command_registry: CommandRegistry,
    modes: Arc<ModeRegistry>,
    project_config: ProjectConfig,
) -> Result<()> {
    let prompt = match prompt_arg {
        Some(p) => p,
        None => {
            let mut buf = String::new();
            io::stdin().read_to_string(&mut buf).context("read stdin")?;
            buf
        }
    };

    let images = load_images(&image_paths)?;
    let literal = AgentInput::from_defaults(prompt, AgentMode::Build, images, defaults);
    let _standard_commands =
        StandardCommands::register(&command_registry, commands, StandardCompletions::default())?;
    let prompt_slots = lua_handle.collect_prompt_slots();
    let session_options = lua_handle.session_option_catalog();
    let cwd = std::env::current_dir().unwrap_or_else(|_| ".".into());
    let (mcp_handle, mcp_config_errors) = smol::block_on(async {
        let (handle, errors) = maki_agent::mcp::start_with_commands(
            &cwd,
            project_config.clone(),
            command_registry.clone(),
        )
        .await;
        if let Some(handle) = &handle {
            handle.ready().await;
        }
        (handle, errors)
    });
    if !mcp_config_errors.is_empty() {
        eprintln!("MCP config error: {mcp_config_errors}");
    }

    let terminal_result_emitted = std::cell::Cell::new(false);
    let state_dir = maki_storage::paths::state_dir().ok();
    let outcome = (|| {
        let (binding, installed) = bind_print_root(lua_handle.clone());
        let handle = maki_agent::headless::spawn_with_binding(
            HeadlessParams {
                model: model.clone(),
                config,
                permissions_config,
                timeouts,
                input: print_initialization_input(&literal),
                prompt_slots,
                excluded_tools: vec![QUESTION_TOOL_NAME],
                mcp_handle,
                initial_wd: cwd,
                model_policy: Arc::clone(&model_policy),
                system_prompt_override,
                append_system_prompt,
                plugin_rules: Arc::clone(&plugin_rules),
                project_config,
                modes: Arc::clone(&modes),
                session_options: session_options.clone(),
                state_dir: state_dir.clone(),
            },
            Box::new(|mut model, timeouts| {
                Box::pin(async move {
                    let provider =
                        maki_providers::provider::from_model_async(&mut model, timeouts).await?;
                    Ok((model, Arc::from(provider)))
                })
            }),
            binding,
        )
        .map_err(|error| eyre!("register print session coordinator: {error}"))?;

        with_print_handle(handle, |handle| {
            smol::block_on(handle.initialized_root()).map_err(|error| eyre!(error))?;
            let trusted = installed.recv().map_err(|error| eyre!(error))?;
            let selection =
                CompleteModelSelection::from_model(&trusted.template.model, trusted.template.opts);
            let target = command_registry.bind_target(
                print_capabilities(),
                Arc::new(PrintCommandHost(Some(trusted))),
            );
            let snapshot = HeadlessSnapshot::default();
            snapshot.install(
                &lua_handle,
                HeadlessMeta {
                    id: handle.session_id.to_string(),
                    cwd: handle.cwd.clone(),
                    model: selection.spec.to_string(),
                    fast: selection.fast,
                    thinking: selection.thinking.to_string(),
                },
                || MODE_BUILD,
            );
            drive_print(
                &command_registry,
                &target,
                &CommandTurnMarker,
                literal,
                |input| {
                    smol::block_on(handle.start_with_input(input)).map_err(|error| eyre!(error))?;
                    let event_rx = &handle.event_rx;
                    let tool_names = &handle.tool_names;
                    let session_id = &handle.session_id;
                    let cwd = &handle.cwd;
                    let start = Instant::now();
                    let (mut result, verbose_out) = (|| -> Result<_> {
                        let mut verbose_out = match format {
                            OutputFormat::StreamJson => Some(VerboseOutput::StreamJson),
                            _ if verbose => Some(VerboseOutput::Json(Vec::new())),
                            _ => None,
                        };

                        if let Some(out) = &mut verbose_out {
                            out.emit(&InitEvent {
                                event_type: "system",
                                subtype: "init",
                                cwd,
                                session_id,
                                tools: tool_names,
                                model: &model.id,
                            })?;
                        }

                        let mut state = PrintTurnState::default();
                        // Summed as the turns land: rates move mid-run, and only a turn knows the
                        // rate it paid.
                        let mut cost = None;

                        while let Ok(envelope) = smol::block_on(event_rx.recv_async()) {
                            if matches!(envelope.event, AgentEvent::StreamClosed) {
                                break;
                            }
                            snapshot.observe(&envelope);
                            maki_lua::agent_autocmd::dispatch(
                                &lua_handle,
                                &session_id,
                                &envelope,
                                envelope.subagent.is_some(),
                            );
                            let Envelope {
                                ref event,
                                ref subagent,
                                ..
                            } = envelope;
                            let parent_tool_use_id =
                                subagent.as_ref().map(|s| s.parent_tool_use_id.as_str());

                            match event {
                                AgentEvent::TextDelta { text } => {
                                    if parent_tool_use_id.is_none() {
                                        state.result_text.push_str(text);
                                    }
                                }
                                AgentEvent::ThinkingDelta { .. } | AgentEvent::ThinkingBlockEnd => {
                                }
                                AgentEvent::ToolPending { .. }
                                | AgentEvent::ToolStart(_)
                                | AgentEvent::ToolExecutionStart { .. }
                                | AgentEvent::ToolOutput { .. }
                                | AgentEvent::ToolDone(_)
                                | AgentEvent::QueueItemConsumed { .. }
                                | AgentEvent::ModelSwitched { .. }
                                | AgentEvent::QueueSettled
                                | AgentEvent::AutoCompacting { .. }
                                | AgentEvent::CompactionDone { .. }
                                | AgentEvent::AuthRequired
                                | AgentEvent::PermissionRequest { .. }
                                | AgentEvent::Question { .. }
                                | AgentEvent::SubagentHistory { .. }
                                | AgentEvent::SubagentClosed
                                | AgentEvent::Post(_)
                                | AgentEvent::ToolSnapshot { .. }
                                | AgentEvent::ToolHeaderSnapshot { .. }
                                | AgentEvent::LiveToolBuf { .. }
                                | AgentEvent::Nudge
                                | AgentEvent::PromptProgress { .. }
                                | AgentEvent::StreamClosed => {}
                                AgentEvent::Retry {
                                    attempt,
                                    message,
                                    delay_ms,
                                } => {
                                    if let Some(out) = &mut verbose_out {
                                        out.emit(&RetryEvent {
                                            event_type: "system",
                                            subtype: "api_retry",
                                            attempt: *attempt,
                                            retry_delay_ms: *delay_ms,
                                            error: message,
                                            session_id,
                                        })?;
                                    }
                                }
                                AgentEvent::TurnComplete(tc) => {
                                    add_cost(&mut cost, tc.cost);
                                    if let Some(out) = &mut verbose_out {
                                        let content_value =
                                            serde_json::to_value(&tc.message.content)?;
                                        out.emit(&AssistantEvent {
                                            event_type: "assistant",
                                            message: AssistantMessage {
                                                model: &tc.model,
                                                role: "assistant",
                                                content: &content_value,
                                                usage: &tc.usage,
                                            },
                                            session_id,
                                            parent_tool_use_id,
                                        })?;
                                    }
                                }
                                AgentEvent::ToolResultsSubmitted { message } => {
                                    if let Some(out) = &mut verbose_out {
                                        let content_value = serde_json::to_value(&message.content)?;
                                        out.emit(&UserEvent {
                                            event_type: "user",
                                            message: UserMessage {
                                                role: "user",
                                                content: &content_value,
                                            },
                                            session_id,
                                            parent_tool_use_id,
                                        })?;
                                    }
                                }
                                AgentEvent::TurnOutcome(_) => {
                                    if state.accept_outcome(&envelope) {
                                        break;
                                    }
                                }
                                AgentEvent::ControlComplete { .. } => break,
                                AgentEvent::ControlError { message } => {
                                    state.is_error = true;
                                    state.result_text = message.clone();
                                    break;
                                }
                            }
                        }
                        Ok((
                            PrintResult {
                                result_type: "result",
                                subtype: if state.is_error { "error" } else { "success" },
                                is_error: state.is_error,
                                duration_ms: 0,
                                num_turns: state.num_turns,
                                result: state.result_text,
                                stop_reason: state.stop_reason,
                                session_id: session_id.clone(),
                                // Zero on an unpriced model, which is what its turns reported too.
                                total_cost_usd: cost.unwrap_or_default(),
                                usage: state.usage,
                            },
                            verbose_out,
                        ))
                    })()?;
                    result.duration_ms = start.elapsed().as_millis();
                    match format {
                        OutputFormat::Text => print!("{}", result.result),
                        OutputFormat::Json | OutputFormat::StreamJson => {
                            match verbose_out {
                                Some(VerboseOutput::Json(mut events)) => {
                                    events.push(serde_json::to_value(&result)?);
                                    println!("{}", serde_json::to_string(&events)?);
                                }
                                _ => println!("{}", serde_json::to_string(&result)?),
                            }
                            terminal_result_emitted.set(true);
                        }
                    }
                    if result.is_error {
                        // The error text is already in the emitted result; a detailed
                        // error report here would print it twice.
                        Err(eyre!("agent run failed"))
                    } else {
                        Ok(())
                    }
                },
            )
        })
    })();
    if let (Err(error), true, false) = (
        &outcome,
        matches!(format, OutputFormat::Json | OutputFormat::StreamJson),
        terminal_result_emitted.get(),
    ) {
        let result = PrintResult {
            result_type: "result",
            subtype: "error",
            is_error: true,
            duration_ms: 0,
            num_turns: 0,
            result: error.to_string(),
            stop_reason: None,
            session_id: SessionRef::from_id(MakiId::generate()),
            total_cost_usd: 0.0,
            usage: TokenUsage::default(),
        };
        println!("{}", serde_json::to_string(&result)?);
    }
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use flume::{Receiver, Sender};
    use maki_agent::{
        AgentError, AgentId, SubagentInfo, ToolOutput, TurnCancellationReason, TurnFailure, TurnId,
        tools::{
            DescriptionContext, ExecFuture, HeaderFuture, HeaderResult, ParseError, Tool,
            ToolContext, ToolInvocation, ToolRegistry, ToolSource,
            offload::{OffloadBackend, OffloadError, OffloadStore},
        },
    };
    use maki_providers::{
        ContentBlock, Message, ModelInfo, ProviderEvent, RequestOptions, Role, StopReason,
        StreamResponse, TokenUsage,
        provider::{BoxFuture, Provider},
    };
    use maki_storage::sessions::{SESSIONS_DIR, offload_dir};
    use std::borrow::Cow;
    use std::io::Write;
    use std::sync::atomic::{AtomicBool, Ordering};
    use test_case::test_case;

    const GATE_TIMEOUT: Duration = Duration::from_secs(10);
    const OUTPUT_ERROR: &str = "print output failed";
    const ROOT_RESPONSE: &str = "root response";
    const PRINT_NESTED_TOOL: &str = "print_nested_task";
    const PRINT_PROMPT: &str = "print request";
    const PRINT_RESPONSE: &str = "done";
    const PRINT_SETTLEMENT_TIMEOUT: &str = "print turn did not settle";
    const CHILD_PROMPT: &str = "print child request";
    const GRANDCHILD_PROMPT: &str = "print grandchild request";
    const CHILD_RESPONSE: &str = "print child response";
    const GRANDCHILD_RESPONSE: &str = "print grandchild response";
    const COMMAND_CHILD_PROMPT: &str = "print command child request";

    #[test_case("completed"; "completed")]
    #[test_case("failed"; "failed")]
    #[test_case("cancelled"; "cancelled")]
    fn subagent_outcomes_preserve_print_state_until_root_outcome(status: &str) {
        let child_id = AgentId::generate();
        let turn_id = TurnId::generate();
        let child_usage = TokenUsage {
            input: 10,
            output: 20,
            ..Default::default()
        };
        let outcome = match status {
            "completed" => {
                TurnOutcome::completed(child_id, turn_id, child_usage, 2, DoneReason::MaxTurns)
            }
            "failed" => TurnOutcome::failed(
                child_id,
                turn_id,
                child_usage,
                2,
                TurnFailure::from_agent_error(&AgentError::Config {
                    message: OUTPUT_ERROR.into(),
                }),
            ),
            "cancelled" => TurnOutcome::cancelled(
                child_id,
                turn_id,
                child_usage,
                2,
                TurnCancellationReason::User,
            ),
            _ => unreachable!(),
        };
        let envelope = Envelope {
            event: AgentEvent::TurnOutcome(outcome),
            subagent: Some(SubagentInfo {
                agent_id: child_id,
                parent_agent_id: None,
                parent_is_root: true,
                auto_deliver: true,
                parent_tool_use_id: String::new(),
                name: String::new(),
                prompt: None,
                model: None,
                opts: None,
                answer_tx: None,
                cancel: None,
            }),
            identity: None,
        };
        let mut state = PrintTurnState {
            result_text: ROOT_RESPONSE.into(),
            ..Default::default()
        };
        assert!(!state.accept_outcome(&envelope));
        assert_eq!(state.result_text, ROOT_RESPONSE);
        assert!(!state.is_error);
        assert_eq!(state.num_turns, 0);
        assert_eq!(state.usage, TokenUsage::default());
        assert_eq!(state.stop_reason, None);

        let root_usage = TokenUsage {
            input: 30,
            output: 40,
            ..Default::default()
        };
        assert!(state.accept_outcome(&Envelope {
            event: AgentEvent::TurnOutcome(TurnOutcome::completed(
                AgentId::generate(),
                TurnId::generate(),
                root_usage,
                3,
                DoneReason::EndTurn,
            )),
            subagent: None,
            identity: None,
        }));
        assert_eq!(state.result_text, ROOT_RESPONSE);
        assert!(!state.is_error);
        assert_eq!(state.num_turns, 3);
        assert_eq!(state.usage, root_usage);
        assert_eq!(state.stop_reason, Some(DoneReason::EndTurn));
    }

    struct GatedRemoval {
        persistence: Option<(Sender<()>, Receiver<()>)>,
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
        removed: Arc<AtomicBool>,
    }

    impl OffloadBackend for GatedRemoval {
        fn create_new(&self, _: &str, _: &[u8]) -> io::Result<bool> {
            if let Some((entered, release)) = &self.persistence {
                entered.send(()).unwrap();
                release.recv().unwrap();
            }
            Ok(true)
        }

        fn total_bytes(&self) -> io::Result<u64> {
            Ok(0)
        }

        fn remove_all(&self) -> io::Result<()> {
            self.entered.send(()).unwrap();
            self.release.recv_timeout(GATE_TIMEOUT).unwrap();
            self.removed.store(true, Ordering::Release);
            Ok(())
        }

        fn path(&self, name: &str) -> PathBuf {
            PathBuf::from("/print-offload-test").join(name)
        }
    }

    struct ReleaseRemoval(flume::Sender<()>);

    impl Drop for ReleaseRemoval {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    struct FailingOutput;

    impl Write for FailingOutput {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::Error::other(OUTPUT_ERROR))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn print_cancellation_returns_with_persistence_still_gated() {
        let (writing, writing_rx) = flume::bounded(1);
        let (release_write, write_gate) = flume::bounded(1);
        let release_write = ReleaseRemoval(release_write);
        let (removing, removing_rx) = flume::bounded(1);
        let (release_remove, remove_gate) = flume::bounded(1);
        let release_remove = ReleaseRemoval(release_remove);
        let removed = Arc::new(AtomicBool::new(false));
        let store = Arc::new(OffloadStore::new(Box::new(GatedRemoval {
            persistence: Some((writing, write_gate)),
            entered: removing,
            release: remove_gate,
            removed: Arc::clone(&removed),
        })));
        let writer_store = Arc::clone(&store);
        let writer = std::thread::spawn(move || writer_store.put("pending output"));
        writing_rx.recv_timeout(GATE_TIMEOUT).unwrap();
        let cleanup = OffloadCleanup::new(Arc::clone(&store));
        let observer = cleanup.clone();
        let task = smol::spawn(std::future::pending::<()>());
        let (completed, returned) = flume::bounded(1);
        let thread = std::thread::spawn(move || {
            let result = run_and_settle(
                task,
                Some(cleanup),
                async |_| {},
                || {
                    FailingOutput.write_all(b"result")?;
                    Ok(())
                },
            );
            completed.send(()).unwrap();
            result
        });
        returned.recv_timeout(GATE_TIMEOUT).unwrap();
        let result = thread.join().unwrap();
        assert_eq!(result.unwrap_err().to_string(), OUTPUT_ERROR);
        assert!(matches!(
            store.put("late output"),
            Err(OffloadError::Closed)
        ));
        assert!(removing_rx.try_recv().is_err());
        assert!(!removed.load(Ordering::Acquire));
        drop(release_write);
        writer.join().unwrap().unwrap();
        removing_rx.recv_timeout(GATE_TIMEOUT).unwrap();
        drop(release_remove);
        smol::block_on(observer.wait()).unwrap();
        assert!(removed.load(Ordering::Acquire));
    }

    const PRINT_RESULT_FIELDS: &[&str] = &[
        "type",
        "subtype",
        "is_error",
        "num_turns",
        "result",
        "stop_reason",
        "session_id",
        "total_cost_usd",
        "usage",
        "duration_ms",
    ];
    const INIT_EVENT_FIELDS: &[&str] = &["type", "subtype", "cwd", "session_id", "tools", "model"];
    const RETRY_EVENT_FIELDS: &[&str] = &[
        "type",
        "subtype",
        "attempt",
        "retry_delay_ms",
        "error",
        "session_id",
    ];

    fn input(message: &str) -> AgentInput {
        AgentInput {
            message: message.into(),
            mode: AgentMode::Build,
            images: Vec::new(),
            preamble: Vec::new(),
            thinking: Default::default(),
            fast: false,
            workflow: false,
            prompt: None,
            cancel: None,
        }
    }

    struct RecordingPrintProvider {
        requests: Sender<(String, String, bool)>,
        nested: Option<(Sender<()>, Receiver<()>)>,
    }

    impl maki_providers::provider::Provider for RecordingPrintProvider {
        fn stream_message<'a>(
            &'a self,
            model: &'a Model,
            messages: &'a [maki_providers::Message],
            system: &'a str,
            _: &'a Value,
            events: &'a flume::Sender<maki_providers::ProviderEvent>,
            options: maki_providers::RequestOptions,
            _: Option<&'a SessionRef>,
        ) -> maki_providers::provider::BoxFuture<
            'a,
            Result<maki_providers::StreamResponse, maki_agent::AgentError>,
        > {
            Box::pin(async move {
                self.requests
                    .send((model.spec(), system.to_owned(), options.fast))
                    .unwrap();
                let mut content = ContentBlock::Text {
                    text: PRINT_RESPONSE.into(),
                };
                let mut stop_reason = StopReason::EndTurn;
                assert!(messages.iter().flat_map(|message| &message.content).any(|block| {
                    matches!(block, ContentBlock::Text { text } if [PRINT_PROMPT, CHILD_PROMPT, GRANDCHILD_PROMPT, COMMAND_CHILD_PROMPT].contains(&text.as_str()))
                }));
                let command_child = messages.iter().flat_map(|message| &message.content).any(|block| {
                    matches!(block, ContentBlock::Text { text } if text == COMMAND_CHILD_PROMPT)
                });
                if let Some((entered, release)) = &self.nested
                    && !command_child
                {
                    let prompt = messages
                        .iter()
                        .flat_map(|message| &message.content)
                        .find_map(|block| match block {
                            ContentBlock::Text { text }
                                if [PRINT_PROMPT, CHILD_PROMPT, GRANDCHILD_PROMPT]
                                    .contains(&text.as_str()) =>
                            {
                                Some(text.as_str())
                            }
                            _ => None,
                        })
                        .unwrap();
                    let joined = messages
                        .iter()
                        .flat_map(|message| &message.content)
                        .find_map(|block| match block {
                            ContentBlock::ToolResult {
                                content, is_error, ..
                            } => {
                                assert!(!is_error, "nested task failed: {content}");
                                Some(content.as_str())
                            }
                            _ => None,
                        });
                    let text = if prompt == GRANDCHILD_PROMPT {
                        entered.send_async(()).await.unwrap();
                        release.recv_async().await.unwrap();
                        GRANDCHILD_RESPONSE
                    } else if let Some(joined) = joined {
                        let expected = if prompt == CHILD_PROMPT {
                            GRANDCHILD_RESPONSE
                        } else {
                            CHILD_RESPONSE
                        };
                        assert!(joined.contains(expected), "missing joined text: {joined}");
                        if prompt == CHILD_PROMPT {
                            CHILD_RESPONSE
                        } else {
                            ROOT_RESPONSE
                        }
                    } else {
                        content = ContentBlock::ToolUse {
                            id: PRINT_NESTED_TOOL.into(),
                            name: PRINT_NESTED_TOOL.into(),
                            input: serde_json::json!({ "child": prompt == CHILD_PROMPT }),
                            thought_signature: None,
                        };
                        stop_reason = StopReason::ToolUse;
                        ""
                    };
                    if stop_reason == StopReason::EndTurn {
                        content = ContentBlock::Text { text: text.into() };
                    }
                }
                if let ContentBlock::Text { text } = &content {
                    events
                        .send(ProviderEvent::TextDelta { text: text.clone() })
                        .unwrap();
                }
                Ok(StreamResponse {
                    message: Message {
                        role: Role::Assistant,
                        content: vec![content],
                        ..Default::default()
                    },
                    usage: TokenUsage::default(),
                    stop_reason: Some(stop_reason),
                })
            })
        }

        fn list_models(
            &self,
        ) -> maki_providers::provider::BoxFuture<
            '_,
            Result<Vec<maki_providers::ModelInfo>, maki_agent::AgentError>,
        > {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    struct SettlementTool {
        name: &'static str,
        executed: Sender<()>,
    }

    impl Tool for SettlementTool {
        fn name(&self) -> &str {
            self.name
        }

        fn description(&self, _: &DescriptionContext) -> Cow<'_, str> {
            self.name.into()
        }

        fn schema(&self) -> Value {
            serde_json::json!({ "type": "object", "properties": {} })
        }

        fn parse(&self, _: &Value) -> Result<Box<dyn ToolInvocation>, ParseError> {
            Ok(Box::new(Self {
                name: self.name,
                executed: self.executed.clone(),
            }))
        }
    }

    impl ToolInvocation for SettlementTool {
        fn start_header(&self) -> HeaderFuture {
            HeaderFuture::Ready(HeaderResult::plain(self.name.into()))
        }

        fn execute<'a>(self: Box<Self>, _: &'a ToolContext) -> ExecFuture<'a> {
            Box::pin(async move {
                self.executed.send(()).unwrap();
                Ok(ToolOutput::Plain(String::new().into())).into()
            })
        }
    }

    struct GatedToolProvider {
        name: &'static str,
        polled: Sender<()>,
        release: Receiver<()>,
    }

    impl Provider for GatedToolProvider {
        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            _: &'a [Message],
            _: &'a str,
            _: &'a Value,
            _: &'a Sender<ProviderEvent>,
            _: RequestOptions,
            _: Option<&'a SessionRef>,
        ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
            Box::pin(async move {
                self.polled.send_async(()).await.unwrap();
                self.release.recv_async().await.unwrap();
                Ok(StreamResponse {
                    message: Message {
                        role: Role::Assistant,
                        content: vec![ContentBlock::ToolUse {
                            id: self.name.into(),
                            name: self.name.into(),
                            input: serde_json::json!({}),
                            thought_signature: None,
                        }],
                        ..Default::default()
                    },
                    usage: TokenUsage::default(),
                    stop_reason: Some(StopReason::ToolUse),
                })
            })
        }

        fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    #[test_case(false; "output_error")]
    #[test_case(true; "body_panic")]
    fn print_failure_cancels_pending_headless_before_next_tool(panic: bool) {
        let name = if panic {
            "print_settle_body_panic"
        } else {
            "print_settle_output_error"
        };
        let (executed, execution) = flume::bounded(1);
        ToolRegistry::global()
            .register(
                Arc::new(SettlementTool { name, executed }),
                ToolSource::Lua {
                    plugin: name.into(),
                },
            )
            .unwrap();
        let (polled, polling) = flume::bounded(1);
        let (release, gate) = flume::bounded(1);
        let cwd = std::env::temp_dir();
        let handle = maki_agent::headless::spawn_with_provider(
            HeadlessParams {
                model: Model::from_spec("anthropic/claude-opus-4-8").unwrap(),
                config: AgentConfig::default(),
                permissions_config: PermissionsConfig::default(),
                timeouts: Default::default(),
                input: input(name),
                prompt_slots: Default::default(),
                excluded_tools: Vec::new(),
                mcp_handle: None,
                initial_wd: cwd.clone(),
                system_prompt_override: None,
                append_system_prompt: None,
                model_policy: Arc::default(),
                plugin_rules: Arc::default(),
                project_config: ProjectConfig::for_project(&cwd),
                modes: Arc::default(),
                session_options: Default::default(),
                state_dir: None,
            },
            Arc::new(GatedToolProvider {
                name,
                polled,
                release: gate,
            }),
        )
        .unwrap();
        polling.recv_timeout(GATE_TIMEOUT).unwrap();
        let manager = handle.manager.clone();
        let session_id = handle.session_id.id();
        let result = catch_unwind(AssertUnwindSafe(|| {
            with_print_handle(handle, |_| {
                if panic {
                    std::panic::panic_any(OUTPUT_ERROR);
                }
                FailingOutput.write_all(b"result")?;
                Ok(())
            })
        }));
        if panic {
            assert_eq!(
                result.unwrap_err().downcast_ref::<&str>(),
                Some(&OUTPUT_ERROR)
            );
        } else {
            assert_eq!(result.unwrap().unwrap_err().to_string(), OUTPUT_ERROR);
        }
        assert!(manager.snapshot().is_empty());
        assert!(SessionCoordinatorHandle::resolve(session_id).is_err());
        let _ = release.send(());
        assert!(execution.try_recv().is_err());
    }

    /// The provider's report is a rendezvous, so the run can't reach its
    /// cleanup before the saved output below exists.
    #[test]
    fn print_run_removes_offload_dir() {
        let state = tempfile::tempdir().unwrap();
        let cwd = std::env::temp_dir();
        let (requests, received) = flume::bounded(0);
        let handle = maki_agent::headless::spawn_with_provider(
            HeadlessParams {
                model: Model::from_spec("anthropic/claude-opus-4-8").unwrap(),
                config: AgentConfig::default(),
                permissions_config: PermissionsConfig::default(),
                timeouts: Default::default(),
                input: input("print request"),
                prompt_slots: Default::default(),
                excluded_tools: Vec::new(),
                mcp_handle: None,
                initial_wd: cwd.clone(),
                system_prompt_override: None,
                append_system_prompt: None,
                model_policy: Arc::default(),
                plugin_rules: Arc::default(),
                project_config: ProjectConfig::for_project(&cwd),
                modes: Arc::default(),
                session_options: Default::default(),
                state_dir: Some(state.path().to_path_buf()),
            },
            Arc::new(RecordingPrintProvider {
                requests,
                nested: None,
            }),
        )
        .unwrap();
        let dir = offload_dir(&state.path().join(SESSIONS_DIR), handle.session_id.id());
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("saved.txt"), "output").unwrap();

        received.recv_timeout(GATE_TIMEOUT).unwrap();
        smol::block_on(futures_lite::future::or(handle.task, async {
            smol::Timer::after(GATE_TIMEOUT).await;
            panic!("{PRINT_SETTLEMENT_TIMEOUT}");
        }));
        assert!(!dir.exists(), "print mode must remove its offload dir");
    }

    #[test_case(false, "turn"; "command_turn")]
    #[test_case(true, "turn"; "nested_task")]
    #[test_case(false, "completed"; "completed_command")]
    #[test_case(false, "unknown"; "unknown_command")]
    fn print_turn_uses_initialized_config(nested: bool, outcome: &str) {
        const SPEC: &str = "anthropic/claude-opus-4-8";
        const SYSTEM: &str = "print initialized system";
        const COMMAND: &str = "/print_initialized";
        const COMMAND_FINISHED: &str = "print command finished";
        let registry = CommandRegistry::new();
        let _commands = StandardCommands::register(
            &registry,
            &[command::CustomCommand {
                name: "print_prompt".into(),
                description: String::new(),
                content: PRINT_PROMPT.into(),
                scope: command::CommandScope::Project,
                accepts_args: false,
                argument_hint: None,
            }],
            StandardCompletions::default(),
        )
        .unwrap();
        let state_dir = tempfile::tempdir().unwrap();
        let (requests, received) = flume::unbounded();
        let (entered, entering) = flume::bounded(1);
        let (release, gate) = flume::bounded(1);
        let provider: Arc<dyn Provider> = Arc::new(RecordingPrintProvider {
            requests,
            nested: nested.then_some((entered, gate)),
        });
        let child_provider = Arc::clone(&provider);
        let host = maki_lua::PluginHost::with_session_provider_preparer(
            Arc::clone(ToolRegistry::global_arc()),
            registry.clone(),
            true,
            Arc::new(maki_lua::test_support::InMemoryFs::new()),
            Some(state_dir.path().to_path_buf()),
            Some(Arc::new(move |model| {
                Ok((model, Arc::clone(&child_provider)))
            })),
        )
        .unwrap();
        if nested {
            host.load_source(
                "print_bundled_task",
                include_str!("../plugins/task/init.lua"),
            )
            .unwrap();
            host.load_source(PRINT_NESTED_TOOL, &format!(r#"
                maki.api.register_tool({{
                    name = "{PRINT_NESTED_TOOL}", description = "Exercise nested print tasks",
                    schema = {{ type = "object", properties = {{ child = {{ type = "boolean" }} }} }},
                    audiences = {{ "main", "general_sub" }},
                    handler = function(input, ctx)
                        local agent = assert(maki.agent.current(ctx))
                        local selection = assert(agent:model(ctx))
                        local root_ref = assert(maki.agent.root(ctx))
                        if input.child then
                            assert(selection.spec == "{SPEC}" and selection.fast)
                        else
                            local root = assert(maki.agent.get(ctx, root_ref))
                            assert(root:id() == agent:id())
                            local value, err = root:set_model(ctx, {{ fast = false }})
                            assert(value == nil and err == "{fixed_model}")
                        end
                        local prompt = input.child and "{GRANDCHILD_PROMPT}" or "{CHILD_PROMPT}"
                        local spawned, err = maki.agent.call_tool(ctx, "task_spawn", {{
                            description = prompt, prompt = prompt, subagent_type = "general",
                        }})
                        assert(not err, err)
                        local task = maki.json.decode(spawned)
                        if input.child then
                            local changed = assert(agent:set_model(ctx, {{ fast = false }}))
                            assert(not changed.fast)
                        end
                        local joined, wait_err = maki.agent.call_tool(ctx, "task_wait", {{ task_id = task.task_id, turn_id = task.turn_id }})
                        assert(not wait_err, wait_err)
                        local result = maki.json.decode(joined)
                        local expected = input.child and "{GRANDCHILD_RESPONSE}" or "{CHILD_RESPONSE}"
                        assert(result.result.text == expected, joined)
                        return result.result.text
                    end,
                }})
            "#, fixed_model = maki_lua::agent_errors::FIXED_MODEL_ERROR)).unwrap();
        }
        host.load_source(
            "print_initialized_command",
            &format!(r#"
                maki.api.register_command({{
                    name = "{COMMAND}", tui_only = false,
                    handler = function(_, ctx)
                        local root = assert(maki.agent.current(ctx))
                        assert(root:id() == assert(maki.agent.root(ctx)):id())
                        local selection = assert(root:model(ctx))
                        assert(selection.spec == "{SPEC}" and selection.fast)
                        local changed, err = root:set_model(ctx, {{ fast = false }})
                        assert(changed == nil and err == "{fixed_model}")
                        local child = assert(maki.agent.spawn(ctx, {{
                            inherit_provider = true, silent = true, tools = {{}},
                        }}))
                        assert(assert(child:prompt(ctx, "{COMMAND_CHILD_PROMPT}")).text == "{PRINT_RESPONSE}")
                        assert(child:close(ctx))
                        maki.ui.flash("{COMMAND_FINISHED}")
                    end,
                }})
            "#, fixed_model = maki_lua::agent_errors::FIXED_MODEL_ERROR),
        ).unwrap();
        let mut literal = input(match outcome {
            "unknown" => "/print_unknown",
            "completed" => "/exit",
            _ => "/project:print_prompt",
        });
        literal.fast = true;
        let cwd = std::env::temp_dir();
        let lua_handle = host.event_handle();
        let (binding, installed) = bind_print_root(lua_handle.clone());
        let handle = maki_agent::headless::spawn_with_binding(
            HeadlessParams {
                model: Model::from_spec(SPEC).unwrap(),
                config: AgentConfig {
                    max_concurrent_agent_turns: 1,
                    ..Default::default()
                },
                permissions_config: PermissionsConfig {
                    yolo: true,
                    ..Default::default()
                },
                timeouts: Default::default(),
                input: print_initialization_input(&literal),
                prompt_slots: lua_handle.collect_prompt_slots(),
                excluded_tools: Vec::new(),
                mcp_handle: None,
                initial_wd: cwd.clone(),
                system_prompt_override: Some(SYSTEM.into()),
                append_system_prompt: None,
                model_policy: Arc::default(),
                plugin_rules: Arc::default(),
                project_config: ProjectConfig::for_project(&cwd),
                modes: Arc::default(),
                session_options: lua_handle.session_option_catalog(),
                state_dir: Some(state_dir.path().to_path_buf()),
            },
            Box::new(move |model, _| Box::pin(async move { Ok((model, provider)) })),
            binding,
        )
        .unwrap();
        let manager = handle.manager.clone();
        let session_id = handle.session_id.id();
        let dir = offload_dir(&state_dir.path().join(SESSIONS_DIR), session_id);
        let mut ran = false;
        let result = with_print_handle(handle, |handle| {
            let root = smol::block_on(handle.initialized_root()).unwrap();
            let trusted = installed.recv_timeout(GATE_TIMEOUT).unwrap();
            assert_eq!(trusted.target.id(), root.id());
            assert!(trusted.template.opts.fast);
            let command_host = Arc::new(PrintCommandHost(Some(trusted)));
            let target = registry.bind_target(print_capabilities(), command_host.clone());
            assert!(matches!(
                smol::block_on(command_host.request(HostRequest::Builtin(
                    BuiltinOperation::ChangeDirectory {
                        path: state_dir.path().to_path_buf(),
                    },
                ))),
                Err(CommandError::Producer(_))
            ));
            assert_eq!(root.effective_config().unwrap().unwrap().context.cwd(), cwd);
            host.wait_for_worker_barrier_for_test();
            assert_eq!(manager.snapshot()[0].parent_id, None);
            assert_eq!(manager.limits().max_concurrent_agent_turns, 1);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("saved.txt"), "output").unwrap();
            if outcome == "turn" {
                let dispatch = smol::block_on(registry.dispatch_input(&target, COMMAND.into()));
                assert!(
                    matches!(
                        dispatch,
                        InputDispatch::Dispatched(CommandOutcome::Completed)
                    ),
                    "{dispatch:?}"
                );
                let action = host.ui_action_rx().recv_timeout(GATE_TIMEOUT).unwrap();
                assert!(
                    matches!(action, maki_lua::UiAction::Flash(ref text) if text == COMMAND_FINISHED),
                    "initialized command did not finish"
                );
            }
            drive_print(
                &registry,
                &target,
                &CommandTurnMarker,
                literal,
                |input: AgentInput| {
                    ran = true;
                    assert_eq!(input.message, PRINT_PROMPT);
                    let ticket = smol::block_on(handle.start_with_input(input)).unwrap();
                    let child_actor = if nested {
                        entering.recv_timeout(GATE_TIMEOUT).unwrap();
                        let graph = manager.snapshot();
                        assert_eq!(graph.len(), 3);
                        let child = graph
                            .iter()
                            .find(|node| node.parent_id == Some(root.id()))
                            .unwrap();
                        let grandchild = graph
                            .iter()
                            .find(|node| node.parent_id == Some(child.agent_id))
                            .unwrap();
                        assert_eq!((child.depth, grandchild.depth), (1, 2));
                        assert_eq!(grandchild.root_id, root.id());
                        let actor = manager.lookup(child.agent_id).unwrap().actor().unwrap();
                        assert!(root.effective_config().unwrap().unwrap().settings.fast);
                        release.send(()).unwrap();
                        Some(actor)
                    } else {
                        None
                    };
                    let events = handle.event_rx.clone();
                    let state = smol::block_on(futures_lite::future::or(
                        async {
                            let mut state = PrintTurnState::default();
                            let mut outcomes = 0;
                            while let Ok(envelope) = events.recv_async().await {
                                if matches!(envelope.event, AgentEvent::StreamClosed) {
                                    break;
                                }
                                if let AgentEvent::TextDelta { text } = &envelope.event
                                    && envelope.subagent.is_none()
                                {
                                    state.result_text.push_str(text);
                                }
                                outcomes += usize::from(state.accept_outcome(&envelope));
                            }
                            assert_eq!(outcomes, 1);
                            Ok(state)
                        },
                        async {
                            smol::Timer::after(GATE_TIMEOUT).await;
                            Err(eyre!(PRINT_SETTLEMENT_TIMEOUT))
                        },
                    ))?;
                    assert_eq!(
                        state.result_text,
                        if nested {
                            ROOT_RESPONSE
                        } else {
                            PRINT_RESPONSE
                        }
                    );
                    assert!(matches!(
                        smol::block_on(ticket.wait()),
                        TurnOutcome::Completed { .. }
                    ));
                    if let Some(child) = child_actor {
                        assert!(!child.effective_config().unwrap().settings.fast);
                    }
                    Ok(())
                },
            )
        });
        assert_eq!(ran, outcome == "turn", "{result:?}");
        if outcome == "unknown" {
            assert!(result.is_err());
        } else {
            result.unwrap();
        }
        assert!(manager.snapshot().is_empty());
        assert!(SessionCoordinatorHandle::resolve(session_id).is_err());
        assert!(!dir.exists());
        let requests = received.try_iter().collect::<Vec<_>>();
        if outcome != "turn" {
            assert!(requests.is_empty());
            return;
        }
        assert_eq!(requests[0], (SPEC.into(), String::new(), true));
        assert_eq!(requests[1], (SPEC.into(), SYSTEM.into(), true));
        if nested {
            assert_eq!(requests.len(), 6);
            assert!(requests.iter().all(|(spec, _, _)| spec == SPEC));
            assert!(requests[2].2);
            assert!(requests[3].2);
            assert_eq!(requests[5], (SPEC.into(), SYSTEM.into(), true));
        } else {
            assert_eq!(requests.len(), 2);
        }
    }

    fn target(registry: &CommandRegistry) -> maki_commands::TargetHandle {
        registry.bind_target(print_capabilities(), Arc::new(PrintCommandHost::default()))
    }

    struct ReplaceAttachment;

    impl maki_commands::CommandBehavior for ReplaceAttachment {
        fn execute(
            &self,
            invocation: maki_commands::CommandInvocation,
        ) -> CommandFuture<Result<CommandOutcome, CommandError>> {
            Box::pin(async move {
                let Some(attachment) = invocation.content.attachments.first() else {
                    return Err(CommandError::Producer(Arc::from(
                        "missing command attachment",
                    )));
                };
                if attachment.media_type.as_ref() != "image/png"
                    || attachment.data.as_ref() != "AAAA"
                {
                    return Err(CommandError::Producer(Arc::from(
                        "command attachment changed before dispatch",
                    )));
                }
                Ok(CommandOutcome::AgentTurn(maki_commands::AgentTurn {
                    content: CommandContent {
                        text: Arc::from("inspected"),
                        attachments: Arc::from([maki_commands::CommandAttachment {
                            media_type: Arc::from("image/jpeg"),
                            data: Arc::from("BBBB"),
                        }]),
                    },
                    prompt: None,
                }))
            })
        }
    }

    #[test]
    fn command_behavior_owns_attachment_policy() {
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(maki_commands::ProducerPrecedence::Plugin);
        producer
            .replace(vec![maki_commands::Registration {
                spec: maki_commands::CommandSpec {
                    name: Arc::from("/inspect"),
                    aliases: Arc::from([]),
                    arguments: maki_commands::CommandArguments::Positional(Arc::from([])),
                    docs: maki_commands::CommandDocs {
                        summary: Arc::from("inspect"),
                        argument_hint: None,
                    },
                    required_capabilities: TargetCapabilities::from_capability(
                        TargetCapability::AgentTurns,
                    ),
                },
                behavior: Arc::new(ReplaceAttachment),
                argument_completions: Vec::new(),
            }])
            .unwrap();
        let target = target(&registry);
        let sink = CommandTurnMarker;
        let mut literal = input("/inspect");
        literal.images.push(ImageSource::new(
            maki_agent::ImageMediaType::Png,
            Arc::from("AAAA"),
        ));
        let mut received = None;

        drive_print(&registry, &target, &sink, literal, |input| {
            received = Some(input);
            Ok(())
        })
        .unwrap();

        let received = received.unwrap();
        assert_eq!(received.message, "inspected");
        assert_eq!(received.images.len(), 1);
        assert_eq!(
            received.images[0].media_type,
            maki_agent::ImageMediaType::Jpeg
        );
        assert_eq!(received.images[0].data.as_ref(), "BBBB");
    }

    #[test]
    fn quick_question_runs_as_agent_turn() {
        let registry = CommandRegistry::new();
        let _commands =
            StandardCommands::register(&registry, &[], StandardCompletions::default()).unwrap();
        let target = target(&registry);
        let sink = CommandTurnMarker;
        let mut literal = input("/btw explain this");
        literal.images.push(ImageSource::new(
            maki_agent::ImageMediaType::Png,
            Arc::from("AAAA"),
        ));
        let mut received = None;

        drive_print(&registry, &target, &sink, literal, |input| {
            received = Some(input);
            Ok(())
        })
        .unwrap();

        let received = received.unwrap();
        assert_eq!(received.message, "explain this");
        assert_eq!(received.images.len(), 1);
        assert_eq!(
            received.images[0].media_type,
            maki_agent::ImageMediaType::Png
        );
        assert_eq!(received.images[0].data.as_ref(), "AAAA");
    }

    #[test]
    fn completed_command_skips_runner() {
        struct Completed;
        impl maki_commands::CommandBehavior for Completed {
            fn execute(
                &self,
                _invocation: maki_commands::CommandInvocation,
            ) -> CommandFuture<Result<CommandOutcome, CommandError>> {
                Box::pin(async { Ok(CommandOutcome::Completed) })
            }
        }
        let registry = CommandRegistry::new();
        registry
            .create_producer(maki_commands::ProducerPrecedence::Plugin)
            .replace(vec![maki_commands::Registration {
                spec: maki_commands::CommandSpec {
                    name: Arc::from("/done"),
                    aliases: Arc::from([]),
                    arguments: maki_commands::CommandArguments::Positional(Arc::from([])),
                    docs: maki_commands::CommandDocs {
                        summary: Arc::from("done"),
                        argument_hint: None,
                    },
                    required_capabilities: TargetCapabilities::default(),
                },
                behavior: Arc::new(Completed),
                argument_completions: Vec::new(),
            }])
            .unwrap();
        let target = target(&registry);
        let sink = CommandTurnMarker;
        let mut ran = false;
        drive_print(&registry, &target, &sink, input("/done"), |_| {
            ran = true;
            Ok(())
        })
        .unwrap();
        assert!(!ran);
    }

    #[test]
    fn unknown_command_rejects_and_skips_runner() {
        let registry = CommandRegistry::new();
        let target = target(&registry);
        let sink = CommandTurnMarker;
        let mut literal = input("/unknown literal");
        literal.images.push(ImageSource::new(
            maki_agent::ImageMediaType::Webp,
            Arc::from("AAAA"),
        ));
        let mut ran = false;
        let _ = drive_print(&registry, &target, &sink, literal, |_input: AgentInput| {
            ran = true;
            Ok(())
        })
        .expect_err("unknown slash command must be rejected");
        assert!(!ran);
    }

    #[test_case("//unknown literal", "/unknown literal"; "escaped literal strips one slash")]
    #[test_case("///unknown literal", "//unknown literal"; "triple slash strips one slash")]
    fn escaped_slash_runs_literal_input(message: &str, expected: &str) {
        let registry = CommandRegistry::new();
        let target = target(&registry);
        let sink = CommandTurnMarker;
        let mut literal = input(message);
        literal.images.push(ImageSource::new(
            maki_agent::ImageMediaType::Webp,
            Arc::from("AAAA"),
        ));
        let mut received = None;
        drive_print(&registry, &target, &sink, literal, |input: AgentInput| {
            received = Some(input);
            Ok(())
        })
        .unwrap();
        let received = received.unwrap();
        assert_eq!(received.message, expected);
        assert_eq!(received.images.len(), 1);
        assert_eq!(
            received.images[0].media_type,
            maki_agent::ImageMediaType::Webp
        );
        assert_eq!(received.images[0].data.as_ref(), "AAAA");
    }

    #[test]
    fn wire_format_required_fields() {
        let result = PrintResult {
            result_type: "result",
            subtype: "success",
            is_error: false,
            duration_ms: 1234,
            num_turns: 2,
            result: "done".into(),
            stop_reason: Some(DoneReason::EndTurn),
            session_id: SessionRef::generate(),
            total_cost_usd: 0.003,
            usage: TokenUsage::default(),
        };
        let json: Value = serde_json::to_value(&result).unwrap();
        for field in PRINT_RESULT_FIELDS {
            assert!(json.get(field).is_some(), "PrintResult missing: {field}");
        }

        let sid = SessionRef::generate();
        let init = InitEvent {
            event_type: "system",
            subtype: "init",
            cwd: "/tmp",
            session_id: &sid,
            tools: &["bash".into(), "read".into()],
            model: "test-model",
        };
        let json: Value = serde_json::to_value(&init).unwrap();
        for field in INIT_EVENT_FIELDS {
            assert!(json.get(field).is_some(), "InitEvent missing: {field}");
        }

        let retry = RetryEvent {
            event_type: "system",
            subtype: "api_retry",
            attempt: 2,
            retry_delay_ms: 3000,
            error: "rate_limit",
            session_id: &sid,
        };
        let json: Value = serde_json::to_value(&retry).unwrap();
        for field in RETRY_EVENT_FIELDS {
            assert!(json.get(field).is_some(), "RetryEvent missing: {field}");
        }
    }
}
