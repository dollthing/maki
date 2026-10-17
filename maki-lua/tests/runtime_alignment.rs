use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use maki_agent::{
    ActorBackend, AgentInput, AgentManagerHandle, AgentMode, BackendResult, ControlWork,
    DoneReason, History, RunSettings, ToolOutput, TurnContext, TurnOutcome, WorkKind,
    actor::{ActorEvent, DeliveryStatus, EffectiveAgentConfig, graph_quiescence_barrier},
    cancel::CancelTrigger,
    tools::{ToolContext, ToolRegistry, TurnToolBindings},
};
use maki_commands::CommandInvocation;
use maki_lua::{
    PluginHost,
    orchestration::{OrchestrationServices, TrustedTarget},
};
use maki_providers::{
    AgentError, ContentBlock, Message, Model, ModelInfo, ProviderEvent, RequestOptions,
    StreamResponse,
    provider::{BoxFuture, Provider},
};
use maki_storage::id::{MakiId, SessionRef};
use serde_json::{Value, json};

mod common;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const INITIAL: &str = "alignment initial";
const STEERING: &str = "alignment steering";
const REPLY: &str = "alignment complete";
const PROMPTS: [&str; 3] = ["alignment A", "alignment B", "alignment C"];
const TASK_SPAWN: &str = "task_spawn";
const TASK_SEND: &str = "task_send";
const TASK_WAIT: &str = "task_wait";
const TASK_DESPAWN: &str = "task_despawn";
const PRELUDE: &str = r#"
local handlers = {}
local register_tool = maki.api.register_tool
maki.api.register_tool = function(spec)
    handlers[spec.name] = spec.handler
    return register_tool(spec)
end
maki.api.register_command({
    name = "/alignment", tui_only = false,
    arguments = { raw = true, required = true },
    handler = function(opts, ctx)
        local call = maki.json.decode(opts.args)
        local output = handlers[call.name](call.input, ctx)
        assert(not output.is_error, output.llm_output)
        alignment_result = output.llm_output
    end,
})
maki.api.register_tool({
    name = "alignment_result", description = "read command result",
    schema = { type = "object", properties = {} },
    handler = function() return alignment_result end,
})
local real_spawn = maki.agent.spawn
maki.agent.spawn = function(ctx, opts)
    opts.inherit_provider = true
    return real_spawn(ctx, opts)
end
maki.agent.resolve_model = function()
    return { spec = "anthropic/claude-sonnet-4-20250514" }
end
maki.agent.system_prompt = function() return "alignment system" end
maki.agent.tools = function()
    return {
        { name = "task_spawn", description = "spawn", input_schema = { type = "object" } },
        { name = "task_wait", description = "wait", input_schema = { type = "object" } },
    }
end
"#;

struct Request {
    messages: Vec<Message>,
    reply: flume::Sender<Result<StreamResponse, AgentError>>,
}

impl Request {
    fn respond(self, mut response: StreamResponse) {
        for block in &mut response.message.content {
            if let ContentBlock::ToolUse { id, .. } = block {
                *id = MakiId::generate().to_string();
            }
        }
        self.reply.send(Ok(response)).unwrap();
    }

    fn contains(&self, text: &str) -> bool {
        self.messages.iter().any(|message| {
            message.content.iter().any(
                |block| matches!(block, ContentBlock::Text { text: value } if value.contains(text)),
            )
        })
    }

    fn last_tool_result(&self) -> Value {
        self.messages
            .iter()
            .rev()
            .flat_map(|message| &message.content)
            .find_map(|block| match block {
                ContentBlock::ToolResult { content, .. } => Some(
                    serde_json::from_str(content)
                        .unwrap_or_else(|error| panic!("invalid tool result {content:?}: {error}")),
                ),
                _ => None,
            })
            .expect("provider request must contain the completed tool result")
    }
}

struct GatedProvider {
    requests: flume::Sender<Request>,
}

impl Provider for GatedProvider {
    fn stream_message<'a>(
        &'a self,
        _: &'a Model,
        messages: &'a [Message],
        _: &'a str,
        _: &'a Value,
        _: &'a flume::Sender<ProviderEvent>,
        _: RequestOptions,
        _: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            let (reply, response) = flume::bounded(1);
            self.requests
                .send(Request {
                    messages: messages.to_vec(),
                    reply,
                })
                .unwrap();
            common::bounded(response.recv_async()).await.unwrap()
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

struct Targets(Mutex<Vec<TrustedTarget>>);

impl OrchestrationServices for Targets {
    fn resolve_target(
        &self,
        _: Option<&CommandInvocation>,
    ) -> Result<Option<TrustedTarget>, String> {
        Ok(self.0.lock().unwrap().first().cloned())
    }

    fn register_target(&self, target: TrustedTarget) -> Result<(), String> {
        self.0.lock().unwrap().push(target);
        Ok(())
    }

    fn template(&self, target: &maki_agent::AgentRef) -> Result<ToolContext, String> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .find(|entry| {
                entry.target.id() == target.id()
                    && entry.target.manager().same_manager(&target.manager())
            })
            .map(|entry| entry.template.clone())
            .ok_or_else(|| "unregistered alignment target".into())
    }
}

struct Fixture {
    _host: PluginHost,
    _cancel: CancelTrigger,
    _release: flume::Sender<()>,
    manager: AgentManagerHandle,
    registry: Arc<ToolRegistry>,
    context: ToolContext,
    requests: flume::Receiver<Request>,
    pending_requests: Mutex<Vec<Request>>,
}

impl Fixture {
    fn new() -> Self {
        let registry = Arc::new(ToolRegistry::new());
        let host = PluginHost::new(Arc::clone(&registry)).unwrap();
        host.load_source_with_opts(
            "runtime_alignment",
            &format!("{PRELUDE}\n{}", include_str!("../../plugins/task/init.lua")),
            serde_json::Map::from_iter([("max_concurrent".into(), json!(1))]),
        )
        .unwrap();
        let (requests_tx, requests) = flume::unbounded();
        let (mut context, _events, cancel) = common::ctx_with_provider(Arc::new(GatedProvider {
            requests: requests_tx,
        }));
        context.tool_use_id = None;
        context.registry = Arc::clone(&registry);
        context.turn_bindings = Arc::new(TurnToolBindings::capture(
            &registry,
            &context.local_tools,
            None,
        ));
        let manager = AgentManagerHandle::new(Default::default()).unwrap();
        let (spawned, ready) = flume::unbounded();
        let (release, release_rx) = flume::unbounded();
        let held_context = Arc::new(Mutex::new(None));
        let root = manager
            .create_root_with_config(Some(policy(&context)), Vec::new(), None, |_| {
                Ok::<_, String>(Box::new(ManagedDriver {
                    context: context.clone(),
                    registry: Arc::clone(&registry),
                    spawned,
                    release: release_rx,
                    started: true,
                    held_context: Some(Arc::clone(&held_context)),
                }) as Box<dyn ActorBackend>)
            })
            .unwrap();
        host.event_handle()
            .install_orchestration_services(Arc::new(Targets(Mutex::new(vec![TrustedTarget {
                target: root.clone(),
                template: context.clone(),
            }]))));
        host.wait_for_worker_barrier_for_test();
        root.actor()
            .unwrap()
            .admit_turn(ordinary_input(INITIAL), None, INITIAL.into())
            .unwrap();
        ready.recv_timeout(SHUTDOWN_TIMEOUT).unwrap();
        context = held_context.lock().unwrap().take().unwrap();
        Self {
            _host: host,
            _cancel: cancel,
            _release: release,
            manager,
            registry,
            context,
            requests,
            pending_requests: Mutex::new(Vec::new()),
        }
    }

    fn tool(&self, name: &str, input: Value) -> Value {
        common::exec_tool(&self.registry, &self.context, name, input).unwrap()
    }

    fn spawn(&self, prompt: &str) -> Value {
        self.tool(TASK_SPAWN, spawn_input(prompt))
    }

    async fn request(&self) -> Request {
        common::bounded(self.requests.recv_async()).await.unwrap()
    }

    async fn request_for(&self, prompt: &str) -> Request {
        {
            let mut pending = self.pending_requests.lock().unwrap();
            if let Some(index) = pending.iter().position(|request| request.contains(prompt)) {
                return pending.remove(index);
            }
        }
        loop {
            let request = self.request().await;
            if request.contains(prompt) {
                return request;
            }
            self.pending_requests.lock().unwrap().push(request);
        }
    }

    fn close(&self, task: &Value) {
        assert_eq!(
            self.tool(TASK_DESPAWN, json!({ "task_id": task["task_id"] }))["ok"],
            true
        );
    }
}

fn policy(context: &ToolContext) -> EffectiveAgentConfig {
    EffectiveAgentConfig::new(
        RunSettings {
            provider: Arc::clone(&context.provider),
            model: (*context.model).clone(),
            fast: false,
            workflow: false,
            thinking: Default::default(),
        },
        context.mode.clone(),
    )
    .with_mode_def(Some(context.modes.current(&context.mode)))
}

fn spawn_input(prompt: &str) -> Value {
    json!({ "description": "alignment", "prompt": prompt, "subagent_type": "general" })
}

fn wait_input(task: &Value) -> Value {
    json!({ "task_id": task["task_id"], "turn_id": task["turn_id"] })
}

fn decode_output(output: ToolOutput) -> Value {
    let (ToolOutput::Plain(output) | ToolOutput::Markdown(output)) = output else {
        panic!("unexpected task output")
    };
    serde_json::from_str(&output.text).unwrap()
}

#[test]
fn runtime_gate_controls_real_transition() {
    smol::block_on(async {
        let fixture = Fixture::new();
        let task = fixture.spawn(INITIAL);
        let request = fixture.request().await;
        assert!(request.contains(INITIAL));
        assert!(!request.contains(STEERING));
        let input = wait_input(&task);
        let invocation = fixture
            .registry
            .get(TASK_WAIT)
            .unwrap()
            .tool
            .parse(&input)
            .unwrap();
        let context = common::with_request_tools(&fixture.registry, &fixture.context);
        let wait = invocation.execute(&context);
        futures_lite::pin!(wait);
        assert!(futures_lite::future::poll_once(&mut wait).await.is_none());

        let admitted = fixture.tool(
            TASK_SEND,
            json!({ "task_id": task["task_id"], "message": STEERING }),
        );
        assert_eq!(admitted["turn_id"], task["turn_id"]);
        assert!(fixture.requests.is_empty());
        request.respond(common::canned_reply("before steering"));

        let request = fixture.request().await;
        assert!(request.contains(INITIAL));
        assert!(request.contains(STEERING));
        assert!(futures_lite::future::poll_once(&mut wait).await.is_none());
        request.respond(common::canned_reply(REPLY));
        let result = decode_output(common::bounded(wait).await.output.unwrap());
        assert_eq!(result["status"], "done");
        assert_eq!(result["result"]["turn_id"], task["turn_id"]);
        assert_eq!(result["result"]["text"], REPLY);

        let mut successor = fixture.tool(
            TASK_SEND,
            json!({ "task_id": task["task_id"], "message": INITIAL }),
        );
        successor["task_id"] = task["task_id"].clone();
        assert_ne!(successor["turn_id"], task["turn_id"]);
        fixture.request().await.respond(common::canned_reply(REPLY));
        assert_eq!(
            fixture.tool(TASK_WAIT, wait_input(&successor))["status"],
            "done"
        );
        fixture.close(&task);
    });
}

struct ManagedDriver {
    context: ToolContext,
    registry: Arc<ToolRegistry>,
    spawned: flume::Sender<Value>,
    release: flume::Receiver<()>,
    started: bool,
    held_context: Option<Arc<Mutex<Option<ToolContext>>>>,
}

impl ActorBackend for ManagedDriver {
    fn run_turn<'a>(
        &'a mut self,
        _: &'a mut History,
        context: TurnContext,
        _: AgentInput,
        _: WorkKind,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            if let Some(config) = &context.policy {
                self.context.provider = Arc::clone(&config.provider);
                self.context.model = Arc::new(config.model.clone());
                self.context.mode = config.mode.clone();
                self.context.mode_def = config.mode_def.clone().map(Arc::new);
                self.context.opts.thinking = config.thinking;
                self.context.opts.fast = config.fast;
                self.context.workflow = config.workflow;
                self.context.cwd = config.context.cwd().to_path_buf();
            }
            if let Some(held) = &self.held_context {
                let mut tools = self.context.clone();
                tools.execution_agent_id = Some(context.agent_id);
                tools.managed_turn = context.managed_turn.clone();
                tools.cancel = context.cancel.clone();
                *held.lock().unwrap() = Some(tools);
                self.spawned.send(json!(true)).unwrap();
                let _ = self.release.recv_async().await;
            }
            if self.started {
                if let Some(inbox) = &context.inbox {
                    inbox.commit_completions(&mut |_| {});
                    inbox.instructions_submitted();
                    assert!(inbox.finish());
                }
                return BackendResult::EnteredRun(TurnOutcome::completed(
                    context.agent_id,
                    context.turn_id.unwrap(),
                    Default::default(),
                    0,
                    DoneReason::EndTurn,
                ));
            }
            self.started = true;
            self.context.tool_use_id = None;
            self.context.execution_agent_id = Some(context.agent_id);
            self.context.managed_turn = context.managed_turn;
            self.context.cancel = context.cancel;
            let context_tools = common::with_request_tools(&self.registry, &self.context);
            let input = spawn_input(PROMPTS[0]);
            let invocation = self
                .registry
                .get(TASK_SPAWN)
                .unwrap()
                .tool
                .parse(&input)
                .unwrap();
            let spawned = decode_output(invocation.execute(&context_tools).await.output.unwrap());
            assert!(spawned["task_id"].is_string(), "spawn failed: {spawned}");
            self.spawned.send(spawned).unwrap();
            self.release.recv_async().await.unwrap();
            if let Some(inbox) = &context.inbox {
                inbox.commit_completions(&mut |_| {});
                inbox.instructions_submitted();
                assert!(inbox.finish());
            }
            BackendResult::EnteredRun(TurnOutcome::completed(
                context.agent_id,
                context.turn_id.unwrap(),
                Default::default(),
                1,
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
        Box::pin(async { BackendResult::ControlDone })
    }

    fn run_compact<'a>(
        &'a mut self,
        _: &'a mut History,
        _: TurnContext,
        _: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async { BackendResult::CompactDone })
    }
}

fn ordinary_input(message: &str) -> AgentInput {
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
        lease_committer: None,
    }
}

struct ActorProbe {
    actor: maki_agent::AgentActorHandle,
    events: flume::Receiver<ActorEvent>,
    observed: Vec<ActorEvent>,
}

impl ActorProbe {
    fn child(manager: &AgentManagerHandle, parent: maki_agent::AgentId) -> Self {
        let node = manager
            .snapshot()
            .into_iter()
            .find(|node| node.parent_id == Some(parent))
            .unwrap_or_else(|| panic!("missing child of {parent}: {:?}", manager.snapshot()));
        let actor = manager.actor(node.agent_id).unwrap();
        let events = actor.subscribe_events();
        Self {
            actor,
            events,
            observed: Vec::new(),
        }
    }

    async fn idle_after_end(&mut self) {
        common::bounded(async {
            let mut ended = false;
            loop {
                let event = self.events.recv_async().await.unwrap();
                ended |= matches!(event, ActorEvent::End(_));
                let idle = ended && matches!(event, ActorEvent::Idle { .. });
                self.observed.push(event);
                if idle {
                    break;
                }
            }
        })
        .await;
        assert!(self.actor.latest_ticket().unwrap().peek_result().is_some());
        assert_eq!(self.actor.snapshot().queued, 0);
    }

    fn executed_deliveries(&mut self) -> usize {
        self.observed.extend(self.events.try_iter());
        let accepted = self.observed.iter().filter(|event| matches!(event,
            ActorEvent::Delivery(delivery) if matches!(delivery.status, DeliveryStatus::Accepted)
        )).count();
        let executed = self.observed.iter().filter(|event| matches!(event,
            ActorEvent::Delivery(delivery) if matches!(delivery.status, DeliveryStatus::Executed { .. })
        )).count();
        assert_eq!(
            accepted, executed,
            "every accepted automatic report must execute"
        );
        executed
    }
}

async fn graph_probe(probes: &mut [&mut ActorProbe; 3]) -> [usize; 3] {
    let actors: Vec<_> = probes.iter().map(|probe| probe.actor.clone()).collect();
    let drained = common::bounded(graph_quiescence_barrier(&actors))
        .await
        .unwrap();
    assert!(drained.iter().all(|probe| probe.is_quiescent()));
    std::array::from_fn(|index| probes[index].executed_deliveries())
}

#[test]
fn nested_continuations_report_once_and_quiesce() {
    automatic_graph_continuations(false);
}

#[test]
fn graph_quiescence_probe_detects_extra_continuation() {
    automatic_graph_continuations(true);
}

fn automatic_graph_continuations(extra_continuation: bool) {
    smol::block_on(async {
        let fixture = Fixture::new();
        let manager = AgentManagerHandle::new(Default::default()).unwrap();
        let policy = policy(&fixture.context);
        let (spawned_tx, spawned) = flume::bounded(1);
        let (release, release_rx) = flume::bounded(1);
        let root = manager
            .create_root_with_config(Some(policy), Vec::new(), None, |_| {
                Ok::<_, String>(Box::new(ManagedDriver {
                    context: fixture.context.clone(),
                    registry: Arc::clone(&fixture.registry),
                    spawned: spawned_tx,
                    release: release_rx,
                    started: false,
                    held_context: None,
                }) as Box<dyn ActorBackend>)
            })
            .unwrap();
        let driver = root
            .actor()
            .unwrap()
            .admit_turn(ordinary_input(INITIAL), None, INITIAL.into())
            .unwrap();
        common::bounded(spawned.recv_async()).await.unwrap();
        let mut a = ActorProbe::child(&manager, root.id());
        let request = fixture.request_for(PROMPTS[0]).await;
        assert!(request.contains(PROMPTS[0]));
        request.respond(common::canned_tool_use(TASK_SPAWN, spawn_input(PROMPTS[1])));
        let request = fixture.request_for(PROMPTS[0]).await;
        assert!(request.contains(PROMPTS[0]));
        assert!(
            request.last_tool_result()["task_id"].is_string(),
            "nested spawn failed: {}",
            request.last_tool_result()
        );
        let mut b = ActorProbe::child(&manager, a.actor.agent_id());
        let original_b = b
            .actor
            .turn_ticket(
                serde_json::from_value(request.last_tool_result()["turn_id"].clone()).unwrap(),
            )
            .unwrap();
        request.respond(common::canned_reply(REPLY));
        let request = fixture.request_for(PROMPTS[1]).await;
        assert!(request.contains(PROMPTS[1]));
        request.respond(common::canned_tool_use(TASK_SPAWN, spawn_input(PROMPTS[2])));
        let request = fixture.request_for(PROMPTS[1]).await;
        assert!(request.contains(PROMPTS[1]));
        assert!(
            request.last_tool_result()["task_id"].is_string(),
            "nested spawn failed: {}",
            request.last_tool_result()
        );
        let mut c = ActorProbe::child(&manager, b.actor.agent_id());
        let blocked_b = request;
        let leaf = fixture.request_for(PROMPTS[2]).await;
        assert!(leaf.contains(PROMPTS[2]));

        // End B's original ticket without closing C, so C must wake an idle B.
        assert!(original_b.peek().is_none());
        assert_eq!(
            b.actor.latest_ticket().unwrap().turn_id(),
            original_b.turn_id()
        );
        b.actor.cancel_local(0);
        b.idle_after_end().await;
        drop(blocked_b);
        assert!(matches!(
            original_b.peek(),
            Some(TurnOutcome::Cancelled { .. })
        ));
        let request = fixture.request_for(PROMPTS[0]).await;
        assert!(request.contains(PROMPTS[0]));
        request.respond(common::canned_reply(REPLY));
        a.idle_after_end().await;
        let original_a = a.actor.latest_ticket().unwrap();

        leaf.respond(common::canned_reply(REPLY));
        c.idle_after_end().await;
        let request = fixture.request_for(PROMPTS[1]).await;
        assert!(request.contains(PROMPTS[1]));
        assert!(request.messages.iter().any(Message::is_observation));
        request.respond(common::canned_reply(REPLY));
        b.idle_after_end().await;
        let request = fixture.request_for(PROMPTS[0]).await;
        assert!(request.contains(PROMPTS[0]));
        assert!(request.messages.iter().any(Message::is_observation));
        request.respond(common::canned_reply(REPLY));
        a.idle_after_end().await;
        assert_ne!(
            a.actor.latest_ticket().unwrap().turn_id(),
            original_a.turn_id()
        );
        assert_ne!(
            b.actor.latest_ticket().unwrap().turn_id(),
            original_b.turn_id()
        );
        assert!(matches!(
            original_b.peek(),
            Some(TurnOutcome::Cancelled { .. })
        ));
        let baseline = graph_probe(&mut [&mut c, &mut b, &mut a]).await;
        assert_eq!(baseline, [0, 1, 2]);
        if extra_continuation {
            let leaf_turn = c
                .actor
                .admit_turn(ordinary_input(STEERING), None, STEERING.into())
                .unwrap();
            let request = fixture.request().await;
            assert!(request.contains(PROMPTS[2]));
            request.respond(common::canned_reply(REPLY));
            c.idle_after_end().await;
            common::bounded(leaf_turn.wait()).await;
            for probe in [&mut b, &mut a] {
                let request = fixture.request().await;
                assert!(request.messages.iter().any(Message::is_observation));
                request.respond(common::canned_reply(REPLY));
                probe.idle_after_end().await;
            }
            let extra = graph_probe(&mut [&mut c, &mut b, &mut a]).await;
            assert_ne!(
                extra, baseline,
                "probe must detect the extra upward continuation"
            );
            assert_eq!(extra, [0, 2, 3]);
        }
        release.send(()).unwrap();
        common::bounded(driver.wait()).await;
        let drained = common::bounded(manager.drain_graph_for_test())
            .await
            .unwrap();
        assert_eq!(drained.len(), 4);
        assert!(drained.iter().all(|(_, probe)| probe.is_quiescent()));
        assert_eq!(
            graph_probe(&mut [&mut c, &mut b, &mut a]).await,
            if extra_continuation {
                [0, 2, 3]
            } else {
                baseline
            }
        );
        assert!(fixture.requests.is_empty());
        assert!(
            manager
                .shutdown(SHUTDOWN_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}

enum SettledState {
    Done,
    Cancelled,
    Failed,
}

fn task_send_after_settlement(state: SettledState) {
    smol::block_on(async {
        let fixture = Fixture::new();
        let task = fixture.spawn(INITIAL);
        let request = fixture.request().await;
        let root = fixture.manager.root().unwrap();
        let actor = ActorProbe::child(&fixture.manager, root.id()).actor;
        let original = actor.latest_ticket().unwrap();
        match state {
            SettledState::Done => request.respond(common::canned_reply(REPLY)),
            SettledState::Cancelled => {
                actor.cancel_local(0);
                common::bounded(original.wait_result()).await;
                drop(request);
            }
            SettledState::Failed => request
                .reply
                .send(Err(AgentError::Config {
                    message: "alignment provider failure".into(),
                }))
                .unwrap(),
        }
        let result = common::bounded(original.wait_result()).await;
        assert!(match state {
            SettledState::Done => matches!(result.outcome, TurnOutcome::Completed { .. }),
            SettledState::Cancelled => matches!(result.outcome, TurnOutcome::Cancelled { .. }),
            SettledState::Failed => matches!(result.outcome, TurnOutcome::Failed { .. }),
        });
        let mut successor = fixture.tool(
            TASK_SEND,
            json!({
                "task_id": task["task_id"], "message": STEERING,
            }),
        );
        assert_eq!(successor["queued"], true);
        assert_ne!(successor["turn_id"], task["turn_id"]);
        successor["task_id"] = task["task_id"].clone();
        let request = fixture.request().await;
        assert!(request.contains(STEERING));
        request.respond(common::canned_reply(REPLY));
        let settled = fixture.tool(TASK_WAIT, wait_input(&successor));
        assert_eq!(settled["status"], "done");
        assert_eq!(settled["result"]["status"], "completed");
        assert_eq!(settled["result"]["turn_id"], successor["turn_id"]);
        assert_eq!(original.wait_result().await.outcome, result.outcome);
        fixture.close(&task);
    });
}

#[test]
fn task_send_after_done_starts_successor() {
    task_send_after_settlement(SettledState::Done);
}

#[test]
fn task_send_after_cancel_starts_successor() {
    task_send_after_settlement(SettledState::Cancelled);
}

#[test]
fn task_send_after_provider_failure_starts_successor() {
    task_send_after_settlement(SettledState::Failed);
}

#[test]
fn task_send_after_despawn_rejects() {
    smol::block_on(async {
        let fixture = Fixture::new();
        let task = fixture.spawn(INITIAL);
        fixture.request().await.respond(common::canned_reply(REPLY));
        assert_eq!(fixture.tool(TASK_WAIT, wait_input(&task))["status"], "done");
        fixture.close(&task);
        let error = common::exec_tool_text(
            &fixture.registry,
            &fixture.context,
            TASK_SEND,
            json!({ "task_id": task["task_id"], "message": STEERING }),
        )
        .unwrap_err();
        assert!(error.contains("unknown task_id"));
        assert!(fixture.requests.is_empty());
    });
}

#[test]
fn nested_provider_gates_cross_bundled_task_wait_boundaries() {
    smol::block_on(async {
        let fixture = Fixture::new();
        let a = fixture.spawn(PROMPTS[0]);
        let request = fixture.request_for(PROMPTS[0]).await;
        assert!(request.contains(PROMPTS[0]));
        request.respond(common::canned_tool_use(TASK_SPAWN, spawn_input(PROMPTS[1])));

        let request = fixture.request_for(PROMPTS[0]).await;
        assert!(request.contains(PROMPTS[0]));
        let b = request.last_tool_result();
        assert!(b["task_id"].is_string());
        request.respond(common::canned_tool_use(TASK_WAIT, wait_input(&b)));

        let request = fixture.request_for(PROMPTS[1]).await;
        assert!(request.contains(PROMPTS[1]));
        request.respond(common::canned_tool_use(TASK_SPAWN, spawn_input(PROMPTS[2])));

        let request = fixture.request_for(PROMPTS[1]).await;
        assert!(request.contains(PROMPTS[1]));
        let c = request.last_tool_result();
        assert!(c["task_id"].is_string());
        request.respond(common::canned_tool_use(TASK_WAIT, wait_input(&c)));

        let request = fixture.request_for(PROMPTS[2]).await;
        assert!(request.contains(PROMPTS[2]));
        let admitted = fixture.tool(
            TASK_SEND,
            json!({ "task_id": a["task_id"], "message": STEERING }),
        );
        assert_eq!(admitted["turn_id"], a["turn_id"]);
        assert!(fixture.requests.is_empty());
        request.respond(common::canned_reply(REPLY));

        for prompt in [PROMPTS[1], PROMPTS[0]] {
            let request = fixture.request().await;
            assert!(request.contains(prompt));
            if prompt == PROMPTS[0] {
                assert!(request.contains(STEERING));
            }
            let waited = request.last_tool_result();
            assert_eq!(waited["status"], "done");
            assert_eq!(waited["result"]["text"], REPLY);
            request.respond(common::canned_reply(REPLY));
        }
        let result = fixture.tool(TASK_WAIT, wait_input(&a));
        assert_eq!(result["status"], "done");
        assert_eq!(result["result"]["turn_id"], a["turn_id"]);
        assert_eq!(result["result"]["text"], REPLY);
        fixture.close(&a);
    });
}
