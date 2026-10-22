use std::sync::Arc;

use maki_agent::actor::{
    ActorError, ActorLifecycle, ActorStatus, ConfigChange, ConfigPatch, PreparedModel,
};
use maki_agent::{
    AgentInput, CurrentManagedTurn, TranscriptRequest, TurnAdmissionOptions, TurnId, TurnOrigin,
    TurnProvenance,
};
use maki_lua_macro::{lua_class, lua_fn};
use mlua::{Lua, Table, UserDataMethods, UserDataRef, Value as LuaValue};

use super::authority::{authorize, destination_template, observe, retire_closed};
use super::projection::selection_table;
use super::subscriptions::{
    on_close__doc, on_close__register, on_config_change__doc, on_config_change__register,
    on_idle__doc, on_idle__register, on_turn_end__doc, on_turn_end__register, on_turn_start__doc,
    on_turn_start__register,
};
use super::tickets::ticket_wait;
use super::{AgentOwners, LuaAgent, LuaAgentRef, LuaAgentTurn};
use crate::agent_errors::{
    AGENT_BUSY, AGENT_CONFIG_UNAVAILABLE, NO_ORCHESTRATION_SERVICES, NO_PLUGIN_AUTHORITY,
    RESTRICTED_MODE_CHANGE, STALE_TURN, TURN_WAIT_TIMEOUT,
};
use crate::api::agent::{
    LuaTaskOwner, SessionProviderPreparer, build_session_provider, validate_required_output_mode,
};
use crate::api::env::host_state_dir;
use crate::api::util::convert::json_to_lua;
use crate::api::util::ctx::{AgentContext, LuaCtx};
use crate::api::util::pair::{Pair, err_pair, try_pair};
use crate::api::util::selection::selection_from_lua;
use crate::orchestration::{self, plugin_authority};
use crate::runtime::{current_managed_turn, current_plugin, register_defer_close};

/// Read the stable agent ID.
/// @return (string?, string?) Agent ID.
#[lua_fn(name = "id")]
fn agent_id(_lua: &Lua, this: &LuaAgent) -> mlua::Result<Pair<String>> {
    Ok((Some(this.identity()), None))
}

/// Read lifecycle, active turn, queue size, and cumulative usage.
/// @param ctx LuaCtx Invocation context.
/// @return (table?, string?) Fields: id (string), status ("idle", "running", or "closed"), queued (integer), active_turn (string or nil), input_tokens and output_tokens (cumulative integers). Authorized held managed handles retain a closed snapshot after graph removal.
#[lua_fn(name = "status")]
fn agent_status(lua: &Lua, this: &LuaAgent, ctx: UserDataRef<LuaCtx>) -> mlua::Result<Pair<Table>> {
    try_pair!(observe(lua, &ctx, this));
    let table = lua.create_table()?;
    table.set("id", this.identity())?;
    let snapshot = if let Some(held) = &this.held {
        try_pair!(
            try_pair!(held.snapshot())
                .actor
                .ok_or("agent snapshot is unavailable")
        )
    } else {
        try_pair!(this.manager.actor(this.id)).snapshot()
    };
    let status = match snapshot.lifecycle {
        ActorLifecycle::Closed | ActorLifecycle::Shutdown => "closed",
        ActorLifecycle::Open => match snapshot.status {
            ActorStatus::Idle => "idle",
            _ => "running",
        },
    };
    table.set("status", status)?;
    table.set("queued", snapshot.queued)?;
    table.set("active_turn", snapshot.active_turn.map(|id| id.to_string()))?;
    table.set("input_tokens", snapshot.cumulative_usage.total_input())?;
    table.set("output_tokens", snapshot.cumulative_usage.output)?;
    Ok((Some(table), None))
}

/// Read the actor's committed model configuration.
/// @param ctx LuaCtx Invocation context.
/// @return (table?, string?) Complete normalized selection {spec, thinking, fast}. thinking is a setting string or budget integer; disabled options are "off" and false. Descriptive model metadata is not included.
#[lua_fn(name = "model")]
fn agent_model(lua: &Lua, this: &LuaAgent, ctx: UserDataRef<LuaCtx>) -> mlua::Result<Pair<Table>> {
    try_pair!(authorize(lua, &ctx, this));
    let actor = try_pair!(this.manager.actor(this.id));
    let config = try_pair!(actor.effective_config().ok_or(AGENT_CONFIG_UNAVAILABLE));
    Ok((Some(selection_table(lua, &config)?), None))
}

/// List authenticated, policy-filtered model spec strings without changing focus.
/// @param ctx LuaCtx Invocation context.
/// @return (table?, string?) Array of available model spec strings.
#[lua_fn(name = "available_models")]
fn agent_available_models(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
) -> mlua::Result<Pair<Table>> {
    try_pair!(authorize(lua, &ctx, this));
    let service =
        try_pair!(orchestration::services(lua).ok_or("host model catalog is unavailable"));
    let target = try_pair!(this.manager.lookup(this.id));
    let models = try_pair!(service.available_models(&target));
    Ok((Some(lua.create_sequence_from(models)?), None))
}

/// Read a bounded committed transcript, optionally through a completed turn.
/// Omitted through_turn selects the latest history-bearing terminal turn; no
/// completed history returns an empty snapshot. Pending and unknown turns fail
/// explicitly. A boundary removed by compaction returns transcript_unavailable.
/// Snapshots are bounded history views, not an archive immune to compaction.
/// @param ctx LuaCtx Invocation context.
/// @param opts table Required last_messages (1..1000) and max_bytes (2..1048576); optional through_turn. max_bytes bounds compact JSON of the messages array, not metadata.
/// @return (table?, string?) agent_id, through_turn, messages, epoch, total_messages, omitted_messages, omitted_tool_groups, compacted, bytes, truncated. Tool-call/response groups are indivisible; omitted_tool_groups counts omitted groups, not calls. compacted identifies a history containing a compaction summary, independently of bounded truncation.
#[lua_fn(name = "transcript")]
fn agent_transcript(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    opts: Table,
) -> mlua::Result<Pair<Table>> {
    try_pair!(authorize(lua, &ctx, this));
    let through_turn = opts.get::<Option<String>>("through_turn")?;
    let through_turn = try_pair!(through_turn.map(|id| id.parse::<TurnId>()).transpose());
    let actor = try_pair!(this.manager.actor(this.id));
    let snapshot = try_pair!(actor.transcript(TranscriptRequest {
        through_turn,
        last_messages: opts.get::<usize>("last_messages")?,
        max_bytes: opts.get::<usize>("max_bytes")?,
    }));
    let table = lua.create_table()?;
    table.set("agent_id", this.identity())?;
    table.set("messages", json_to_lua(lua, &snapshot.messages)?)?;
    table.set(
        "through_turn",
        snapshot.through_turn.map(|id| id.to_string()),
    )?;
    table.set("epoch", snapshot.epoch)?;
    table.set("total_messages", snapshot.total_messages)?;
    table.set("omitted_messages", snapshot.omitted_messages)?;
    table.set("omitted_tool_groups", snapshot.omitted_tool_groups)?;
    table.set("compacted", snapshot.compacted)?;
    table.set("bytes", snapshot.bytes)?;
    table.set("truncated", snapshot.truncated)?;
    Ok((Some(table), None))
}

/// Return another nonowning handle to this agent.
/// @return (AgentRef?, string?) Stable reference.
#[lua_fn(name = "ref")]
fn agent_ref(_lua: &Lua, this: &LuaAgent) -> mlua::Result<Pair<LuaAgentRef>> {
    Ok((Some(this.reference()), None))
}

fn admit(
    lua: &Lua,
    agent: &LuaAgent,
    ctx: &LuaCtx,
    message: String,
    options: TurnAdmissionOptions,
    submit: bool,
) -> Result<LuaAgentTurn, String> {
    authorize(lua, ctx, agent)?;
    let actor = agent
        .manager
        .actor(agent.id)
        .map_err(|error| error.to_string())?;
    let config = actor.effective_config().ok_or(AGENT_CONFIG_UNAVAILABLE)?;
    let input = AgentInput {
        message,
        mode: config.mode.clone(),
        thinking: config.thinking,
        fast: config.fast,
        workflow: config.workflow,
        images: Vec::new(),
        preamble: Vec::new(),
        prompt: None,
        cancel: None,
    };
    let provenance = if let Ok(trusted) = ctx.trusted(lua) {
        TurnProvenance {
            origin: TurnOrigin::Plugin,
            plugin: Some(trusted.plugin.to_string()),
            plugin_generation: Some(trusted.generation),
            ..Default::default()
        }
    } else {
        let current = current_managed_turn(lua);
        let plugin = current_plugin(lua);
        TurnProvenance {
            origin: TurnOrigin::Plugin,
            plugin: plugin.as_ref().map(|plugin| plugin.0.to_string()),
            plugin_generation: plugin.map(|plugin| plugin.1),
            source_agent: current.as_ref().map(CurrentManagedTurn::agent_id),
            source_turn: current.as_ref().map(CurrentManagedTurn::turn_id),
        }
    };
    let options = TurnAdmissionOptions {
        provenance,
        ..options
    };
    let ticket = if submit {
        actor.submit(input, None, String::new(), options)
    } else {
        actor.admit_turn_with_options(input, None, String::new(), options)
    }
    .map_err(|error| match error {
        ActorError::Busy => AGENT_BUSY.to_owned(),
        ActorError::StaleTurn => STALE_TURN.to_owned(),
        other => other.to_string(),
    })?;
    Ok(LuaAgentTurn {
        agent: agent.clone(),
        ticket,
    })
}

/// Reads the optional `after_turn` admission guard; the inner error is a parse failure.
fn after_turn(opts: Option<&Table>) -> mlua::Result<Result<Option<TurnId>, String>> {
    let after_turn = opts
        .map(|opts| opts.get::<Option<String>>("after_turn"))
        .transpose()?
        .flatten();
    Ok(after_turn
        .map(|id| id.parse::<TurnId>())
        .transpose()
        .map_err(|error| error.to_string()))
}

/// Admit a turn only when this agent has no active, preparing, or queued work.
/// @param ctx LuaCtx Invocation context.
/// @param message string User message.
/// @param opts table? Optional after_turn ID required to match the latest terminal turn, including failure or cancellation.
/// @return (AgentTurn?, string?) Exact-turn ticket.
#[lua_fn(name = "send")]
fn agent_send(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    message: String,
    opts: Option<Table>,
) -> mlua::Result<Pair<LuaAgentTurn>> {
    let after_turn = try_pair!(after_turn(opts.as_ref())?);
    let turn = try_pair!(admit(
        lua,
        this,
        &ctx,
        message,
        TurnAdmissionOptions {
            idle_only: true,
            after_turn,
            ..Default::default()
        },
        false,
    ));
    Ok((Some(turn), None))
}

/// Submit instructions to open ordinary work, or start a successor when idle or admission is closed.
/// Busy and compatible pending submissions return the existing ticket. Instructions reach the next
/// safe model request without interrupting a provider request or tool. Bounded inbox rejection does
/// not cancel accepted work. Exact enqueue operations remain separate FIFO turns.
/// @param ctx LuaCtx Invocation context.
/// @param message string User instructions (text only).
/// @param opts table? existing_only joins compatible admitted work or returns busy without starting a new turn. Defaults to false.
/// @return (AgentTurn?, string?) Ticket covering these instructions, or an admission error.
#[lua_fn(name = "submit")]
fn agent_submit(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    message: String,
    opts: Option<Table>,
) -> mlua::Result<Pair<LuaAgentTurn>> {
    let existing_only = opts
        .map(|opts| opts.get::<Option<bool>>("existing_only"))
        .transpose()?
        .flatten()
        .unwrap_or_default();
    let turn = try_pair!(admit(
        lua,
        this,
        &ctx,
        message,
        TurnAdmissionOptions {
            existing_only,
            ..Default::default()
        },
        true,
    ));
    Ok((Some(turn), None))
}

/// Queue a turn behind already admitted work.
/// @param ctx LuaCtx Invocation context.
/// @param message string User message.
/// @return (AgentTurn?, string?) Exact-turn ticket.
#[lua_fn(name = "enqueue")]
fn agent_enqueue(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    message: String,
) -> mlua::Result<Pair<LuaAgentTurn>> {
    let turn = try_pair!(admit(lua, this, &ctx, message, Default::default(), false));
    Ok((Some(turn), None))
}

/// Send an idle-only turn and wait for its exact result.
/// @param ctx LuaCtx Invocation context.
/// @param message string User message.
/// @param opts table? timeout is a nonnegative integer number of seconds; zero is nonblocking. after_turn must match the latest terminal turn.
/// @return (table?, string|table?) AgentTurn result, or an admission/wait error. A pending timeout returns nil and {kind = "timeout", message, ticket}; the exact ticket remains usable with current valid authority. Timeout does not cancel execution.
#[lua_fn(name = "prompt")]
async fn agent_prompt(
    lua: Lua,
    this: UserDataRef<LuaAgent>,
    ctx: UserDataRef<LuaCtx>,
    message: String,
    opts: Option<Table>,
) -> mlua::Result<(Option<Table>, Option<LuaValue>)> {
    // Malformed options must fail before admission, not strand a queued turn.
    if let Some(opts) = &opts {
        opts.get::<Option<u64>>("timeout")?;
    }
    let admitted = after_turn(opts.as_ref())?.and_then(|after_turn| {
        admit(
            &lua,
            &this,
            &ctx,
            message,
            TurnAdmissionOptions {
                idle_only: true,
                after_turn,
                ..Default::default()
            },
            false,
        )
    });
    let turn = match admitted {
        Ok(turn) => turn,
        Err(error) => return Ok((None, Some(LuaValue::String(lua.create_string(error)?)))),
    };
    drop(this);
    let userdata = lua.create_userdata(turn)?;
    let (result, error) =
        ticket_wait(lua.clone(), userdata.borrow::<LuaAgentTurn>()?, ctx, opts).await?;
    if result.is_none()
        && error
            .as_deref()
            .is_none_or(|error| error == TURN_WAIT_TIMEOUT)
    {
        let timeout = lua.create_table()?;
        timeout.set("kind", "timeout")?;
        timeout.set("message", TURN_WAIT_TIMEOUT)?;
        timeout.set("ticket", userdata)?;
        return Ok((None, Some(LuaValue::Table(timeout))));
    }
    let error = error
        .map(|error| lua.create_string(error).map(LuaValue::String))
        .transpose()?;
    Ok((result, error))
}

/// Read the most recently admitted turn, including turns the runtime admitted
/// to deliver a child's result.
/// @param ctx LuaCtx Invocation context.
/// @return (AgentTurn?, string?) Exact-turn ticket, or nil without an error when no turn was admitted or it is no longer retained.
#[lua_fn(name = "latest_turn")]
fn agent_latest_turn(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
) -> mlua::Result<Pair<LuaAgentTurn>> {
    try_pair!(observe(lua, &ctx, this));
    let actor = try_pair!(this.manager.actor(this.id));
    Ok((
        actor.latest_ticket().map(|ticket| LuaAgentTurn {
            agent: this.clone(),
            ticket,
        }),
        None,
    ))
}

/// Switch model through a reserved actor configuration operation.
/// @param ctx LuaCtx Invocation context.
/// @param patch string|table Spec string or partial spec/thinking/fast selection.
/// @return (table?, string?) Complete normalized committed selection {spec, thinking, fast}, accepted as a subsequent patch subject to destination policy.
#[lua_fn(name = "set_model")]
async fn agent_set_model(
    lua: Lua,
    this: UserDataRef<LuaAgent>,
    ctx: UserDataRef<LuaCtx>,
    patch: LuaValue,
) -> mlua::Result<Pair<Table>> {
    try_pair!(authorize(&lua, &ctx, &this));
    let selection = try_pair!(selection_from_lua(&lua, patch));
    let spec = selection.spec.map(|spec| spec.to_string());
    let template = try_pair!(destination_template(&lua, &this));
    let target = try_pair!(this.manager.lookup(this.id));
    let services = try_pair!(orchestration::services(&lua).ok_or(NO_ORCHESTRATION_SERVICES));
    try_pair!(services.validate_config_mutation(&target));
    let commit = orchestration::commit_config_change(
        &*services,
        &target,
        async |publication, predecessor| {
            let claim = ctx.agent_publication_claim(&lua, &target, &publication)?;
            let mut captured = template.to_tool_context();
            captured.apply_effective_config(&predecessor);
            let template = AgentContext::from(&captured);
            let prepare = lua
                .app_data_ref::<SessionProviderPreparer>()
                .map(|prepare| Arc::clone(&prepare));
            let model = match spec {
                Some(_) => {
                    let (model, provider) =
                        build_session_provider(&spec, false, &template, prepare).await?;
                    Some(PreparedModel { model, provider })
                }
                None => None,
            };
            authorize(&lua, &ctx, &this)?;
            drop(this);
            drop(ctx);
            let patch = ConfigPatch {
                model,
                thinking: selection.thinking,
                fast: selection.fast,
                ..Default::default()
            };
            Ok((claim, ConfigChange::Patch(patch)))
        },
    )
    .await;
    let commit = try_pair!(commit);
    Ok((Some(selection_table(&lua, &commit.config)?), None))
}

fn mode_is_restricted(context: &AgentContext) -> bool {
    context.restrict_write_to().is_some()
        || context
            .mode_def
            .as_ref()
            .is_some_and(|mode| mode.tools.is_some())
}

/// Read the currently committed mode.
/// @param ctx LuaCtx Invocation context.
/// @return (string?, string?) Registered mode name.
#[lua_fn(name = "mode")]
fn agent_mode(lua: &Lua, this: &LuaAgent, ctx: UserDataRef<LuaCtx>) -> mlua::Result<Pair<String>> {
    try_pair!(authorize(lua, &ctx, this));
    let actor = try_pair!(this.manager.actor(this.id));
    let config = try_pair!(actor.effective_config().ok_or(AGENT_CONFIG_UNAVAILABLE));
    Ok((Some(config.mode.id().key().to_string()), None))
}

/// Switch mode through the actor configuration FIFO.
/// @param ctx LuaCtx Invocation context.
/// @param name string Registered mode name.
/// @return (string?, string?) Committed mode name or error.
#[lua_fn(name = "set_mode")]
async fn agent_set_mode(
    lua: Lua,
    this: UserDataRef<LuaAgent>,
    ctx: UserDataRef<LuaCtx>,
    name: String,
) -> mlua::Result<Pair<String>> {
    try_pair!(authorize(&lua, &ctx, &this));
    let target = try_pair!(this.manager.lookup(this.id));
    let services = try_pair!(orchestration::services(&lua).ok_or(NO_ORCHESTRATION_SERVICES));
    try_pair!(services.validate_config_mutation(&target));
    let template = try_pair!(destination_template(&lua, &this));
    if ctx.trusted(&lua).is_err()
        && (mode_is_restricted(&template) || ctx.agent().is_some_and(mode_is_restricted))
    {
        return Ok(err_pair(RESTRICTED_MODE_CHANGE));
    }
    let structured = this
        .state
        .as_ref()
        .is_some_and(|state| state.structured.is_some());
    let state_dir = try_pair!(host_state_dir(&lua));
    let ticket = try_pair!(orchestration::reserve_config_change(&*services, &target));
    let commit = ticket
        .commit_mode(
            &target,
            &template.modes,
            &name,
            &state_dir,
            async |publication, definition| {
                validate_required_output_mode(structured, Some(definition))?;
                let claim = ctx.agent_publication_claim(&lua, &target, &publication)?;
                authorize(&lua, &ctx, &this)?;
                drop(this);
                drop(ctx);
                Ok(claim)
            },
        )
        .await;
    let commit = try_pair!(commit);
    Ok((Some(commit.config.mode.id().key().to_string()), None))
}

/// Cancel existing work without closing the agent.
/// @param ctx LuaCtx Invocation context.
/// @return (boolean?, string?) Success or error.
#[lua_fn(name = "cancel")]
fn agent_cancel(lua: &Lua, this: &LuaAgent, ctx: UserDataRef<LuaCtx>) -> mlua::Result<Pair<bool>> {
    try_pair!(authorize(lua, &ctx, this));
    try_pair!(this.manager.cancel_agent(this.id));
    Ok((Some(true), None))
}

/// Cancel existing work in this agent and its descendants.
/// @param ctx LuaCtx Invocation context.
/// @return (boolean?, string?) Success or error.
#[lua_fn(name = "cancel_subtree")]
fn agent_cancel_subtree(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
) -> mlua::Result<Pair<bool>> {
    try_pair!(authorize(lua, &ctx, this));
    try_pair!(this.manager.cancel_subtree(this.id));
    Ok((Some(true), None))
}

/// Permanently close this agent and its descendants.
/// @param ctx LuaCtx Invocation context.
/// @return (boolean?, string?) Success or error.
#[lua_fn(name = "close")]
fn agent_close(lua: &Lua, this: &LuaAgent, ctx: UserDataRef<LuaCtx>) -> mlua::Result<Pair<bool>> {
    try_pair!(authorize(lua, &ctx, this));
    try_pair!(this.manager.close_subtree(this.id));
    retire_closed(lua);
    Ok((Some(true), None))
}

/// Close this agent at the end of the current host task, even on cancellation.
/// @param ctx LuaCtx Invocation context.
/// @return (boolean?, string?) Cleanup registered.
#[lua_fn(name = "defer_close")]
pub(crate) fn agent_defer_close(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
) -> mlua::Result<Pair<bool>> {
    try_pair!(authorize(lua, &ctx, this));
    let manager = this.manager.clone();
    let id = this.id;
    let sessions = lua
        .app_data_ref::<AgentOwners>()
        .map(|sessions| sessions.clone());
    try_pair!(register_defer_close(
        lua,
        Box::new(move || {
            let _ = manager.close_subtree(id);
            if let Some(sessions) = sessions {
                sessions.retire_settled();
            }
        })
    ));
    Ok((Some(true), None))
}

// Bundled task plugin internals, not public API. `_maki_session_id` reads the
// task/UI identity. `_maki_task_owner` returns a host-stamped guard whose
// `_maki_can_control` checks plugin generation, host and parent branch, and whose
// `_maki_retired` reports a closed child. The guard authorizes no effect.
fn agent_internal_methods<M: UserDataMethods<LuaAgent>>(methods: &mut M) {
    methods.add_method("_maki_task_owner", |lua, this, ctx: UserDataRef<LuaCtx>| {
        authorize(lua, &ctx, this).map_err(mlua::Error::runtime)?;
        let (plugin, generation) =
            current_plugin(lua).ok_or_else(|| mlua::Error::runtime(NO_PLUGIN_AUTHORITY))?;
        Ok(LuaTaskOwner {
            owner_id: this.state.as_ref().and_then(|state| state.parent_agent_id),
            owner: plugin_authority(lua, plugin.clone()),
            plugin,
            generation,
            target: this
                .manager
                .lookup(this.id)
                .map_err(mlua::Error::external)?,
        })
    });
    methods.add_method("_maki_session_id", |_, this, ()| {
        Ok(this
            .state
            .as_ref()
            .map(|state| state.ui_id.clone())
            .unwrap_or_else(|| this.identity()))
    });
}

lua_class! {
    /// A nonowning agent handle. Garbage collection never closes its actor.
    /// Reads and effects require current invocation authority; identity is context-free.
    /// A held managed handle retains authorized closed-status reads, not mutation rights.
    "maki.agent.Agent" => LuaAgent, AGENT_DOCS [
        agent_id, agent_latest_turn, agent_status, agent_mode, agent_model, agent_available_models,
        agent_transcript, agent_ref, agent_send, agent_submit, agent_enqueue, agent_prompt, agent_set_model,
        agent_set_mode, agent_cancel, agent_cancel_subtree, agent_close, agent_defer_close,
        on_turn_start, on_turn_end, on_idle, on_config_change, on_close,
    ]
    extra agent_internal_methods
}
