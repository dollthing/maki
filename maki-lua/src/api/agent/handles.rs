use super::{LuaActorState, LuaSession, SessionControl, build_session};
use crate::agent_errors::{
    DELEGATED_FOCUS, MANAGED_AUTHORITY_INACTIVE, NO_FOCUSED_AGENT, NO_ORCHESTRATION_SERVICES,
    NO_PLUGIN_AUTHORITY, NO_TASK_AUTHORITY, OTHER_RUNTIME, RESTRICTED_MODE_CHANGE,
    TRUSTED_SUBSCRIPTION_REQUIRED, TURN_WAIT_TIMEOUT,
};
use crate::api::util::{
    convert::json_to_lua,
    ctx::LuaCtx,
    pair::{Pair, err_pair, try_pair},
};
use crate::host_root::{HostRootRef, HostRootServices, UNSUPPORTED_OPERATION_ERROR};
use maki_agent::{
    AgentActorHandle, AgentId, AgentInput, AgentManagerHandle, AgentRef, CurrentManagedTurn,
    TurnId, TurnTicket,
    actor::{ActorLifecycle, ActorStatus},
    cancel::CancelMap,
};
use maki_lua_macro::{lua_class, lua_fn};
use maki_providers::{CompleteModelSelection, ModelSelection, RequestOptions};
use mlua::{Function, Lua, LuaSerdeExt, Table, UserDataRef};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

const UNSETTLED_TURN_RESULT: &str = "turn has no settled result";

#[derive(Clone)]
enum AgentRuntime {
    HostRoot {
        services: Arc<HostRootServices>,
        reference: HostRootRef,
    },
    Managed(AgentManagerHandle),
    Unmanaged {
        actor: Arc<AgentActorHandle>,
        parent: Arc<CancelMap<String>>,
        plugin: Arc<str>,
        generation: u64,
        owner: Arc<crate::orchestration::PluginAuthority>,
    },
}

impl AgentRuntime {
    fn managed(&self) -> Result<&AgentManagerHandle, String> {
        match self {
            Self::Managed(manager) => Ok(manager),
            Self::HostRoot { .. } => Err(UNSUPPORTED_OPERATION_ERROR.into()),
            Self::Unmanaged { .. } => Err("operation requires a managed graph".into()),
        }
    }

    fn actor(&self, id: Option<AgentId>) -> Result<AgentActorHandle, String> {
        let id = id.ok_or(UNSUPPORTED_OPERATION_ERROR)?;
        match self {
            Self::Managed(manager) => manager.actor(id).map_err(|error| error.to_string()),
            Self::Unmanaged { actor, .. } if actor.agent_id() == id => Ok(actor.as_ref().clone()),
            Self::HostRoot { .. } => Err(UNSUPPORTED_OPERATION_ERROR.into()),
            Self::Unmanaged { .. } => Err(OTHER_RUNTIME.into()),
        }
    }

    fn lookup(&self, id: Option<AgentId>) -> Result<AgentRef, String> {
        let id = id.ok_or(UNSUPPORTED_OPERATION_ERROR)?;
        self.managed()?
            .lookup(id)
            .map_err(|error| error.to_string())
    }

    fn close_subtree(&self, id: Option<AgentId>) -> Result<(), String> {
        let id = id.ok_or(UNSUPPORTED_OPERATION_ERROR)?;
        match self {
            Self::HostRoot { .. } => Err(UNSUPPORTED_OPERATION_ERROR.into()),
            Self::Managed(manager) => manager.close_subtree(id).map_err(|error| error.to_string()),
            Self::Unmanaged { actor, .. } => {
                actor.close();
                Ok(())
            }
        }
    }

    fn cancel_agent(&self, id: Option<AgentId>) -> Result<(), String> {
        let id = id.ok_or(UNSUPPORTED_OPERATION_ERROR)?;
        match self {
            Self::HostRoot { .. } => Err(UNSUPPORTED_OPERATION_ERROR.into()),
            Self::Managed(manager) => manager.cancel_agent(id).map_err(|error| error.to_string()),
            Self::Unmanaged { actor, .. } => {
                actor.cancel_existing();
                Ok(())
            }
        }
    }

    fn cancel_subtree(&self, id: Option<AgentId>) -> Result<(), String> {
        let id = id.ok_or(UNSUPPORTED_OPERATION_ERROR)?;
        match self {
            Self::Managed(manager) => manager
                .cancel_subtree(id)
                .map_err(|error| error.to_string()),
            Self::HostRoot { .. } => Err(UNSUPPORTED_OPERATION_ERROR.into()),
            Self::Unmanaged { .. } => self.cancel_agent(Some(id)),
        }
    }
}

#[derive(Clone)]
pub(crate) struct LuaAgent {
    manager: AgentRuntime,
    id: Option<AgentId>,
    state: Option<Arc<LuaActorState>>,
    held: Option<AgentRef>,
    root: Option<AgentRef>,
    ancestors: Vec<AgentId>,
}

impl LuaAgent {
    fn identity(&self) -> String {
        match &self.manager {
            AgentRuntime::HostRoot { reference, .. } => reference.id().to_string(),
            _ => self.id.map(|id| id.to_string()).unwrap_or_default(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct LuaAgentRef {
    manager: AgentRuntime,
    id: Option<AgentId>,
    held: Option<AgentRef>,
}

#[derive(Clone)]
struct LuaAgentTurn {
    agent: LuaAgent,
    ticket: TurnTicket,
}

#[derive(Clone, Default)]
struct AgentSessions(Arc<Mutex<HashMap<AgentId, LuaSession>>>);

#[derive(Default)]
struct AgentOwners(HashMap<AgentId, Arc<str>>);

pub(crate) fn close_plugin_agents(lua: &Lua, plugin: &str) {
    let ids = if let Some(mut owners) = lua.app_data_mut::<AgentOwners>() {
        let ids: Vec<_> = owners
            .0
            .iter()
            .filter(|(_, owner)| owner.as_ref() == plugin)
            .map(|(id, _)| *id)
            .collect();
        for id in &ids {
            owners.0.remove(id);
        }
        ids
    } else {
        return;
    };
    let retired = lua.app_data_ref::<AgentSessions>().map(|sessions| {
        let mut sessions = sessions.0.lock().unwrap_or_else(|error| error.into_inner());
        ids.into_iter()
            .filter_map(|id| sessions.remove(&id))
            .collect::<Vec<_>>()
    });
    drop(retired);
}

fn session_settled_closed(session: &LuaSession) -> bool {
    let snapshot = session.actor.snapshot();
    snapshot.lifecycle != ActorLifecycle::Open && snapshot.active_turn.is_none()
}

fn retire_closed(lua: &Lua) {
    let Some(sessions) = lua.app_data_ref::<AgentSessions>() else {
        return;
    };
    let retired = {
        let mut sessions = sessions.0.lock().unwrap_or_else(|error| error.into_inner());
        let ids: Vec<_> = sessions
            .iter()
            .filter(|(_, session)| session_settled_closed(session))
            .map(|(id, _)| *id)
            .collect();
        ids.into_iter()
            .filter_map(|id| sessions.remove(&id))
            .collect::<Vec<_>>()
    };
    drop(retired);
}

fn validate_unmanaged(lua: &Lua, ctx: &LuaCtx, runtime: &AgentRuntime) -> Result<(), String> {
    ctx.validate_origin(lua)?;
    let AgentRuntime::Unmanaged {
        parent,
        plugin,
        generation,
        owner,
        ..
    } = runtime
    else {
        return Err("not an unmanaged agent".into());
    };
    let template = ctx
        .trusted_host_root(lua)
        .ok()
        .and_then(|host| host.template.as_ref())
        .map(super::AgentContext::from);
    let context = ctx
        .agent()
        .or(template.as_ref())
        .ok_or_else(|| ctx.cap_err("orchestration"))?;
    let active = lua
        .app_data_ref::<crate::runtime::TaskHandle>()
        .map(|task| Arc::clone(&task))
        .ok_or(NO_TASK_AUTHORITY)?;
    if !crate::runtime::current_plugin(lua)
        .is_some_and(|current| current.0 == *plugin && current.1 == *generation)
        || !Arc::ptr_eq(
            owner,
            &crate::orchestration::plugin_authority(lua, plugin.clone()),
        )
    {
        return Err("agent belongs to another plugin generation".into());
    }
    let cell = crate::runtime::lock_cell(&active);
    if !cell.scope_alive
        || cell.cancel.is_cancelled()
        || context.cancel.is_cancelled()
        || context.managed_turn.is_some()
        || cell.managed_turn.is_some()
        || !Arc::ptr_eq(parent, &context.subagent_cancels)
    {
        return Err("unmanaged agent authority is not active in this invocation".into());
    }
    Ok(())
}

fn host_agent(services: Arc<HostRootServices>) -> LuaAgent {
    LuaAgent {
        manager: AgentRuntime::HostRoot {
            reference: services.reference(),
            services,
        },
        id: None,
        state: None,
        held: None,
        root: None,
        ancestors: Vec::new(),
    }
}

fn validate_host_root(lua: &Lua, ctx: &LuaCtx, agent: &LuaAgent) -> Result<(), String> {
    let AgentRuntime::HostRoot {
        services,
        reference,
    } = &agent.manager
    else {
        return Err(UNSUPPORTED_OPERATION_ERROR.into());
    };
    let current = ctx
        .host_root(lua)?
        .ok_or("host root authority is unavailable")?;
    if !Arc::ptr_eq(services, &current) {
        return Err(OTHER_RUNTIME.into());
    }
    services
        .validate(reference)
        .map_err(|error| error.to_string())
}

fn context_graph(lua: &Lua, ctx: &LuaCtx) -> Result<(AgentManagerHandle, AgentId), String> {
    ctx.validate_origin(lua)?;
    if let Ok(trusted) = ctx.trusted(lua) {
        return Ok((trusted.target.manager(), trusted.target.id()));
    }
    let current = ctx
        .agent()
        .and_then(|agent| agent.managed_turn.as_ref())
        .ok_or_else(|| "current/root/list require a managed graph".to_owned())?;
    let active = crate::runtime::current_managed_turn(lua).ok_or(MANAGED_AUTHORITY_INACTIVE)?;
    if !current.same_authority(&active) {
        return Err(MANAGED_AUTHORITY_INACTIVE.into());
    }
    current
        .validate_active()
        .map_err(|error| error.to_string())?;
    Ok((current.manager(), current.agent_id()))
}

fn scope(lua: &Lua, ctx: &LuaCtx, target: AgentId) -> Result<Option<CurrentManagedTurn>, String> {
    let (manager, _) = context_graph(lua, ctx)?;
    manager.node(target).map_err(|error| error.to_string())?;
    let expected = ctx.agent().and_then(|agent| agent.managed_turn.as_ref());
    let active = crate::runtime::current_managed_turn(lua);
    match (expected, active) {
        (Some(expected), Some(active)) if expected.same_authority(&active) => {
            active
                .validate_active()
                .map_err(|error| error.to_string())?;
            if target != active.agent_id() {
                active
                    .validate_descendant(target)
                    .map_err(|error| error.to_string())?;
            }
            Ok(Some(active))
        }
        (None, None) if ctx.trusted(lua).is_ok() => {
            let trusted = ctx.trusted(lua)?;
            let target = manager.lookup(target).map_err(|error| error.to_string())?;
            trusted.validate_target(lua, &target)?;
            Ok(None)
        }
        _ => Err(MANAGED_AUTHORITY_INACTIVE.into()),
    }
}

fn authorize(
    lua: &Lua,
    ctx: &LuaCtx,
    agent: &LuaAgent,
) -> Result<Option<CurrentManagedTurn>, String> {
    if matches!(agent.manager, AgentRuntime::HostRoot { .. }) {
        validate_host_root(lua, ctx, agent)?;
        return Ok(None);
    }
    if matches!(agent.manager, AgentRuntime::Unmanaged { .. }) {
        validate_unmanaged(lua, ctx, &agent.manager)?;
        return Ok(None);
    }
    if let Ok(trusted) = ctx.trusted(lua) {
        let target = agent
            .manager
            .lookup(agent.id)
            .map_err(|error| error.to_string())?;
        trusted.validate_target(lua, &target)?;
        return Ok(None);
    }
    retire_closed(lua);
    let (manager, _) = context_graph(lua, ctx)?;
    if !manager.same_manager(agent.manager.managed()?) {
        return Err(OTHER_RUNTIME.into());
    }
    scope(lua, ctx, agent.id.ok_or(UNSUPPORTED_OPERATION_ERROR)?)
}

fn destination_template(lua: &Lua, agent: &LuaAgent) -> Result<super::AgentContext, String> {
    if let Some(template) = agent.state.as_ref().and_then(|state| state.template.get()) {
        let mut template = template.clone();
        if let Some(config) = agent
            .manager
            .actor(agent.id)
            .map_err(|error| error.to_string())?
            .effective_config()
        {
            template.apply_effective_config(&config);
        }
        return Ok(super::AgentContext::from(&template));
    }
    let target = agent
        .manager
        .lookup(agent.id)
        .map_err(|error| error.to_string())?;
    let service =
        crate::orchestration::services(lua).ok_or("destination template is unavailable")?;
    Ok(super::AgentContext::from(&service.template(&target)?))
}

fn observe(
    lua: &Lua,
    ctx: &LuaCtx,
    agent: &LuaAgent,
) -> Result<Option<CurrentManagedTurn>, String> {
    if matches!(agent.manager, AgentRuntime::HostRoot { .. }) {
        validate_host_root(lua, ctx, agent)?;
        return Ok(None);
    }
    if matches!(agent.manager, AgentRuntime::Unmanaged { .. }) {
        validate_unmanaged(lua, ctx, &agent.manager)?;
        return Ok(None);
    }
    if let Ok(trusted) = ctx.trusted_observer(lua) {
        let root = agent
            .root
            .as_ref()
            .ok_or("managed root identity is unavailable")?;
        if !root.manager().same_manager(agent.manager.managed()?) {
            return Err(OTHER_RUNTIME.into());
        }
        if !trusted.target.manager().same_manager(&root.manager()) {
            return Err(OTHER_RUNTIME.into());
        }
        let subject = trusted
            .target
            .snapshot()
            .map_err(|error| error.to_string())?;
        if subject.root_id != root.id() {
            return Err("agent belongs to another graph".into());
        }
        return Ok(None);
    }
    retire_closed(lua);
    let (manager, _) = context_graph(lua, ctx)?;
    if !manager.same_manager(agent.manager.managed()?) {
        return Err(OTHER_RUNTIME.into());
    }
    let current = crate::runtime::current_managed_turn(lua).ok_or("managed authority expired")?;
    if !agent.ancestors.contains(&current.agent_id()) {
        return Err("agent is not a descendant of the caller".into());
    }
    Ok(Some(current))
}

fn lookup(manager: AgentManagerHandle, id: AgentId, lua: &Lua) -> Result<LuaAgent, String> {
    let held = manager.lookup(id).map_err(|error| error.to_string())?;
    let node = held.snapshot().map_err(|error| error.to_string())?;
    let root = manager
        .lookup(node.root_id)
        .map_err(|error| error.to_string())?;
    let mut ancestors = vec![id];
    let mut parent = node.parent_id;
    while let Some(id) = parent {
        ancestors.push(id);
        parent = manager
            .node(id)
            .map_err(|error| error.to_string())?
            .parent_id;
    }
    let state = lua.app_data_ref::<AgentSessions>().and_then(|sessions| {
        sessions
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&id)
            .map(|session| Arc::clone(&session.state))
    });
    Ok(LuaAgent {
        manager: AgentRuntime::Managed(manager),
        id: Some(id),
        state,
        held: Some(held),
        root: Some(root),
        ancestors,
    })
}

impl LuaCtx {
    pub(crate) fn pin_subject(&mut self, lua: &Lua) {
        if self.subject.is_some() {
            return;
        }
        self.subject = self.agent().and_then(|context| context.execution_agent_id);
        if self.subject.is_some() {
            return;
        }
        self.subject = self.agent().and_then(|context| {
            lua.app_data_ref::<AgentSessions>().and_then(|sessions| {
                sessions
                    .0
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .values()
                    .find(|session| {
                        session
                            .state
                            .params
                            .get()
                            .is_some_and(|params| Arc::ptr_eq(&params.ledger, &context.ledger))
                    })
                    .map(|session| session.agent_id)
            })
        });
    }
}

/// Read the agent associated with an explicit invocation context.
/// @param ctx LuaCtx Invocation context.
/// @return (Agent?, string?) Nonowning agent handle.
#[lua_fn]
pub(crate) fn current(lua: &Lua, ctx: UserDataRef<LuaCtx>) -> mlua::Result<Pair<LuaAgent>> {
    if let Some(services) = try_pair!(ctx.host_root(lua)) {
        return Ok((Some(host_agent(services)), None));
    }
    if let Some(context) = ctx.agent().filter(|agent| agent.managed_turn.is_none()) {
        let session = lua.app_data_ref::<AgentSessions>().and_then(|sessions| {
            sessions
                .0
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(&ctx.subject?)
                .filter(|session| {
                    session.state.params.get().is_some_and(|params| {
                        context.execution_agent_id == Some(session.agent_id)
                            || Arc::ptr_eq(&params.ledger, &context.ledger)
                    })
                })
                .map(|session| {
                    (
                        session.agent_id,
                        session.actor.clone(),
                        session.state.clone(),
                    )
                })
        });
        let (id, actor, state) = try_pair!(session.ok_or("execution subject is unavailable"));
        let (plugin, generation) =
            try_pair!(crate::runtime::current_plugin(lua).ok_or(NO_PLUGIN_AUTHORITY));
        let agent = LuaAgent {
            manager: AgentRuntime::Unmanaged {
                actor,
                parent: context.subagent_cancels.clone(),
                owner: crate::orchestration::plugin_authority(lua, plugin.clone()),
                plugin,
                generation,
            },
            id: Some(id),
            state: Some(state),
            held: None,
            root: None,
            ancestors: Vec::new(),
        };
        try_pair!(authorize(lua, &ctx, &agent));
        return Ok((Some(agent), None));
    }
    let (manager, id) = try_pair!(context_graph(lua, &ctx));
    try_pair!(scope(lua, &ctx, id));
    Ok((Some(try_pair!(lookup(manager, id, lua))), None))
}

/// Resolves the agent that has UI focus now, pinned against later focus changes.
pub(crate) fn focused_agent_ref(lua: &Lua) -> Result<LuaAgentRef, String> {
    if crate::runtime::in_delegated_tool(lua) {
        return Err(DELEGATED_FOCUS.into());
    }
    let held = crate::orchestration::resolve_target(lua, None)?
        .ok_or(NO_FOCUSED_AGENT)?
        .target;
    Ok(LuaAgentRef {
        manager: AgentRuntime::Managed(held.manager()),
        id: Some(held.id()),
        held: Some(held),
    })
}

/// Read the root agent of this context's graph.
/// @param ctx LuaCtx Invocation context.
/// @return (AgentRef?, string?) Visibility-only root reference.
#[lua_fn]
pub(super) fn root(lua: &Lua, ctx: UserDataRef<LuaCtx>) -> mlua::Result<Pair<LuaAgentRef>> {
    if let Some(services) = try_pair!(ctx.host_root(lua)) {
        let agent = host_agent(services);
        return Ok((
            Some(LuaAgentRef {
                manager: agent.manager,
                id: None,
                held: None,
            }),
            None,
        ));
    }
    let (manager, _) = try_pair!(context_graph(lua, &ctx));
    let id = try_pair!(manager.root_id());
    if let Ok(trusted) = ctx.trusted(lua) {
        try_pair!(trusted.validate_target(lua, &try_pair!(manager.lookup(id))));
    }
    Ok((
        Some(LuaAgentRef {
            held: Some(try_pair!(manager.lookup(id))),
            manager: AgentRuntime::Managed(manager),
            id: Some(id),
        }),
        None,
    ))
}

/// Resolve an AgentRef with current control authority. String IDs are not accepted.
/// @param ctx LuaCtx Invocation context.
/// @param reference AgentRef Visibility-only reference.
/// @return (Agent?, string?) Nonowning handle or error.
#[lua_fn]
pub(super) fn get(
    lua: &Lua,
    ctx: UserDataRef<LuaCtx>,
    reference: UserDataRef<LuaAgentRef>,
) -> mlua::Result<Pair<LuaAgent>> {
    if matches!(reference.manager, AgentRuntime::HostRoot { .. }) {
        let agent = LuaAgent {
            manager: reference.manager.clone(),
            id: None,
            state: None,
            held: None,
            root: None,
            ancestors: Vec::new(),
        };
        try_pair!(validate_host_root(lua, &ctx, &agent));
        return Ok((Some(agent), None));
    }
    if matches!(reference.manager, AgentRuntime::Unmanaged { .. }) {
        let id = try_pair!(reference.id.ok_or(UNSUPPORTED_OPERATION_ERROR));
        let agent = LuaAgent {
            manager: reference.manager.clone(),
            id: reference.id,
            held: None,
            root: None,
            ancestors: Vec::new(),
            state: lua.app_data_ref::<AgentSessions>().and_then(|sessions| {
                sessions
                    .0
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .get(&id)
                    .map(|session| Arc::clone(&session.state))
            }),
        };
        try_pair!(authorize(lua, &ctx, &agent));
        return Ok((Some(agent), None));
    }
    let (manager, _) = try_pair!(context_graph(lua, &ctx));
    if ctx.trusted(lua).is_err() && !manager.same_manager(try_pair!(reference.manager.managed())) {
        return Ok(err_pair("agent reference belongs to another runtime"));
    }
    let agent = try_pair!(lookup(
        try_pair!(reference.manager.managed()).clone(),
        try_pair!(reference.id.ok_or(UNSUPPORTED_OPERATION_ERROR)),
        lua
    ));
    try_pair!(authorize(lua, &ctx, &agent));
    Ok((Some(agent), None))
}

/// List accessible live agents in this context's graph.
/// @param ctx LuaCtx Invocation context.
/// @return (table?, string?) Array of visibility-only AgentRef values. Visibility does not grant control. Non-TUI hosts include their root and accessible live plugin-owned standalone children.
#[lua_fn]
pub(super) fn list(lua: &Lua, ctx: UserDataRef<LuaCtx>) -> mlua::Result<Pair<Table>> {
    if let Some(services) = try_pair!(ctx.host_root(lua)) {
        let agent = host_agent(services);
        let table = lua.create_table()?;
        table.push(LuaAgentRef {
            manager: agent.manager,
            id: None,
            held: None,
        })?;
        if let Some(sessions) = lua.app_data_ref::<AgentSessions>() {
            let sessions = sessions.0.lock().unwrap_or_else(|error| error.into_inner());
            for session in sessions.values() {
                let SessionControl::Unmanaged { parent_cancels, .. } = &session.control else {
                    continue;
                };
                let Some((plugin, generation)) = crate::runtime::current_plugin(lua) else {
                    continue;
                };
                if lua
                    .app_data_ref::<AgentOwners>()
                    .is_none_or(|owners| owners.0.get(&session.agent_id) != Some(&plugin))
                {
                    continue;
                }
                let runtime = AgentRuntime::Unmanaged {
                    actor: session.actor.clone(),
                    parent: parent_cancels.clone(),
                    owner: crate::orchestration::plugin_authority(lua, plugin.clone()),
                    plugin,
                    generation,
                };
                if validate_unmanaged(lua, &ctx, &runtime).is_ok()
                    && session.actor.snapshot().lifecycle == ActorLifecycle::Open
                {
                    table.push(LuaAgentRef {
                        manager: runtime,
                        id: Some(session.agent_id),
                        held: None,
                    })?;
                }
            }
        }
        return Ok((Some(table), None));
    }
    let (manager, id) = try_pair!(context_graph(lua, &ctx));
    try_pair!(scope(lua, &ctx, id));
    let table = lua.create_table()?;
    for node in manager.snapshot() {
        if ctx.trusted(lua).is_err() {
            let mut caller = id;
            let mut ancestor = false;
            loop {
                if caller == node.agent_id {
                    ancestor = true;
                    break;
                }
                let parent = try_pair!(manager.node(caller)).parent_id;
                let Some(parent) = parent else {
                    break;
                };
                caller = parent;
            }
            if !ancestor && scope(lua, &ctx, node.agent_id).is_err() {
                continue;
            }
        }
        if let Ok(trusted) = ctx.trusted(lua) {
            let Ok(target) = manager.lookup(node.agent_id) else {
                continue;
            };
            if trusted.validate_target(lua, &target).is_err() {
                continue;
            }
        }
        table.push(LuaAgentRef {
            held: Some(try_pair!(manager.lookup(node.agent_id))),
            manager: AgentRuntime::Managed(manager.clone()),
            id: Some(node.agent_id),
        })?;
    }
    Ok((Some(table), None))
}

/// Spawn a child with a copy of its parent's configuration.
///
/// Omitted tools inherit the parent's capability-filtered request tools and
/// cannot widen the parent's audience or mode policy. `tools = {}` excludes
/// all request tools. Explicit tools select the child's request definitions.
/// Unlike spawn, the compatibility Session API defaults to no request tools.
/// Unmanaged children use the current host task's authority, not a managed graph.
///
/// @param ctx LuaCtx Active agent context or trusted pinned parent.
/// @param opts table Optional creation fields:
///   model_spec (string): override the inherited model; no command brace grammar.
///   thinking (string|integer) and fast (boolean): override inherited options.
///   system (string): custom system prompt contribution; defaults to empty.
///   tools (array): request definitions; omitted inherits permitted parent tools.
///   local_tools (table): name-keyed Lua tool specs with description, input_schema,
///     handler(input), and optional capture_input for successful input capture.
///   name (string): display name. audience (string): defaults to "general_sub";
///     the child cannot widen the parent's capability ceiling.
///   mcp (boolean): defaults to true, subject to inherited restrictions.
///   output_schema (table): validated before creation; requires a valid final
///     report each turn through the Rust structured-output reporting tool.
///   silent (boolean): defaults to false; suppresses parent relay and delivery.
///   auto_deliver (boolean): defaults to true; managed children notify their
///     recorded parent once per settled turn. A wait or result read in the
///     parent's own turn returns the result directly and withdraws the
///     queued notification.
///   semaphore (maki.async.Semaphore): unmanaged concurrency limit; managed
///     children use their manager's capacity instead.
///   inherit_provider (boolean): defaults to false; reuse the caller's provider
///     and model, ignoring model_spec, when true.
/// @return (Agent?, string?) Nonowning child handle. Garbage collection does not close it.
#[lua_fn]
pub(super) async fn spawn(
    lua: Lua,
    ctx: UserDataRef<LuaCtx>,
    opts: Table,
) -> mlua::Result<Pair<LuaAgent>> {
    try_pair!(ctx.validate_origin(&lua));
    let task = lua
        .app_data_ref::<crate::runtime::TaskHandle>()
        .map(|task| Arc::downgrade(&task));
    if let Ok(host) = ctx.trusted_host_root(&lua) {
        try_pair!(ctx.host_root(&lua));
        if host.template.is_none() {
            return Ok(err_pair(UNSUPPORTED_OPERATION_ERROR));
        }
    } else if ctx.trusted(&lua).is_ok()
        || ctx
            .agent()
            .is_some_and(|agent| agent.managed_turn.is_some())
    {
        let (_, id) = try_pair!(context_graph(&lua, &ctx));
        try_pair!(scope(&lua, &ctx, id));
    } else {
        let context = try_pair!(ctx.agent().ok_or_else(|| ctx.cap_err("orchestration")));
        let handle = try_pair!(
            lua.app_data_ref::<crate::runtime::TaskHandle>()
                .map(|task| Arc::clone(&task))
                .ok_or(NO_TASK_AUTHORITY)
        );
        let cell = crate::runtime::lock_cell(&handle);
        if !cell.scope_alive
            || cell.cancel.is_cancelled()
            || context.cancel.is_cancelled()
            || cell.managed_turn.is_some()
        {
            return Ok(err_pair(
                "unmanaged agent authority is not active in this invocation",
            ));
        }
    }
    let owner = ctx
        .trusted(&lua)
        .ok()
        .map(|trusted| Arc::clone(&trusted.plugin))
        .or_else(|| crate::runtime::current_plugin(&lua).map(|(plugin, _)| plugin));
    let (session, error) = build_session(lua.clone(), ctx, opts, true).await?;
    let Some(session) = session else {
        return Ok((None, error));
    };
    let manager = match &session.control {
        SessionControl::Managed { agent, .. } => AgentRuntime::Managed(agent.manager()),
        SessionControl::Unmanaged { parent_cancels, .. } => {
            let task = try_pair!(task.ok_or("no captured task authority"));
            let captured = try_pair!(task.upgrade().ok_or("unmanaged owner expired"));
            let active = try_pair!(
                lua.app_data_ref::<crate::runtime::TaskHandle>()
                    .map(|task| Arc::clone(&task))
                    .ok_or(NO_TASK_AUTHORITY)
            );
            let cell = crate::runtime::lock_cell(&active);
            if !Arc::ptr_eq(&active, &captured)
                || !cell.scope_alive
                || cell.cancel.is_cancelled()
                || cell.managed_turn.is_some()
            {
                return Ok(err_pair("unmanaged owner expired during preparation"));
            }
            drop(cell);
            AgentRuntime::Unmanaged {
                actor: Arc::clone(&session.actor),
                parent: Arc::clone(parent_cancels),
                plugin: try_pair!(crate::runtime::current_plugin(&lua).ok_or(NO_PLUGIN_AUTHORITY))
                    .0,
                generation: try_pair!(
                    crate::runtime::current_plugin(&lua).ok_or(NO_PLUGIN_AUTHORITY)
                )
                .1,
                owner: crate::orchestration::plugin_authority(
                    &lua,
                    try_pair!(crate::runtime::current_plugin(&lua).ok_or(NO_PLUGIN_AUTHORITY)).0,
                ),
            }
        }
    };
    let agent = if let AgentRuntime::Managed(manager) = manager {
        let mut agent = try_pair!(lookup(manager, session.agent_id, &lua));
        agent.state = Some(Arc::clone(&session.state));
        agent
    } else {
        LuaAgent {
            manager,
            id: Some(session.agent_id),
            state: Some(Arc::clone(&session.state)),
            held: None,
            root: None,
            ancestors: Vec::new(),
        }
    };
    if let Some(owner) = owner {
        let live_ids = lua
            .app_data_ref::<AgentSessions>()
            .map(|sessions| {
                sessions
                    .0
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .keys()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if let Some(mut owners) = lua.app_data_mut::<AgentOwners>() {
            owners.0.retain(|id, _| live_ids.contains(id));
        }
        if lua.app_data_ref::<AgentOwners>().is_none() {
            lua.set_app_data(AgentOwners::default());
        }
        if let Some(mut owners) = lua.app_data_mut::<AgentOwners>() {
            owners.0.insert(session.agent_id, owner);
        }
    }
    let actor = Arc::clone(&session.actor);
    let events = actor.subscribe_events();
    if lua.app_data_ref::<AgentSessions>().is_none() {
        lua.set_app_data(AgentSessions::default());
    }
    let sessions = try_pair!(
        lua.app_data_ref::<AgentSessions>()
            .map(|sessions| sessions.clone())
            .ok_or("agent session registry is unavailable")
    );
    sessions
        .0
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .insert(
            try_pair!(agent.id.ok_or(UNSUPPORTED_OPERATION_ERROR)),
            session,
        );
    let id = try_pair!(agent.id.ok_or(UNSUPPORTED_OPERATION_ERROR));
    let snapshot = actor.snapshot();
    if snapshot.lifecycle != ActorLifecycle::Open && snapshot.active_turn.is_none() {
        let retired = sessions
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&id);
        drop(retired);
        return Ok((Some(agent), None));
    }
    smol::spawn(async move {
        while let Ok(event) = events.recv_async().await {
            if matches!(event, maki_agent::ActorEvent::Close { .. }) {
                let retired = sessions
                    .0
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .remove(&id);
                drop(retired);
                break;
            }
        }
    })
    .detach();
    Ok((Some(agent), None))
}

/// Read the stable agent ID.
/// @return (string?, string?) Agent ID.
#[lua_fn(name = "id")]
fn agent_id(_lua: &Lua, this: &LuaAgent) -> mlua::Result<Pair<String>> {
    Ok((Some(this.identity()), None))
}

/// Read lifecycle, active turn, queue size, and cumulative usage.
/// @param ctx LuaCtx Invocation context.
/// @return (table?, string?) Fields: id (string), status ("idle", "running", or "closed"), queued (integer), active_turn (string or nil), input_tokens and output_tokens (cumulative integers). Authorized held managed handles retain a closed snapshot after graph removal. Host roots expose only id and status = "running".
#[lua_fn(name = "status")]
fn agent_status(lua: &Lua, this: &LuaAgent, ctx: UserDataRef<LuaCtx>) -> mlua::Result<Pair<Table>> {
    try_pair!(observe(lua, &ctx, this));
    if matches!(this.manager, AgentRuntime::HostRoot { .. }) {
        let table = lua.create_table()?;
        table.set("id", this.identity())?;
        table.set("status", "running")?;
        return Ok((Some(table), None));
    }
    let snapshot = if let Some(held) = &this.held {
        try_pair!(
            try_pair!(held.snapshot())
                .actor
                .ok_or("agent snapshot is unavailable")
        )
    } else {
        try_pair!(this.manager.actor(this.id)).snapshot()
    };
    let table = lua.create_table()?;
    table.set("id", this.identity())?;
    table.set(
        "status",
        match snapshot.lifecycle {
            ActorLifecycle::Closed | ActorLifecycle::Shutdown => "closed",
            ActorLifecycle::Open => match snapshot.status {
                ActorStatus::Idle => "idle",
                _ => "running",
            },
        },
    )?;
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
    if let AgentRuntime::HostRoot {
        services,
        reference,
    } = &this.manager
    {
        return Ok((
            Some(host_selection_table(
                lua,
                &try_pair!(services.model(reference)),
            )?),
            None,
        ));
    }
    let actor = try_pair!(this.manager.actor(this.id));
    let config = try_pair!(
        actor
            .effective_config()
            .ok_or("agent configuration is unavailable")
    );
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
) -> mlua::Result<Pair<mlua::Value>> {
    try_pair!(authorize(lua, &ctx, this));
    if let AgentRuntime::HostRoot {
        services,
        reference,
    } = &this.manager
    {
        let table = lua.create_table()?;
        for model in try_pair!(services.available_models(reference)) {
            table.push(model)?;
        }
        return Ok((Some(mlua::Value::Table(table)), None));
    }
    let service =
        try_pair!(crate::orchestration::services(lua).ok_or("host model catalog is unavailable"));
    let target = try_pair!(this.manager.lookup(this.id));
    let models = try_pair!(service.available_models(&target));
    let table = lua.create_table()?;
    for model in models {
        table.push(model)?;
    }
    Ok((Some(mlua::Value::Table(table)), None))
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
    let snapshot = try_pair!(actor.transcript(maki_agent::TranscriptRequest {
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
fn agent_ref(lua: &Lua, this: &LuaAgent) -> mlua::Result<Pair<LuaAgentRef>> {
    let _ = lua;
    Ok((
        Some(LuaAgentRef {
            manager: this.manager.clone(),
            id: this.id,
            held: this.held.clone(),
        }),
        None,
    ))
}

fn admit(
    lua: &Lua,
    agent: &LuaAgent,
    ctx: &LuaCtx,
    message: String,
    idle: bool,
    after_turn: Option<TurnId>,
) -> Result<LuaAgentTurn, String> {
    authorize(lua, ctx, agent)?;
    let actor = agent
        .manager
        .actor(agent.id)
        .map_err(|error| error.to_string())?;
    let config = actor
        .effective_config()
        .ok_or("agent configuration is unavailable")?;
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
        lease_committer: None,
    };
    let provenance = if let Ok(trusted) = ctx.trusted(lua) {
        maki_agent::TurnProvenance {
            origin: maki_agent::TurnOrigin::Plugin,
            plugin: Some(trusted.plugin.to_string()),
            plugin_generation: Some(trusted.generation),
            ..Default::default()
        }
    } else {
        let current = crate::runtime::current_managed_turn(lua);
        maki_agent::TurnProvenance {
            origin: maki_agent::TurnOrigin::Plugin,
            plugin: crate::runtime::current_plugin(lua).map(|plugin| plugin.0.to_string()),
            plugin_generation: crate::runtime::current_plugin(lua).map(|plugin| plugin.1),
            source_agent: current.as_ref().map(CurrentManagedTurn::agent_id),
            source_turn: current.as_ref().map(CurrentManagedTurn::turn_id),
        }
    };
    let ticket = actor
        .admit_turn_with_options(
            input,
            None,
            String::new(),
            maki_agent::TurnAdmissionOptions {
                idle_only: idle,
                after_turn,
                provenance,
                ..Default::default()
            },
        )
        .map_err(|error| error.to_string())?;
    Ok(LuaAgentTurn {
        agent: agent.clone(),
        ticket,
    })
}

/// Admit a turn only when this agent and its descendants are idle.
/// @param ctx LuaCtx Invocation context.
/// @param message string User message.
/// @param opts table? Optional after_turn ID required to match the latest completed turn.
/// @return (AgentTurn?, string?) Exact-turn ticket.
#[lua_fn(name = "send")]
fn agent_send(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    message: String,
    opts: Option<Table>,
) -> mlua::Result<Pair<LuaAgentTurn>> {
    let after_turn = opts
        .map(|opts| opts.get::<Option<String>>("after_turn"))
        .transpose()?
        .flatten();
    let after_turn = try_pair!(after_turn.map(|id| id.parse::<TurnId>()).transpose());
    Ok((
        Some(try_pair!(admit(lua, this, &ctx, message, true, after_turn))),
        None,
    ))
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
    Ok((
        Some(try_pair!(admit(lua, this, &ctx, message, false, None))),
        None,
    ))
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
) -> mlua::Result<(Option<Table>, Option<mlua::Value>)> {
    let after_turn = opts
        .as_ref()
        .map(|opts| opts.get::<Option<String>>("after_turn"))
        .transpose()?
        .flatten();
    let after_turn = match after_turn.map(|id| id.parse::<TurnId>()).transpose() {
        Ok(value) => value,
        Err(error) => {
            return Ok((
                None,
                Some(mlua::Value::String(lua.create_string(error.to_string())?)),
            ));
        }
    };
    let turn = match admit(&lua, &this, &ctx, message, true, after_turn) {
        Ok(turn) => turn,
        Err(error) => return Ok((None, Some(mlua::Value::String(lua.create_string(error)?)))),
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
        return Ok((None, Some(mlua::Value::Table(timeout))));
    }
    Ok((
        result,
        error
            .map(|error| lua.create_string(error).map(mlua::Value::String))
            .transpose()?,
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
    patch: mlua::Value,
) -> mlua::Result<Pair<Table>> {
    try_pair!(authorize(&lua, &ctx, &this));
    let patch = match patch {
        mlua::Value::String(spec) => {
            let table = lua.create_table()?;
            table.set("spec", spec)?;
            table
        }
        mlua::Value::Table(table) => table,
        _ => return Ok(err_pair("model patch must be a spec string or table")),
    };
    let selection = try_pair!(lua.from_value::<ModelSelection>(mlua::Value::Table(patch)));
    if let AgentRuntime::HostRoot {
        services,
        reference,
    } = &this.manager
    {
        let validator = try_pair!(ctx.host_model_validator(&lua, services, reference));
        let result = services
            .set_model_guarded(reference, selection, Some(validator))
            .await;
        try_pair!(ctx.validate_origin(&lua));
        let selection = try_pair!(result);
        try_pair!(authorize(&lua, &ctx, &this));
        return Ok((Some(host_selection_table(&lua, &selection)?), None));
    }
    let spec = selection.spec.map(|spec| spec.to_string());
    let thinking = selection.thinking;
    let fast = selection.fast;
    let actor = try_pair!(this.manager.actor(this.id));
    let reservation = try_pair!(actor.reserve_config_update());
    let template = try_pair!(destination_template(&lua, &this));
    let prepare = lua
        .app_data_ref::<super::SessionProviderPreparer>()
        .map(|prepare| Arc::clone(&prepare));
    let model = if spec.is_some() {
        let (model, provider) =
            try_pair!(super::build_session_provider(&spec, false, &template, prepare).await);
        Some(maki_agent::actor::PreparedModel { model, provider })
    } else {
        None
    };
    try_pair!(authorize(&lua, &ctx, &this));
    try_pair!(
        reservation.resolve(Ok(maki_agent::actor::ConfigChange::Patch(
            maki_agent::actor::ConfigPatch {
                model,
                thinking,
                fast,
                ..Default::default()
            }
        )))
    );
    drop(this);
    drop(ctx);
    let commit = try_pair!(reservation.wait().await);
    Ok((Some(selection_table(&lua, &commit.config)?), None))
}

fn mode_is_restricted(context: &super::AgentContext) -> bool {
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
    let config = try_pair!(
        try_pair!(this.manager.actor(this.id))
            .effective_config()
            .ok_or("agent configuration is unavailable")
    );
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
    let template = try_pair!(destination_template(&lua, &this));
    if ctx.trusted(&lua).is_err()
        && (mode_is_restricted(&template) || ctx.agent().is_some_and(mode_is_restricted))
    {
        return Ok(err_pair(RESTRICTED_MODE_CHANGE));
    }
    let definition = try_pair!(
        template
            .modes
            .get_by_key(&name)
            .ok_or_else(|| format!("unknown mode: {name}"))
    );
    let actor = try_pair!(this.manager.actor(this.id));
    let reservation = try_pair!(actor.reserve_config_update());
    let mode = match name.as_str() {
        "build" => maki_agent::AgentMode::Build,
        "plan" => {
            let path = template.mode.plan_path().map(|path| path.to_path_buf());
            let path = match path {
                Some(path) => path,
                None => {
                    if let Some(prepare) = crate::orchestration::services(&lua)
                        .and_then(|service| service.plan_path_preparer())
                    {
                        try_pair!(prepare())
                    } else {
                        let state_dir = lua
                            .app_data_ref::<crate::api::env::StateDirOverride>()
                            .map(|dir| maki_storage::StateDir::from_path(dir.0.clone()));
                        try_pair!(
                            smol::unblock(move || {
                                let dir = state_dir
                                    .map(Ok)
                                    .unwrap_or_else(maki_storage::StateDir::resolve)?;
                                maki_storage::plans::new_plan_path(&dir)
                            })
                            .await
                        )
                    }
                }
            };
            try_pair!(authorize(&lua, &ctx, &this));
            maki_agent::AgentMode::Plan(path)
        }
        _ => maki_agent::AgentMode::Custom(maki_agent::ModeId::parse(&name)),
    };
    try_pair!(
        reservation.resolve(Ok(maki_agent::actor::ConfigChange::Mode {
            mode: mode.clone(),
            mode_def: Some(definition)
        }))
    );
    drop(this);
    drop(ctx);
    let commit = try_pair!(reservation.wait().await);
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

/// Read the stable turn ID.
/// @return (string?, string?) Turn ID.
#[lua_fn(name = "id")]
fn turn_id(_lua: &Lua, this: &LuaAgentTurn) -> mlua::Result<Pair<String>> {
    Ok((Some(this.ticket.turn_id().to_string()), None))
}

/// Read the target agent ID.
/// @return (string?, string?) Agent ID.
#[lua_fn(name = "agent_id")]
fn turn_agent_id(lua: &Lua, this: &LuaAgentTurn) -> mlua::Result<Pair<String>> {
    let _ = lua;
    Ok((Some(this.agent.identity()), None))
}

fn turn_result(lua: &Lua, ticket: &TurnTicket) -> mlua::Result<Pair<Table>> {
    let result = try_pair!(ticket.peek_result().ok_or(UNSETTLED_TURN_RESULT));
    match json_to_lua(lua, &result_json(&result))? {
        mlua::Value::Table(table) => Ok((Some(table), None)),
        _ => Err(mlua::Error::runtime(
            "turn result projection is not a table",
        )),
    }
}

/// Read a retained result without waiting or consuming it.
/// @param ctx LuaCtx Invocation context.
/// @return (table?, string?) Nil without error while pending.
#[lua_fn(name = "result")]
fn ticket_result(
    lua: &Lua,
    this: &LuaAgentTurn,
    ctx: UserDataRef<LuaCtx>,
) -> mlua::Result<Pair<Table>> {
    let active = try_pair!(observe(lua, &ctx, &this.agent));
    if this.ticket.peek().is_none() {
        return Ok((None, None));
    }
    withdraw_delivery(active.as_ref(), this.ticket.turn_id());
    turn_result(lua, &this.ticket)
}

/// A result returned to the parent's own turn makes its queued delivery
/// redundant.
fn withdraw_delivery(active: Option<&CurrentManagedTurn>, turn_id: TurnId) {
    if let Some(parent) = active.and_then(|active| active.manager().actor(active.agent_id()).ok()) {
        parent.withdraw_child_delivery(turn_id);
    }
}

/// Wait for this exact turn. Timeout stops waiting, not the agent.
/// @param ctx LuaCtx Invocation context.
/// @param opts table? timeout is a nonnegative integer number of seconds. Zero returns nil, nil while pending; positive timeout returns nil and "agent turn wait timed out" without cancelling work.
/// @return (table?, string?) Per-turn result or error.
#[lua_fn(name = "wait")]
async fn ticket_wait(
    lua: Lua,
    this: UserDataRef<LuaAgentTurn>,
    ctx: UserDataRef<LuaCtx>,
    opts: Option<Table>,
) -> mlua::Result<Pair<Table>> {
    let active = try_pair!(observe(&lua, &ctx, &this.agent));
    let agent = this.agent.clone();
    let ticket = this.ticket.clone();
    let retained_ticket = ticket.clone();
    let timeout = opts
        .map(|opts| opts.get::<Option<u64>>("timeout"))
        .transpose()?
        .flatten();
    if ticket.peek().is_some() {
        withdraw_delivery(active.as_ref(), ticket.turn_id());
        return turn_result(&lua, &ticket);
    }
    if timeout == Some(0) {
        return Ok((None, None));
    }
    drop(this);
    if let Some(active) = &active {
        if Some(active.agent_id()) == agent.id {
            return Ok(err_pair("cannot wait for a turn on the calling agent"));
        }
        let actor = try_pair!(agent.manager.actor(agent.id));
        let wait = try_pair!(active.lease().observe_descendant(
            active,
            try_pair!(agent.id.ok_or(UNSUPPORTED_OPERATION_ERROR)),
            &actor,
            ticket,
            timeout.map(Duration::from_secs)
        ));
        try_pair!(wait.wait().await.map_err(|error| match error {
            maki_agent::PromptWaitError::Timeout => TURN_WAIT_TIMEOUT.to_string(),
            maki_agent::PromptWaitError::Cancelled => crate::runtime::CANCELLED_MSG.to_string(),
        }));
    } else {
        match timeout {
            Some(seconds) => try_pair!(
                futures_lite::future::race(async { Ok(ticket.wait().await) }, async {
                    smol::Timer::after(Duration::from_secs(seconds)).await;
                    Err(TURN_WAIT_TIMEOUT)
                })
                .await
            ),
            None => ticket.wait().await,
        };
    }
    try_pair!(ctx.validate_origin(&lua));
    withdraw_delivery(active.as_ref(), retained_ticket.turn_id());
    turn_result(&lua, &retained_ticket)
}

/// Cancel only this admitted turn.
/// @param ctx LuaCtx Invocation context.
/// @return (boolean?, string?) Success or error.
#[lua_fn(name = "cancel")]
fn ticket_cancel(
    lua: &Lua,
    this: &LuaAgentTurn,
    ctx: UserDataRef<LuaCtx>,
) -> mlua::Result<Pair<bool>> {
    try_pair!(authorize(lua, &ctx, &this.agent));
    try_pair!(
        try_pair!(this.agent.manager.actor(this.agent.id)).cancel_turn(this.ticket.turn_id())
    );
    Ok((Some(true), None))
}

struct LuaAgentSubscription {
    id: u64,
    stop: flume::Sender<()>,
}

fn subscribe(
    lua: &Lua,
    agent: &LuaAgent,
    ctx: &LuaCtx,
    handler: Function,
    kind: &'static str,
) -> mlua::Result<Pair<LuaAgentSubscription>> {
    if matches!(agent.manager, AgentRuntime::HostRoot { .. }) {
        try_pair!(validate_host_root(lua, ctx, agent));
        return Ok(err_pair(UNSUPPORTED_OPERATION_ERROR));
    }
    try_pair!(ctx.trusted(lua).map_err(|_| TRUSTED_SUBSCRIPTION_REQUIRED));
    try_pair!(authorize(lua, ctx, agent));
    let plugin = try_pair!(crate::runtime::current_plugin(lua).ok_or("no active plugin identity"));
    let target = try_pair!(agent.manager.lookup(agent.id));
    let service = try_pair!(crate::orchestration::services(lua).ok_or(NO_ORCHESTRATION_SERVICES));
    let template = try_pair!(service.template(&target));
    let tx = try_pair!(
        lua.app_data_ref::<crate::runtime::RequestTx>()
            .map(|tx| tx.0.clone())
            .ok_or("agent event dispatch is unavailable")
    );
    let actor = try_pair!(target.actor());
    let events = actor.subscribe_leased_events();
    let id = crate::orchestration::register_event_callback(lua, plugin.0, handler)?;
    let cancel = try_pair!(
        crate::orchestration::event_callback(lua, id)
            .map(|callback| callback.2)
            .ok_or("event registration expired")
    );
    let (stop, stopped) = flume::bounded(1);
    let keep_subscription_open = stop.clone();
    smol::spawn(async move {
        let _keep_subscription_open = keep_subscription_open;
        loop {
            let event = futures_lite::future::race(async { events.recv_async().await.ok() }, async { let _ = cancel.race(stopped.recv_async()).await; None }).await;
            let Some(event) = event else { break; };
            let (event, lease) = event.into_parts();
            let payload = match (kind, event) {
                ("start", maki_agent::ActorEvent::Start { agent_id, turn_id, provenance, config }) => serde_json::json!({
                    "agent_id": agent_id.to_string(), "turn_id": turn_id.to_string(),
                    "origin": origin_json(&provenance), "selection": config.map(|commit| selection_json(&commit.config)),
                }),
                ("end", maki_agent::ActorEvent::End(result)) => result_json(&result),
                ("idle", maki_agent::ActorEvent::Idle { agent_id }) => serde_json::json!({"agent_id": agent_id.to_string()}),
                ("config", maki_agent::ActorEvent::Config(commit)) => serde_json::json!({"agent_id": target.id().to_string(), "generation": commit.generation, "selection": selection_json(&commit.config), "mode": commit.config.mode.id().key()}),
                ("close", maki_agent::ActorEvent::Close { agent_id, lifecycle }) => serde_json::json!({"agent_id": agent_id.to_string(), "lifecycle": format!("{lifecycle:?}").to_lowercase()}),
                _ => continue,
            };
            if tx.send(crate::runtime::Request::AgentEvent(Box::new(crate::orchestration::AgentEventRequest { callback_id: id, payload, lease, target: crate::orchestration::TrustedTarget { target: target.clone(), template: template.clone() } }))).is_err() { break; }
        }
    }).detach();
    Ok((Some(LuaAgentSubscription { id, stop }), None))
}

fn host_selection_table(lua: &Lua, selection: &CompleteModelSelection) -> mlua::Result<Table> {
    match lua.to_value(selection)? {
        mlua::Value::Table(table) => Ok(table),
        _ => Err(mlua::Error::runtime("selection projection is not a table")),
    }
}

fn selection_table(lua: &Lua, config: &maki_agent::EffectiveAgentConfig) -> mlua::Result<Table> {
    match json_to_lua(lua, &selection_json(config))? {
        mlua::Value::Table(table) => Ok(table),
        _ => Err(mlua::Error::runtime("selection projection is not a table")),
    }
}

fn selection_json(config: &maki_agent::EffectiveAgentConfig) -> serde_json::Value {
    serde_json::json!(CompleteModelSelection::from_model(
        &config.model,
        RequestOptions {
            thinking: config.thinking,
            fast: config.fast
        }
    ))
}

fn origin_json(origin: &maki_agent::TurnProvenance) -> serde_json::Value {
    serde_json::json!({
        "kind": match origin.origin { maki_agent::TurnOrigin::Internal => "internal", maki_agent::TurnOrigin::User => "user", maki_agent::TurnOrigin::Plugin => "plugin" },
        "plugin": origin.plugin, "plugin_generation": origin.plugin_generation,
        "source_agent": origin.source_agent.map(|id| id.to_string()), "source_turn": origin.source_turn.map(|id| id.to_string()),
    })
}

fn result_json(result: &maki_agent::TurnResult) -> serde_json::Value {
    let outcome = &result.outcome;
    let mut value = serde_json::json!({"agent_id": outcome.agent_id().to_string(), "turn_id": outcome.turn_id().to_string(), "text": result.text, "usage": outcome.usage(), "output": result.output, "origin": origin_json(&result.provenance)});
    let status = match outcome {
        maki_agent::TurnOutcome::Completed { .. } => "completed",
        maki_agent::TurnOutcome::Failed { failure, .. } => {
            value["error"] = failure.diagnostic.clone().into();
            "failed"
        }
        maki_agent::TurnOutcome::Cancelled { reason, .. } => {
            value["cancellation_reason"] = reason.to_string().into();
            "cancelled"
        }
    };
    value["status"] = status.into();
    value["duration_ms"] = result.duration_ms.into();
    value["input_tokens"] = outcome.usage().total_input().into();
    value["output_tokens"] = outcome.usage().output.into();
    if let Some(output) = result.output.last() {
        value["captured"] = output.clone();
    }
    if let maki_agent::TurnOutcome::Failed { failure, .. } = outcome {
        value["user_message"] = failure.user_message.clone().into();
        value["retryable"] = failure.retryable.into();
    }
    value
}

/// Subscribe to future turn admissions. Handlers may overlap.
/// @param ctx LuaCtx Trusted invocation context.
/// @param handler function Called as handler(payload, ctx) with a fresh trusted scoped context. Payload fields: agent_id, turn_id, origin, selection (normalized {spec, thinking, fast}, when available). origin contains kind ("user", "plugin", or "internal"), plugin?, plugin_generation?, source_agent?, source_turn?. Provenance does not grant authority.
/// @return (AgentSubscription?, string?) Revocable subscription.
#[lua_fn(name = "on_turn_start")]
fn on_turn_start(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    handler: Function,
) -> mlua::Result<Pair<LuaAgentSubscription>> {
    subscribe(lua, this, &ctx, handler, "start")
}

/// Subscribe to exact settled turn results. Handlers may overlap.
/// @param ctx LuaCtx Trusted invocation context.
/// @param handler function Called as handler(result, ctx). result has the same fields as AgentTurn:result and AgentTurn:wait. Each invocation has a fresh trusted scoped context.
/// @return (AgentSubscription?, string?) Revocable subscription.
#[lua_fn(name = "on_turn_end")]
fn on_turn_end(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    handler: Function,
) -> mlua::Result<Pair<LuaAgentSubscription>> {
    subscribe(lua, this, &ctx, handler, "end")
}

/// Subscribe to transitions to no pending turn work.
/// @param ctx LuaCtx Trusted invocation context.
/// @param handler function Called as handler({agent_id}, ctx) with a fresh trusted scoped context. Idle is an observation, not a reservation against new work.
/// @return (AgentSubscription?, string?) Revocable subscription.
#[lua_fn(name = "on_idle")]
fn on_idle(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    handler: Function,
) -> mlua::Result<Pair<LuaAgentSubscription>> {
    subscribe(lua, this, &ctx, handler, "idle")
}

/// Subscribe to committed model or mode changes.
/// @param ctx LuaCtx Trusted invocation context.
/// @param handler function Called as handler(payload, ctx). Payload fields: agent_id, generation (committed integer), selection (normalized {spec, thinking, fast}), mode (registered name). The context is fresh and trusted.
/// @return (AgentSubscription?, string?) Revocable subscription.
#[lua_fn(name = "on_config_change")]
fn on_config_change(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    handler: Function,
) -> mlua::Result<Pair<LuaAgentSubscription>> {
    subscribe(lua, this, &ctx, handler, "config")
}

/// Subscribe to permanent lifecycle closure.
/// @param ctx LuaCtx Trusted invocation context.
/// @param handler function Called as handler({agent_id, lifecycle}, ctx). lifecycle is "closed" or "shutdown". The context is fresh and trusted.
/// @return (AgentSubscription?, string?) Revocable subscription.
#[lua_fn(name = "on_close")]
fn on_close(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    handler: Function,
) -> mlua::Result<Pair<LuaAgentSubscription>> {
    subscribe(lua, this, &ctx, handler, "close")
}

/// Revoke future deliveries and cancel active handlers.
/// @return (boolean?, string?) Success.
#[lua_fn(name = "close")]
fn subscription_close(lua: &Lua, this: &LuaAgentSubscription) -> mlua::Result<Pair<bool>> {
    crate::orchestration::close_event_callback(lua, this.id);
    let _ = this.stop.try_send(());
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
    if matches!(this.manager, AgentRuntime::HostRoot { .. }) {
        return Ok(err_pair(UNSUPPORTED_OPERATION_ERROR));
    }
    let manager = this.manager.clone();
    let id = this.id;
    let sessions = lua
        .app_data_ref::<AgentSessions>()
        .map(|sessions| sessions.clone());
    try_pair!(crate::runtime::register_defer_close(
        lua,
        Box::new(move || {
            let _ = manager.close_subtree(id);
            if let Some(sessions) = sessions {
                let retired = {
                    let mut sessions = sessions.0.lock().unwrap_or_else(|error| error.into_inner());
                    let ids: Vec<_> = sessions
                        .iter()
                        .filter(|(_, session)| session_settled_closed(session))
                        .map(|(id, _)| *id)
                        .collect();
                    ids.into_iter()
                        .filter_map(|id| sessions.remove(&id))
                        .collect::<Vec<_>>()
                };
                drop(retired);
            }
        })
    ));
    Ok((Some(true), None))
}

lua_class! {
    /// A revocable subscription. Garbage collection does not unsubscribe.
    /// Close explicitly to cancel callbacks; plugin unload also revokes registration.
    "maki.agent.AgentSubscription" => LuaAgentSubscription, SUBSCRIPTION_DOCS [subscription_close]
}

/// Read a reference's stable identity.
/// @return (string?, string?) Agent ID.
#[lua_fn(name = "id")]
fn reference_id(_lua: &Lua, this: &LuaAgentRef) -> mlua::Result<Pair<String>> {
    let agent = LuaAgent {
        manager: this.manager.clone(),
        id: this.id,
        state: None,
        held: this.held.clone(),
        root: None,
        ancestors: Vec::new(),
    };
    Ok((Some(agent.identity()), None))
}

lua_class! {
    /// Visibility-only identity. Resolve with get(ctx, reference) to request control.
    "maki.agent.AgentRef" => LuaAgentRef, REF_DOCS [reference_id]
}

fn agent_internal_methods<M: mlua::UserDataMethods<LuaAgent>>(methods: &mut M) {
    methods.add_method("_maki_task_owner", |lua, this, ()| {
        Ok(super::LuaTaskOwner {
            owner_id: this.state.as_ref().and_then(|state| state.parent_agent_id),
            authority: match &this.manager {
                AgentRuntime::Managed(manager) => {
                    let (plugin, generation) = crate::runtime::current_plugin(lua)
                        .ok_or_else(|| mlua::Error::runtime(NO_PLUGIN_AUTHORITY))?;
                    super::TaskOwnerAuthority::Agent {
                        owner: crate::orchestration::plugin_authority(lua, plugin.clone()),
                        plugin,
                        generation,
                        target: manager
                            .lookup(
                                this.id.ok_or_else(|| {
                                    mlua::Error::runtime(UNSUPPORTED_OPERATION_ERROR)
                                })?,
                            )
                            .map_err(mlua::Error::external)?,
                    }
                }
                AgentRuntime::HostRoot { .. } => {
                    return Err(mlua::Error::runtime(UNSUPPORTED_OPERATION_ERROR));
                }
                AgentRuntime::Unmanaged { parent, .. } => {
                    let (plugin, generation) = crate::runtime::current_plugin(lua)
                        .ok_or_else(|| mlua::Error::runtime(NO_PLUGIN_AUTHORITY))?;
                    super::TaskOwnerAuthority::StandaloneAgent {
                        owner: crate::orchestration::plugin_authority(lua, plugin.clone()),
                        plugin,
                        generation,
                        parent: parent.clone(),
                    }
                }
            },
            child_id: this
                .id
                .ok_or_else(|| mlua::Error::runtime(UNSUPPORTED_OPERATION_ERROR))?,
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
    /// Internal compatibility adapters are supported only for the bundled task plugin:
    /// _maki_session_id() reads the existing task/UI identity without granting control;
    /// _maki_task_owner() returns a host-stamped owner guard whose _maki_can_control()
    /// checks the current invocation's plugin generation, host and permitted parent branch.
    /// The guard cannot authorize effects, transfer control or revive expired authority.
    /// Each effect still uses the public ctx-taking Agent or AgentTurn operation.
    /// Third-party plugins should use id(), explicit references and public scoped operations;
    /// these underscore-prefixed adapters are not a stable public API.
    "maki.agent.Agent" => LuaAgent, AGENT_DOCS [agent_id, agent_status, agent_mode, agent_model, agent_available_models, agent_transcript, agent_ref, agent_send, agent_enqueue, agent_prompt, agent_set_model, agent_set_mode, agent_cancel, agent_cancel_subtree, agent_close, agent_defer_close, on_turn_start, on_turn_end, on_idle, on_config_change, on_close]
    extra agent_internal_methods
}
lua_class! {
    /// An exact-turn ticket with repeatable retained result reads. Dropping it does not cancel work.
    /// Settled results remain readable with current valid authority after lookup eviction
    /// or agent close. Result fields: agent_id, turn_id, status ("completed", "failed",
    /// or "cancelled"), text (last assistant message, including partial failure text),
    /// duration_ms (immutable elapsed milliseconds for this turn), usage,
    /// input_tokens, output_tokens (per-turn counters), output (captured array),
    /// origin. usage contains input_tokens, output_tokens, cache_creation_input_tokens
    /// and cache_read_input_tokens; top-level input_tokens includes cached tokens.
    /// captured is the last output when present. Failed results include error,
    /// user_message and retryable; cancelled results include cancellation_reason.
    /// origin contains kind ("user", "plugin", or "internal"), plugin?,
    /// plugin_generation?, source_agent?, source_turn?. It conveys provenance only.
    "maki.agent.AgentTurn" => LuaAgentTurn, TURN_DOCS [turn_id, turn_agent_id, ticket_result, ticket_wait, ticket_cancel]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::agent::{
        LuaActorBackend,
        structured_output::{StructuredOutput, TOOL_NAME},
        tests::{StreamOnceProvider, canned_reply_with_usage, session_with_provider},
    };
    use crate::runtime::{TaskScope, lock_cell};
    use maki_agent::{
        ActorBackend, AgentLimits, BackendResult, ControlWork, History, TurnContext, WorkKind,
        tools::test_support::stub_ctx,
    };
    use maki_providers::TokenUsage;
    use std::{future::Future, pin::Pin};

    const REPLY: &str = "retained reply";
    const RETIRE_TIMEOUT: Duration = Duration::from_secs(5);
    const PLUGIN: &str = "agent-test";
    const FOREIGN_PLUGIN: &str = "foreign-agent-test";
    const WRONG_PLUGIN: &str = "agent belongs to another plugin generation";

    fn plugin_scope(lua: &Lua, plugin: &str) -> TaskScope {
        let scope = TaskScope::detached(lua);
        let plugin: Arc<str> = Arc::from(plugin);
        let owner = crate::orchestration::plugin_authority(lua, plugin.clone());
        lock_cell(scope.handle()).plugin_origin = Some((plugin, owner.generation()));
        scope
    }

    struct TestServices(crate::orchestration::TrustedTarget);

    impl crate::orchestration::OrchestrationServices for TestServices {
        fn resolve_target(
            &self,
            _: Option<&maki_commands::CommandInvocation>,
        ) -> Result<Option<crate::orchestration::TrustedTarget>, String> {
            Ok(Some(self.0.clone()))
        }

        fn register_target(
            &self,
            target: crate::orchestration::TrustedTarget,
        ) -> Result<(), String> {
            if target.target.same_manager(&self.0.target) {
                Ok(())
            } else {
                Err("wrong test manager".into())
            }
        }

        fn template(
            &self,
            target: &maki_agent::AgentRef,
        ) -> Result<maki_agent::tools::ToolContext, String> {
            if target.same_manager(&self.0.target) {
                Ok(self.0.template.clone())
            } else {
                Err("wrong test manager".into())
            }
        }
    }

    #[test]
    fn standalone_current_returns_execution_child_not_installed_root() {
        let lua = Lua::new();
        let scope = TaskScope::detached(&lua);
        let provider = Arc::new(StreamOnceProvider::new_replies(Vec::new()));
        let (actor, state, session, _) = session_with_provider(provider, None, None);
        let id = session.agent_id;
        let mut context = stub_ctx(&maki_agent::AgentMode::Build);
        let services = Arc::new(HostRootServices::fixed(
            AgentId::generate(),
            &context.model,
            RequestOptions::default(),
            Arc::new(maki_config::ModelPolicy::default()),
        ));
        lua.set_app_data(crate::orchestration::HostRootServicesSlot {
            services: Some(services),
            template: Some(context.clone()),
        });
        context.ledger = state.params.get().unwrap().ledger.clone();
        lua.set_app_data(AgentSessions(Arc::new(Mutex::new(HashMap::from([(
            id, session,
        )])))));
        let mut ctx = LuaCtx::handler(&context);
        ctx.pin_host_root(&lua);
        ctx.bind_origin(scope.handle());
        let plugin: Arc<str> = Arc::from("child-tool");
        lock_cell(scope.handle()).plugin_origin = Some((plugin, 0));
        lock_cell(scope.handle()).delegated_tool = true;
        let userdata = lua.create_userdata(ctx).unwrap();
        let (child, error) = current(&lua, userdata.borrow().unwrap()).unwrap();
        assert!(error.is_none(), "{error:?}");
        let child = child.unwrap();
        assert_eq!(child.id, Some(id));
        assert!(matches!(child.manager, AgentRuntime::Unmanaged { .. }));
        actor.close();
    }

    #[test]
    fn subscription_gc_preserves_registration_until_registry_close() {
        let lua = Lua::new();
        let handler = lua.create_function(|_, ()| Ok(())).unwrap();
        let id =
            crate::orchestration::register_event_callback(&lua, Arc::from("gc-owner"), handler)
                .unwrap();
        let cancel = crate::orchestration::event_callback(&lua, id).unwrap().2;
        let (stop, stopped) = flume::bounded(1);
        let _keep_subscription_open = stop.clone();
        lua.globals()
            .set("subscription", LuaAgentSubscription { id, stop })
            .unwrap();
        lua.load("subscription = nil").exec().unwrap();
        lua.gc_collect().unwrap();
        assert_eq!(stopped.try_recv(), Err(flume::TryRecvError::Empty));
        assert!(!cancel.is_cancelled());
        assert!(crate::orchestration::event_callback(&lua, id).is_some());
        crate::orchestration::close_event_callback(&lua, id);
        assert!(cancel.is_cancelled());
        assert!(crate::orchestration::event_callback(&lua, id).is_none());
        lua.gc_collect().unwrap();
    }

    #[test]
    fn spawn_inherits_defaults_while_legacy_session_keeps_empty_tools() {
        smol::block_on(async {
            let lua = Lua::new();
            let scope = plugin_scope(&lua, PLUGIN);
            let mut context = stub_ctx(&maki_agent::AgentMode::Build);
            context.workflow = true;
            let mut expected = context.registry.definitions(
                &maki_agent::template::env_vars(),
                &maki_agent::tools::DescriptionContext {
                    filter: &context.tool_filter,
                    audience: context.audience,
                    workflow: context.workflow,
                    mcp: context.mcp.is_some(),
                },
                context.model.supports_tool_examples(),
            );
            expected.as_array_mut().unwrap().truncate(1);
            let structured = StructuredOutput::compile(serde_json::json!({
                "type": "object", "properties": {}
            }))
            .unwrap();
            let mut parent_definitions = expected.clone();
            parent_definitions
                .as_array_mut()
                .unwrap()
                .push(structured.definition());
            context.local_tools = Arc::new(HashMap::from([(
                TOOL_NAME.to_owned(),
                structured.local_tool(),
            )]));
            context.turn_bindings = Arc::new(maki_agent::tools::TurnToolBindings::capture(
                &context.registry,
                &context.local_tools,
                context.mcp.as_ref(),
            ));
            context.request_tools = Some(maki_agent::tools::RequestTools::assembled(
                parent_definitions,
                &context.config,
                &context.model,
            ));
            let mut ctx = LuaCtx::handler(&context);
            ctx.bind_origin(scope.handle());
            lua.globals().set("ctx", ctx).unwrap();
            lua.globals()
                .set("agent_api", super::super::create_agent_table(&lua).unwrap())
                .unwrap();
            let child: mlua::AnyUserData = scope
                .scope_future(
                    lua.load(
                        r#"
                return assert(agent_api.spawn(ctx, { inherit_provider = true, silent = true }))
            "#,
                    )
                    .eval_async(),
                )
                .await
                .unwrap();
            let child = child.borrow::<LuaAgent>().unwrap();
            let state = child.state.as_ref().unwrap();
            let template = state.template.get().unwrap();
            assert_eq!(template.audience, context.audience);
            assert!(
                child
                    .manager
                    .actor(child.id)
                    .unwrap()
                    .effective_config()
                    .unwrap()
                    .workflow
            );
            assert_eq!(state.tools.definitions(), &expected);
            assert!(!state.local_tools.contains_key(TOOL_NAME));
            assert!(state.structured.is_none());
            assert_eq!(
                template.request_tools.as_ref().unwrap().definitions(),
                state.tools.definitions()
            );
            let empty: mlua::AnyUserData = scope.scope_future(lua.load(r#"
                return assert(agent_api.spawn(ctx, { inherit_provider = true, silent = true, tools = {} }))
            "#).eval_async()).await.unwrap();
            let empty = empty.borrow::<LuaAgent>().unwrap();
            assert!(
                empty
                    .state
                    .as_ref()
                    .unwrap()
                    .tools
                    .definitions()
                    .as_array()
                    .unwrap()
                    .is_empty()
            );
            empty.manager.close_subtree(empty.id).unwrap();
            let legacy: mlua::AnyUserData = scope
                .scope_future(
                    lua.load(
                        r#"
                return assert(agent_api.session(ctx, { inherit_provider = true, silent = true }))
            "#,
                    )
                    .eval_async(),
                )
                .await
                .unwrap();
            let legacy = legacy.borrow::<LuaSession>().unwrap();
            assert!(
                legacy
                    .state
                    .tools
                    .definitions()
                    .as_array()
                    .unwrap()
                    .is_empty()
            );
            child.manager.close_subtree(child.id).unwrap();
        });
    }

    #[test]
    fn unmanaged_spawn_uses_actor_tickets_and_rejects_foreign_plugin_and_runtime() {
        smol::block_on(async {
            let lua = Lua::new();
            let scope = plugin_scope(&lua, PLUGIN);
            let mut context = stub_ctx(&maki_agent::AgentMode::Build);
            context.provider = Arc::new(StreamOnceProvider::new_replies(vec![
                canned_reply_with_usage(REPLY, TokenUsage::default()),
            ]));
            context.request_tools = Some(maki_agent::tools::RequestTools::default());
            let mut ctx = LuaCtx::handler(&context);
            ctx.bind_origin(scope.handle());
            lua.globals().set("ctx", ctx).unwrap();
            lua.globals()
                .set("agent_api", super::super::create_agent_table(&lua).unwrap())
                .unwrap();
            let result: String = scope
                .scope_future(
                    lua.load(
                        r#"
                local child, err = agent_api.spawn(ctx, { inherit_provider = true, silent = true })
                assert(child, err)
                retained_child = child
                local reference = assert(child:ref())
                assert(agent_api.get(ctx, reference):id() == child:id())
                local ticket = assert(child:send(ctx, "standalone request"))
                local result = assert(ticket:wait(ctx))
                assert(ticket:result(ctx).text == result.text)
                assert(child:transcript(ctx, { last_messages = 100, max_bytes = 65536 }))
                local current, current_err = agent_api.current(ctx)
                assert(current == nil and current_err ~= nil)
                return result.text
            "#,
                    )
                    .eval_async(),
                )
                .await
                .unwrap();
            assert_eq!(result, REPLY);
            drop(scope);
            let fresh = plugin_scope(&lua, PLUGIN);
            let mut ctx = LuaCtx::handler(&context);
            ctx.bind_origin(fresh.handle());
            lua.globals().set("ctx", ctx).unwrap();
            let retained: mlua::AnyUserData = lua.globals().get("retained_child").unwrap();
            let child = retained.borrow::<LuaAgent>().unwrap();
            let ctx: mlua::AnyUserData = lua.globals().get("ctx").unwrap();
            assert!(validate_unmanaged(&lua, &ctx.borrow().unwrap(), &child.manager).is_ok());
            {
                let foreign = plugin_scope(&lua, FOREIGN_PLUGIN);
                let mut ctx = LuaCtx::handler(&context);
                ctx.bind_origin(foreign.handle());
                assert_eq!(
                    validate_unmanaged(&lua, &ctx, &child.manager).unwrap_err(),
                    WRONG_PLUGIN
                );
            }
            {
                let other_lua = Lua::new();
                let foreign = plugin_scope(&other_lua, PLUGIN);
                let mut ctx = LuaCtx::handler(&context);
                ctx.bind_origin(foreign.handle());
                assert_eq!(
                    validate_unmanaged(&other_lua, &ctx, &child.manager).unwrap_err(),
                    WRONG_PLUGIN
                );
            }
            drop(child);
            let closed: bool = fresh
                .scope_future(lua.load("return retained_child:close(ctx)").eval_async())
                .await
                .unwrap();
            assert!(closed);
        });
    }

    #[test]
    fn abandoned_defer_close_scope_removes_retained_session_without_gc() {
        smol::block_on(async {
            let lua = Lua::new();
            let scope = TaskScope::detached(&lua);
            let context = stub_ctx(&maki_agent::AgentMode::Build);
            let config = maki_agent::EffectiveAgentConfig::new(
                maki_agent::RunSettings {
                    model: (*context.model).clone(),
                    provider: Arc::clone(&context.provider),
                    thinking: context.opts.thinking,
                    fast: context.opts.fast,
                    workflow: false,
                },
                context.mode.clone(),
            );
            let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
            let mut root_fixture = None;
            let target = manager
                .create_root_with_config(Some(config), Vec::new(), None, |id| {
                    let (_, state, session, _) =
                        super::super::tests::session_with_provider_for_agent(
                            Arc::clone(&context.provider),
                            id,
                        );
                    root_fixture = Some(session);
                    Ok::<_, String>(Box::new(LuaActorBackend::new(state)))
                })
                .unwrap();
            let trusted_target = crate::orchestration::TrustedTarget {
                target,
                template: context,
            };
            lua.set_app_data(crate::orchestration::OrchestrationServicesSlot(Some(
                Arc::new(TestServices(trusted_target.clone())),
            )));
            let ctx = LuaCtx::trusted_context(&lua, trusted_target, Arc::from("agent-test"));
            lua.globals().set("ctx", ctx).unwrap();
            lua.globals()
                .set("agent_api", super::super::create_agent_table(&lua).unwrap())
                .unwrap();
            let child: mlua::AnyUserData = scope
                .scope_future(
                    lua.load(
                        r#"
                local child = assert(agent_api.spawn(ctx, {
                    inherit_provider = true, silent = true, tools = {},
                }))
                assert(child:defer_close(ctx))
                return child
            "#,
                    )
                    .eval_async(),
                )
                .await
                .unwrap();
            let child = child.borrow::<LuaAgent>().unwrap();
            let actor = child.manager.lookup(child.id).unwrap().actor().unwrap();
            assert_eq!(actor.snapshot().lifecycle, ActorLifecycle::Open);
            let sessions = lua.app_data_ref::<AgentSessions>().unwrap().clone();
            assert!(
                sessions
                    .0
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .contains_key(&child.id.unwrap())
            );
            drop(scope);
            assert_ne!(actor.snapshot().lifecycle, ActorLifecycle::Open);
            futures_lite::future::race(
                async {
                    while sessions
                        .0
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .contains_key(&child.id.unwrap())
                    {
                        smol::future::yield_now().await;
                    }
                },
                async {
                    smol::Timer::after(RETIRE_TIMEOUT).await;
                    panic!("deferred session ownership was not released");
                },
            )
            .await;
            assert_eq!(actor.snapshot().lifecycle, ActorLifecycle::Closed);
            assert!(actor.snapshot().active_turn.is_none());
            manager.shutdown(RETIRE_TIMEOUT).await;
            drop(root_fixture);
        });
    }

    struct SettlementGate {
        backend: LuaActorBackend,
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
    }

    impl ActorBackend for SettlementGate {
        fn run_turn<'a>(
            &'a mut self,
            history: &'a mut History,
            context: TurnContext,
            input: AgentInput,
            work: WorkKind,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async move {
                let result = self.backend.run_turn(history, context, input, work).await;
                self.entered.send(()).unwrap();
                self.release.recv_async().await.unwrap();
                result
            })
        }

        fn run_control<'a>(
            &'a mut self,
            history: &'a mut History,
            context: TurnContext,
            control: &'a ControlWork,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            self.backend.run_control(history, context, control)
        }

        fn run_compact<'a>(
            &'a mut self,
            history: &'a mut History,
            context: TurnContext,
            instructions: Option<&'a str>,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            self.backend.run_compact(history, context, instructions)
        }
    }

    #[test]
    fn active_close_keeps_session_until_billed_end_before_close() {
        smol::block_on(async {
            let usage = TokenUsage {
                input: 13,
                output: 7,
                ..Default::default()
            };
            let provider = Arc::new(StreamOnceProvider::new_replies(vec![
                canned_reply_with_usage(REPLY, usage),
            ]));
            let (original, state, mut session, _) = session_with_provider(provider, None, None);
            original.close();
            let (entered_tx, entered_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            let (actor, runner) = AgentActorHandle::spawn(
                session.agent_id,
                Vec::new(),
                None,
                Box::new(SettlementGate {
                    backend: LuaActorBackend::new(Arc::clone(&state)),
                    entered: entered_tx,
                    release: release_rx,
                }),
            );
            runner.detach();
            session.actor = Arc::new(actor.clone());
            let lua = Lua::new();
            let id = session.agent_id;
            let sessions = AgentSessions(Arc::new(Mutex::new(HashMap::from([(id, session)]))));
            lua.set_app_data(sessions.clone());
            let events = actor.subscribe_events();
            let ticket = actor
                .admit_turn(
                    AgentInput {
                        message: REPLY.into(),
                        mode: maki_agent::AgentMode::Build,
                        images: Vec::new(),
                        preamble: Vec::new(),
                        thinking: Default::default(),
                        fast: false,
                        workflow: false,
                        prompt: None,
                        cancel: None,
                        lease_committer: None,
                    },
                    None,
                    String::new(),
                )
                .unwrap();
            futures_lite::future::race(
                async {
                    entered_rx.recv_async().await.unwrap();
                    actor.close();
                    assert!(actor.snapshot().active_turn.is_some());
                    retire_closed(&lua);
                    assert!(
                        sessions
                            .0
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .contains_key(&id)
                    );
                    assert_eq!(actor.snapshot().cumulative_usage, TokenUsage::default());
                    release_tx.send(()).unwrap();
                    let mut ended = false;
                    loop {
                        match events.recv_async().await.unwrap() {
                            maki_agent::ActorEvent::End(result) => {
                                assert_eq!(result.outcome.usage(), usage);
                                assert_eq!(actor.snapshot().cumulative_usage, usage);
                                ended = true;
                            }
                            maki_agent::ActorEvent::Close { .. } => {
                                assert!(ended);
                                break;
                            }
                            _ => {}
                        }
                    }
                },
                async {
                    smol::Timer::after(RETIRE_TIMEOUT).await;
                    panic!("active close did not settle");
                },
            )
            .await;
            ticket.wait().await;
            assert!(actor.snapshot().active_turn.is_none());
            assert_eq!(actor.snapshot().cumulative_usage, usage);
            retire_closed(&lua);
            assert!(
                !sessions
                    .0
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .contains_key(&id)
            );
        });
    }

    #[test]
    fn plugin_revocation_closes_only_owned_children() {
        const OWNER: &str = "agent-owner";
        const OTHER_OWNER: &str = "other-owner";
        let provider = Arc::new(StreamOnceProvider::new_replies(Vec::new()));
        let (actor, _, session, _) = session_with_provider(provider.clone(), None, None);
        let (other_actor, _, other_session, _) = session_with_provider(provider, None, None);
        let lua = Lua::new();
        lua.set_app_data(AgentOwners(HashMap::from([
            (session.agent_id, Arc::from(OWNER)),
            (other_session.agent_id, Arc::from(OTHER_OWNER)),
        ])));
        lua.set_app_data(AgentSessions(Arc::new(Mutex::new(HashMap::from([
            (session.agent_id, session),
            (other_session.agent_id, other_session),
        ])))));
        crate::orchestration::revoke_plugin(&lua, OWNER);
        assert_ne!(actor.snapshot().lifecycle, ActorLifecycle::Open);
        assert_eq!(other_actor.snapshot().lifecycle, ActorLifecycle::Open);
        assert_eq!(
            lua.app_data_ref::<AgentSessions>()
                .unwrap()
                .0
                .lock()
                .unwrap()
                .len(),
            1
        );
        crate::orchestration::revoke_plugin(&lua, OWNER);
        assert_eq!(other_actor.snapshot().lifecycle, ActorLifecycle::Open);
    }

    #[test]
    fn lua_agent_ticket_reads_are_repeatable_and_gc_is_nonowning() {
        smol::block_on(async {
            let usage = TokenUsage {
                input: 13,
                output: 7,
                ..Default::default()
            };
            let provider = Arc::new(StreamOnceProvider::new_replies(vec![
                canned_reply_with_usage(REPLY, usage),
            ]));
            let (_standalone, state, legacy, _events) = session_with_provider(provider, None, None);
            let params = state.params.get().unwrap();
            let config = maki_agent::EffectiveAgentConfig::new(
                maki_agent::RunSettings {
                    model: params.model.clone(),
                    provider: Arc::clone(&params.provider),
                    thinking: state.opts.thinking,
                    fast: state.opts.fast,
                    workflow: false,
                },
                maki_agent::AgentMode::Build,
            )
            .with_mode_def(Some(params.modes.current(&maki_agent::AgentMode::Build)));
            let mut managed_fixture = None;
            let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
            let target = manager
                .create_root_with_config(Some(config), Vec::new(), None, |agent_id| {
                    let (actor, root_state, session, events) =
                        super::super::tests::session_with_provider_for_agent(
                            Arc::clone(&params.provider),
                            agent_id,
                        );
                    assert_eq!(root_state.params.get().unwrap().agent_id, agent_id);
                    managed_fixture = Some((actor, Arc::clone(&root_state), session, events));
                    Ok::<_, String>(Box::new(LuaActorBackend::new(root_state)))
                })
                .unwrap();
            let lua = Lua::new();
            let scope = TaskScope::detached(&lua);
            let mut template = stub_ctx(&maki_agent::AgentMode::Build);
            template.provider = Arc::clone(&params.provider);
            let trusted_target = crate::orchestration::TrustedTarget {
                target: target.clone(),
                template,
            };
            lua.set_app_data(crate::orchestration::OrchestrationServicesSlot(Some(
                Arc::new(TestServices(trusted_target.clone())),
            )));
            let ctx = LuaCtx::trusted_context(&lua, trusted_target, Arc::from("agent-test"));
            lua.globals().set("ctx", ctx).unwrap();
            lua.globals()
                .set(
                    "agent",
                    LuaAgent {
                        manager: AgentRuntime::Managed(manager.clone()),
                        id: Some(target.id()),
                        state: Some(Arc::clone(&managed_fixture.as_ref().unwrap().1)),
                        held: Some(target.clone()),
                        root: Some(manager.root().unwrap()),
                        ancestors: vec![target.id()],
                    },
                )
                .unwrap();
            let result: Table = lua
                .load(
                    r#"
                local ticket, err = agent:send(ctx, "hello")
                assert(ticket, err)
                local result, wait_err = ticket:wait(ctx)
                assert(result, wait_err)
                assert(result.status == "completed", result.error or result.status)
                assert(ticket:id(ctx) ~= agent:id(ctx))
                local again = ticket:result(ctx)
                assert(again.text == result.text)
                assert(again.input_tokens == result.input_tokens)
                local history = agent:transcript(ctx, { last_messages = 100, max_bytes = 65536 })
                assert(history, "transcript unavailable")
                local found_user, found_assistant = false, false
                for _, message in ipairs(history.messages) do
                    for _, block in ipairs(message.content or {}) do
                        if block.type == "text" then
                            found_user = found_user or (message.role == "user" and block.text == "hello")
                            found_assistant = found_assistant or (message.role == "assistant" and block.text == result.text)
                        end
                    end
                end
                assert(found_user, "transcript missing admitted user message")
                local diagnostic = "result.text=" .. tostring(result.text)
                for index, message in ipairs(history.messages) do
                    diagnostic = diagnostic .. " message[" .. index .. "].role=" .. tostring(message.role)
                    for _, block in ipairs(message.content or {}) do
                        diagnostic = diagnostic .. " block.type=" .. tostring(block.type) .. " block.text=" .. tostring(block.text)
                    end
                end
                assert(found_assistant, "transcript missing retained assistant message: " .. diagnostic)
                ticket = nil
                agent = nil
                collectgarbage("collect")
                return result
            "#,
                )
                .eval_async()
                .await
                .unwrap();
            assert_eq!(result.get::<String>("text").unwrap(), REPLY);
            assert_eq!(
                result.get::<u32>("input_tokens").unwrap(),
                usage.total_input()
            );
            assert_eq!(
                target.actor().unwrap().snapshot().lifecycle,
                ActorLifecycle::Open
            );
            drop(scope);
            manager.shutdown(Duration::from_secs(1)).await;
            drop(legacy);
        });
    }
}
