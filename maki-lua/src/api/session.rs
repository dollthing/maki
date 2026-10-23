//! `maki.session`: host session primitives. Session management round-trips to
//! the UI event loop, which owns live runtimes and storage. `notify` posts
//! directly to the agent mailbox so synchronous callbacks can use it.

use maki_agent::SessionMailbox;
use maki_agent::session_coordinator::SessionCoordinatorHandle;
use maki_agent::session_options::{
    SessionOptionCategory, SessionOptionOwner, SessionOptionsSnapshot,
};
use maki_lua_macro::{lua_fn, lua_table};
use maki_providers::parse_thinking;
use maki_storage::id::MakiId;
use maki_storage::sessions::{Prefs, write_prefs};
use mlua::{Lua, Result as LuaResult, Table, Value};

use crate::agent_errors::{
    DELEGATED_FOCUS, DELEGATED_FOREIGN_SESSION, DELEGATED_HOST_MUTATION,
    DELEGATED_SESSION_LIFECYCLE, NO_FOCUSED_AGENT, NO_ORCHESTRATION_SERVICES, SESSION_REQUIRED_ERR,
};
use crate::api::env::host_state_dir;
use crate::api::session_option::{ensure_validation_not_in_progress, validate_option_value};
use crate::api::util::command::{SessionRequest, UiAction, ui_json_roundtrip};
use crate::api::util::convert::json_to_lua;
use crate::api::util::pair::{Pair, err_pair, try_pair};
use crate::orchestration::resolve_target;

/// Answers `maki.session.read` for a driver that has no UI to ask. Takes the
/// session id from Lua and returns a serialized
/// [`crate::session_snapshot::SessionSnapshot`].
pub type SessionSnapshotFn =
    Box<dyn Fn(MakiId) -> Result<serde_json::Value, String> + Send + Sync + 'static>;

pub struct SessionSnapshotSlot(pub SessionSnapshotFn);

const BLANK_NOTIFY_ERR: &str = "text must not be blank";
const NO_FOCUSED_SESSION: &str = "the focused agent has no session";

async fn roundtrip(
    lua: Lua,
    tx: Option<flume::Sender<UiAction>>,
    req: SessionRequest,
) -> LuaResult<Pair<Value>> {
    try_pair!(crate::runtime::validate_plugin_effect(&lua));
    ui_json_roundtrip(&lua, tx.as_ref(), |reply_tx| UiAction::Session {
        req,
        reply_tx,
    })
    .await
}

/// The coordinator of the session {opts} names in `session`.
pub(crate) fn resolve_coordinator(
    opts: Option<&Table>,
) -> Result<SessionCoordinatorHandle, String> {
    let id = required_session(opts).map_err(|error| error.to_string())??;
    SessionCoordinatorHandle::resolve(id).map_err(|e| e.to_string())
}

/// The `session` id {opts} must name; the inner error is a missing or
/// malformed id.
fn required_session(opts: Option<&Table>) -> LuaResult<Result<MakiId, String>> {
    let Some(raw) = opts
        .map(|opts| opts.get::<Option<String>>("session"))
        .transpose()?
        .flatten()
    else {
        return Ok(Err(SESSION_REQUIRED_ERR.to_owned()));
    };
    Ok(raw.parse::<MakiId>().map_err(|error| error.to_string()))
}

fn snapshot_table(lua: &Lua, snapshot: &SessionOptionsSnapshot) -> LuaResult<Table> {
    let result = lua.create_table()?;
    result.set("version", snapshot.version)?;
    let options = lua.create_table_with_capacity(snapshot.options.len(), 0)?;
    for (index, state) in snapshot.options.iter().enumerate() {
        let option = lua.create_table()?;
        let definition = &state.definition;
        option.set("id", definition.id.as_ref())?;
        option.set("name", definition.name.as_ref())?;
        option.set("description", definition.description.as_ref())?;
        option.set(
            "category",
            match definition.category {
                SessionOptionCategory::Model => "model",
                SessionOptionCategory::Mode => "mode",
            },
        )?;
        option.set("current_value", state.current_value.as_ref())?;
        option.set("persistent", definition.persistent)?;
        option.set(
            "owner",
            match &definition.owner {
                SessionOptionOwner::Builtin => "builtin",
                SessionOptionOwner::Plugin { plugin, .. } => plugin.as_ref(),
            },
        )?;
        let values = lua.create_table_with_capacity(definition.values.len(), 0)?;
        for (value_index, value) in definition.values.iter().enumerate() {
            let item = lua.create_table()?;
            item.set("value", value.value.as_ref())?;
            item.set("name", value.name.as_ref())?;
            values.set(value_index + 1, item)?;
        }
        option.set("values", values)?;
        options.set(index + 1, option)?;
    }
    result.set("options", options)?;
    Ok(result)
}

/// Lists sessions stored for the current project. Answered from a
/// background scan, so a slow disk never blocks the UI. `open_elsewhere` is
/// true while another makima instance has the session open. `message_count`
/// counts main transcript messages only and excludes subagent histories.
///
/// @return (table|nil, string|nil) Array of `{id, title, updated_at, cwd, message_count, open_elsewhere}`, or nil and an error.
/// @example
/// local stored, err = maki.session.list()
#[lua_fn]
async fn list(lua: Lua, #[ctx] tx: Option<flume::Sender<UiAction>>) -> LuaResult<Pair<Value>> {
    roundtrip(lua, tx, SessionRequest::List).await
}

/// Lists stored sessions across every project directory, most recently
/// updated first. Answered from a background scan, so a slow disk never
/// blocks the UI. `open_elsewhere` is true while another makima instance has
/// the session open. `message_count` counts main transcript messages only
/// and excludes subagent histories.
///
/// @return (table|nil, string|nil) Array of `{id, title, updated_at, cwd, message_count, open_elsewhere}`, or nil and an error.
/// @example
/// local stored, err = maki.session.list_all()
#[lua_fn]
async fn list_all(lua: Lua, #[ctx] tx: Option<flume::Sender<UiAction>>) -> LuaResult<Pair<Value>> {
    roundtrip(lua, tx, SessionRequest::ListAll).await
}

/// Lists the sessions currently running in this UI. Status is "working",
/// "needs_input", or "idle". A mailbox follow-up stays "working" without an
/// intermediate "idle" status.
///
/// `message_count` counts main transcript messages only and excludes subagent histories.
///
/// @return (table|nil, string|nil) Array of `{id, title, status, updated_at, message_count, focused}`, or nil and an error.
/// @example
/// local live, err = maki.session.live()
#[lua_fn]
async fn live(lua: Lua, #[ctx] tx: Option<flume::Sender<UiAction>>) -> LuaResult<Pair<Value>> {
    roundtrip(lua, tx, SessionRequest::Live).await
}

/// Returns the id of the focused agent's session; a headless frontend's
/// focus is its one session.
///
/// @return (string|nil, string|nil) Session id, or nil and an error.
/// @example
/// local id = maki.session.current()
#[lua_fn]
fn current(lua: &Lua) -> LuaResult<Pair<String>> {
    if crate::runtime::in_delegated_tool(lua) {
        return Ok(err_pair(DELEGATED_FOCUS));
    }
    let target = try_pair!(try_pair!(resolve_target(lua, None)).ok_or(NO_FOCUSED_AGENT));
    let session = try_pair!(target.template.session_id.ok_or(NO_FOCUSED_SESSION));
    Ok((Some(session.to_string()), None))
}

/// Returns a session's token usage without making a network request.
/// Costs are the values recorded when each turn was billed and remain nil when
/// no turn for that total or model was priced. Models are sorted by total tokens
/// descending, then model spec ascending.
///
/// @param opts table Required `session` (string) id.
/// @return (table|nil, string|nil) `{total={input, output, cache_creation,
///   cache_read, cost}, models}`, where each model has `{model, input, output,
///   cache_creation, cache_read, cost}`, or nil and an error.
/// @example
/// local usage, err = maki.session.usage({ session = ctx:session_id() })
#[lua_fn]
async fn usage(
    lua: Lua,
    #[ctx] tx: Option<flume::Sender<UiAction>>,
    opts: Table,
) -> LuaResult<Pair<Value>> {
    let id = try_pair!(required_session(Some(&opts))?);
    roundtrip(lua, tx, SessionRequest::Usage { id }).await
}

/// Switches the UI to the session with {id}. The session must belong to
/// the current directory and must not be open in another terminal.
///
/// @param id string Session id, as returned by `list()` or `live()`.
/// @return (boolean|nil, string|nil) true on success, or nil and an error.
/// @example
/// local _, err = maki.session.focus(id)
#[lua_fn]
async fn focus(
    lua: Lua,
    #[ctx] tx: Option<flume::Sender<UiAction>>,
    id: String,
) -> LuaResult<Pair<Value>> {
    if crate::runtime::in_delegated_tool(&lua) {
        return Ok(err_pair(DELEGATED_SESSION_LIFECYCLE));
    }
    roundtrip(lua, tx, SessionRequest::Focus { id }).await
}

/// Deletes a session and its stored history, cancelling it first if it
/// is running. The focused session cannot be deleted.
///
/// @param id string Session id to delete.
/// @return (boolean|nil, string|nil) true on success, or nil and an error.
/// @example
/// local _, err = maki.session.delete(id)
#[lua_fn]
async fn delete(
    lua: Lua,
    #[ctx] tx: Option<flume::Sender<UiAction>>,
    id: String,
) -> LuaResult<Pair<Value>> {
    if crate::runtime::in_delegated_tool(&lua) {
        return Ok(err_pair(DELEGATED_SESSION_LIFECYCLE));
    }
    roundtrip(lua, tx, SessionRequest::Delete { id }).await
}

/// One-call snapshot of a session: queue, usage, context, cost, mode, and
/// status of the session you name in `session`.
///
/// The returned table:
/// ```lua
/// {
///   id, cwd, model, fast, thinking,
///   mode,              -- committed mode id: "build", "plan" or a plugin mode
///   status = "idle" | "working" | "needs_input",
///   focused, updated_at,
///   usage = { input_tokens, output_tokens, cache_creation_input_tokens, cache_read_input_tokens },
///   context_size, context_window,
///   cost,
///   queue = { count }, -- nil under headless drivers
///   title,             -- nil under headless drivers
/// }
/// ```
///
/// `usage` and `cost` include subagent spend. `context_size` is the main
/// session's own, since a subagent runs its own window. There is no
/// `list_cost` here because `cost` is re-settled from stored usage when a
/// session resumes and list price is not stored, so per-turn list price
/// lives on the `TurnEnd` autocmd instead.
///
/// @param opts table Required `session` (string) id.
/// @return (table|nil, string|nil) Snapshot table, or nil and an error.
/// @example
/// local s = maki.session.read({ session = ctx:session_id() })
/// if s.context_size > s.context_window * 0.8 then
///   maki.ui.notify("context is nearly full")
/// end
#[lua_fn]
async fn read(
    lua: Lua,
    #[ctx] tx: Option<flume::Sender<UiAction>>,
    opts: Table,
) -> LuaResult<Pair<Value>> {
    let id = try_pair!(required_session(Some(&opts))?);
    if let Some(slot) = lua.app_data_ref::<SessionSnapshotSlot>() {
        return match (slot.0)(id) {
            Ok(value) => Ok((Some(json_to_lua(&lua, &value)?), None)),
            Err(msg) => Ok(err_pair(msg)),
        };
    }
    ui_json_roundtrip(&lua, tx.as_ref(), |reply_tx| UiAction::Session {
        req: SessionRequest::Read { id },
        reply_tx,
    })
    .await
}

/// Starts a new session in the current project.
///
/// @param opts table? Optional fields: prompt (string) first user message
///   to submit right away; focus (boolean) switch the UI to the new session.
/// @return (string|nil, string|nil) New session id, or nil and an error.
/// @example
/// local id, err = maki.session.new({ prompt = "fix the tests", focus = true })
#[lua_fn]
async fn new(
    lua: Lua,
    #[ctx] tx: Option<flume::Sender<UiAction>>,
    opts: Option<Table>,
) -> LuaResult<Pair<Value>> {
    if crate::runtime::in_delegated_tool(&lua) {
        return Ok(err_pair(DELEGATED_SESSION_LIFECYCLE));
    }
    let (prompt, focus) = match opts {
        Some(opts) => (opts.get("prompt")?, opts.get("focus").unwrap_or(false)),
        None => (None, false),
    };
    roundtrip(lua, tx, SessionRequest::New { prompt, focus }).await
}

/// Sends {text} as a regular user prompt to a live session. The text is
/// never interpreted: slash commands, `exit`, and `!` shell prefixes are
/// all sent to the model verbatim. If the session is currently streaming,
/// the prompt is queued and picked up when the agent reaches it.
///
/// @param text string The prompt to send. Must not be blank.
/// @param opts table Required `session` (string) id of a live session.
/// @return (string|nil, string|nil) "started" or "queued", or nil and an error.
/// @example
/// local state, err = maki.session.prompt("run the tests", { session = id })
#[lua_fn]
async fn prompt(
    lua: Lua,
    #[ctx] tx: Option<flume::Sender<UiAction>>,
    text: String,
    opts: Table,
) -> LuaResult<Pair<Value>> {
    let id = try_pair!(required_session(Some(&opts))?);
    if let Some(denial) = delegated_tool_denial(&lua, id) {
        return Ok(err_pair(denial));
    }
    roundtrip(lua, tx, SessionRequest::Prompt { id, text }).await
}

/// Reports {text} to a live session without creating a user turn. The
/// observation waits for the session's next agent run.
///
/// @param text string What to report. Must not be blank.
/// @param opts table Options:
///   `session` (string) id of a live session.
///   `wake` (boolean) start a TUI turn when it next becomes idle (default false).
/// @return (boolean|nil, string|nil) true, or nil and an error.
/// @example
/// maki.session.notify("[monitor] deploy failed", { session = id, wake = true })
#[lua_fn]
fn notify(_lua: &Lua, text: String, opts: Table) -> LuaResult<Pair<bool>> {
    if text.trim().is_empty() {
        return Ok(err_pair(BLANK_NOTIFY_ERR));
    }
    let Some(raw_id) = opts.get::<Option<String>>("session")? else {
        return Ok(err_pair(SESSION_REQUIRED_ERR));
    };
    let session_id: MakiId = match raw_id.parse() {
        Ok(id) => id,
        Err(error) => return Ok(err_pair(error)),
    };
    let wake = opts.get("wake").unwrap_or(false);
    if let Err(error) = SessionMailbox::notify(session_id, text, wake) {
        return Ok(err_pair(error));
    }
    Ok((Some(true), None))
}

/// Returns the complete ordered option snapshot for a live session.
///
/// @param opts table Required `session` (string) id.
/// @return (table|nil, string|nil) `{version, options}`, or nil and an error.
/// @example
/// local snapshot, err = maki.session.options({ session = id })
#[lua_fn]
fn options(lua: &Lua, opts: Table) -> LuaResult<Pair<Table>> {
    try_pair!(crate::runtime::validate_plugin_effect(lua));
    let coordinator = try_pair!(resolve_coordinator(Some(&opts)));
    let snapshot = snapshot_table(lua, &coordinator.read().options())?;
    Ok((Some(snapshot), None))
}

/// Sets one option explicitly for a live session. Model, thinking, fast, and
/// workflow changes follow the agent configuration rules. Other options keep
/// their owner's persistence behavior.
/// See [Configuration changes](/docs/modes/#configuration-changes).
///
/// @param id string Stable option id.
/// @param value string Selectable value id.
/// @param opts table Required `session` (string) id.
/// @return (boolean|nil, string|nil) true, or nil and an error.
/// @example
/// local ok, err = maki.session.set_option("fast", "enabled", { session = id })
#[lua_fn]
async fn set_option(lua: Lua, id: String, value: String, opts: Table) -> LuaResult<Pair<bool>> {
    try_pair!(crate::runtime::validate_plugin_effect(&lua));
    let session = try_pair!(required_session(Some(&opts))?);
    if let Some(denial) = delegated_tool_denial(&lua, session) {
        return Ok(err_pair(denial));
    }
    if let Err(error) = ensure_validation_not_in_progress(&lua) {
        return Ok(err_pair(error));
    }
    let coordinator = try_pair!(SessionCoordinatorHandle::resolve(session));
    let snapshot = coordinator.read().options();
    if let Some(option) = snapshot
        .options
        .iter()
        .find(|option| option.definition.id.as_ref() == id)
        && let Err(error) = validate_option_value(&lua, option, &value)
    {
        return Ok(err_pair(error));
    }
    try_pair!(crate::runtime::validate_plugin_effect(&lua));
    match coordinator
        .set_option_if_version(id, value, Some(snapshot.version))
        .await
    {
        Ok(_) => Ok((Some(true), None)),
        Err(error) => Ok(err_pair(error)),
    }
}

/// Switches a live session's root agent to mode {name}, through the same
/// config queue as `Agent:set_mode`. Autocmd and timer callbacks have no ctx,
/// so they set a mode this way, naming the session their event carries.
/// Tool handlers use `Agent:set_mode` on their own agent instead.
///
/// @param name string Registered mode name.
/// @param opts table Required `session` (string) id of a live session.
/// @return (string|nil, string|nil) The committed mode name, or nil and an error.
/// @example
/// local mode, err = maki.session.set_mode("plan", { session = ev.data.session_id })
#[lua_fn]
async fn set_mode(lua: Lua, name: String, opts: Table) -> LuaResult<Pair<String>> {
    if crate::runtime::in_delegated_tool(&lua) {
        return Ok(err_pair(DELEGATED_HOST_MUTATION));
    }
    try_pair!(crate::runtime::validate_plugin_effect(&lua));
    let session = try_pair!(required_session(Some(&opts))?);
    let target = try_pair!(try_pair!(SessionCoordinatorHandle::resolve(session)).agent());
    let services = try_pair!(crate::orchestration::services(&lua).ok_or(NO_ORCHESTRATION_SERVICES));
    let template = try_pair!(services.template(&target));
    let state_dir = try_pair!(host_state_dir(&lua));
    let commit = try_pair!(
        crate::orchestration::set_mode(&*services, &target, &template.modes, &name, &state_dir)
            .await
    );
    Ok((Some(commit.config.mode.id().key().to_string()), None))
}

/// Renames a session, live or stored.
///
/// @param opts table Required fields: id (string) session to rename;
///   title (string) the new title.
/// @return (boolean|nil, string|nil) true on success, or nil and an error.
/// @example
/// local _, err = maki.session.set_title({ id = id, title = "refactor" })
#[lua_fn]
async fn set_title(
    lua: Lua,
    #[ctx] tx: Option<flume::Sender<UiAction>>,
    opts: Table,
) -> LuaResult<Pair<Value>> {
    let id: String = opts.get("id")?;
    let parsed = try_pair!(id.parse::<MakiId>());
    if let Some(denial) = delegated_tool_denial(&lua, parsed) {
        return Ok(err_pair(denial));
    }
    let req = SessionRequest::SetTitle {
        id,
        title: opts.get("title")?,
    };
    roundtrip(lua, tx, req).await
}

/// Persists the default thinking setting for new sessions without changing
/// any live session. Every frontend starts a blank session on it, ahead of
/// `always_thinking`. `mode` is a thinking setting as `Agent:set_model`
/// accepts it.
///
/// @param mode string The thinking setting.
/// @return (table|nil, string|nil) `{mode}` with the normalized setting, or nil and an error.
/// @example
/// maki.session.set_default_thinking("medium")
#[lua_fn]
fn set_default_thinking(lua: &Lua, mode: String) -> LuaResult<Pair<Table>> {
    if crate::runtime::in_delegated_tool(lua) {
        return Ok(err_pair(DELEGATED_HOST_MUTATION));
    }
    try_pair!(crate::runtime::validate_plugin_effect(lua));
    let thinking = try_pair!(parse_thinking(mode.trim()));
    let state_dir = try_pair!(host_state_dir(lua));
    try_pair!(write_prefs(
        &state_dir,
        &Prefs {
            default_thinking: Some(thinking.into()),
        },
    ));
    let reply = lua.create_table()?;
    reply.set("mode", thinking.to_string())?;
    Ok((Some(reply), None))
}

/// Delegated tools may change only the session their turn belongs to.
fn delegated_tool_denial(lua: &Lua, session: MakiId) -> Option<&'static str> {
    let tool = crate::runtime::delegated_tool(lua)?;
    (tool.session != Some(session)).then_some(DELEGATED_FOREIGN_SESSION)
}

lua_table! {
    /// Host session primitives. The interactive UI can run several sessions
    /// at once; these functions let plugins list, create, focus, rename, and
    /// delete them. Session management returns `nil, "no interactive UI
    /// attached"` without a UI. `notify` instead targets a live agent mailbox
    /// directly, so it also works under ACP and SDK frontends. A tool handler
    /// cannot create, focus or delete sessions, and the functions that change
    /// a session (`prompt`, `set_option`, `set_title`) accept
    /// only the session its own turn belongs to; `set_mode` refuses them. Every function that reads or
    /// changes one session takes it explicitly; `ctx:session_id()` names the
    /// invocation's session, and `current` names the focused one. Tool
    /// handlers cannot read UI focus, so `current` refuses them.
    "maki.session" => pub(crate) fn create_session_table(tx: Option<flume::Sender<UiAction>>),
    DOCS [list(tx), list_all(tx), live(tx), current, usage(tx), read(tx), focus(tx), delete(tx), new(tx), prompt(tx), notify(), options, set_option, set_mode, set_title(tx), set_default_thinking]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::util::command::NO_UI_ERR;
    use std::sync::Arc;

    use maki_agent::session_coordinator::{
        SessionCheckpoint, SessionCoordinatorHandle, SessionCoordinatorParams,
        builtin_option_definitions,
    };
    use maki_storage::checkpoint::{CheckpointWriter, accept_all};
    use mlua::Value;
    use serde_json::json;
    use test_case::test_case;

    fn lua_with_session(tx: Option<flume::Sender<UiAction>>) -> Lua {
        let lua = Lua::new();
        let t = create_session_table(&lua, tx).unwrap();
        lua.globals().set("session", t).unwrap();
        lua
    }

    fn live_mailbox(id: MakiId) -> (SessionMailbox, SessionCoordinatorHandle) {
        let mailbox = SessionMailbox::new(id);
        let checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>> = accept_all();
        let coordinator = SessionCoordinatorHandle::register(SessionCoordinatorParams {
            session_id: id,
            catalog: Default::default(),
            definitions: builtin_option_definitions(
                "test/model",
                [Arc::from("test/model")],
                false,
                false,
                false,
                maki_agent::ThinkingConfig::Off,
            ),
            persisted_options: Default::default(),
            model_policy: Arc::default(),
            checkpoint,
            mailbox: mailbox.clone(),
        })
        .unwrap();
        (mailbox, coordinator)
    }

    #[test]
    fn live_without_ui_returns_error_pair() {
        let lua = lua_with_session(None);
        let (val, err): (Value, Option<String>) =
            smol::block_on(lua.load("return session.live()").eval_async()).unwrap();
        assert!(val.is_nil());
        assert_eq!(err.as_deref(), Some(NO_UI_ERR));
    }

    #[test]
    fn usage_roundtrips_through_ui_channel() {
        let (tx, rx) = flume::unbounded::<UiAction>();
        let lua = lua_with_session(Some(tx));
        let checker = std::thread::spawn(move || {
            let Ok(UiAction::Session {
                req: SessionRequest::Usage { .. },
                reply_tx,
            }) = rx.recv()
            else {
                panic!("expected usage request");
            };
            reply_tx
                .send(Ok(json!({
                    "total": {
                        "input": 1,
                        "output": 2,
                        "cache_creation": 3,
                        "cache_read": 4,
                        "cost": null,
                    },
                    "models": [],
                })))
                .unwrap();
        });
        lua.globals()
            .set("session_id", MakiId::generate().to_string())
            .unwrap();
        let (val, err): (Table, Option<String>) = smol::block_on(
            lua.load("return session.usage({ session = session_id })")
                .eval_async(),
        )
        .unwrap();
        checker.join().unwrap();
        assert_eq!(err, None);
        let total: Table = val.get("total").unwrap();
        assert_eq!(total.get::<u32>("input").unwrap(), 1);
        assert!(total.get::<Value>("cost").unwrap().is_nil());
        assert_eq!(val.get::<Table>("models").unwrap().raw_len(), 0);
    }

    #[test]
    fn focus_roundtrips_through_ui_channel() {
        let (tx, rx) = flume::unbounded::<UiAction>();
        let lua = lua_with_session(Some(tx));
        std::thread::spawn(move || {
            let Ok(UiAction::Session {
                req: SessionRequest::Focus { id },
                reply_tx,
            }) = rx.recv()
            else {
                panic!("expected focus request");
            };
            reply_tx.send(Ok(json!({ "focused": id }))).unwrap();
        });
        let (val, err): (Table, Option<String>) =
            smol::block_on(lua.load("return session.focus('abc')").eval_async()).unwrap();
        assert_eq!(err, None);
        assert_eq!(val.get::<String>("focused").unwrap(), "abc");
    }

    #[test]
    fn prompt_forwards_text_and_session_id() {
        let (tx, rx) = flume::unbounded::<UiAction>();
        let lua = lua_with_session(Some(tx));
        let expected_id = MakiId::generate();
        lua.globals()
            .set("session_id", expected_id.to_string())
            .unwrap();
        let checker = std::thread::spawn(move || {
            let Ok(UiAction::Session {
                req: SessionRequest::Prompt { id, text },
                reply_tx,
            }) = rx.recv()
            else {
                panic!("expected prompt request");
            };
            assert_eq!(id, expected_id);
            assert_eq!(text, "hi");
            reply_tx.send(Ok(json!("queued"))).unwrap();
        });
        let (val, err): (String, Option<String>) = smol::block_on(
            lua.load("return session.prompt('hi', { session = session_id })")
                .eval_async(),
        )
        .unwrap();
        checker.join().unwrap();
        assert_eq!(err, None);
        assert_eq!(val, "queued");
    }

    #[test_case("return session.prompt('hi', {})" ; "prompt")]
    #[test_case("return session.read({})" ; "read")]
    #[test_case("return session.usage({})" ; "usage")]
    #[test_case("return session.options({})" ; "options")]
    #[test_case("return session.set_option('yolo', 'enabled', {})" ; "set_option")]
    #[test_case("return session.set_mode('plan', {})" ; "set_mode")]
    fn session_target_is_required(code: &str) {
        let (tx, rx) = flume::unbounded::<UiAction>();
        let lua = lua_with_session(Some(tx));
        let (value, error): (Value, Option<String>) =
            smol::block_on(lua.load(code).eval_async()).unwrap();
        assert!(value.is_nil());
        assert_eq!(error.as_deref(), Some(SESSION_REQUIRED_ERR));
        assert!(rx.is_empty());
    }

    #[test]
    fn options_explicit_session_returns_complete_ordered_snapshot() {
        let id = MakiId::generate();
        let (_, coordinator) = live_mailbox(id);
        let lua = lua_with_session(None);
        lua.globals().set("session_id", id.to_string()).unwrap();

        let (snapshot, error): (Table, Option<String>) = smol::block_on(
            lua.load("return session.options({ session = session_id })")
                .eval_async(),
        )
        .unwrap();

        assert_eq!(error, None);
        assert_eq!(snapshot.get::<u64>("version").unwrap(), 1);
        let options = snapshot.get::<Table>("options").unwrap();
        let ids = options
            .sequence_values::<Table>()
            .map(|option| option.unwrap().get::<String>("id").unwrap())
            .collect::<Vec<_>>();
        assert_eq!(ids, ["model", "yolo", "fast", "workflow", "thinking"]);
        let model = options.get::<Table>(1).unwrap();
        assert_eq!(model.get::<String>("category").unwrap(), "model");
        assert_eq!(model.get::<String>("current_value").unwrap(), "test/model");
        assert_eq!(model.get::<Table>("values").unwrap().raw_len(), 1);
        smol::block_on(coordinator.close()).unwrap();
    }

    #[test]
    fn set_option_commits_through_the_named_coordinator() {
        let id = MakiId::generate();
        let (_, coordinator) = live_mailbox(id);
        let (tx, rx) = flume::unbounded::<UiAction>();
        let lua = lua_with_session(Some(tx));
        lua.globals().set("session_id", id.to_string()).unwrap();

        let (ok, error): (bool, Option<String>) = smol::block_on(
            lua.load("return session.set_option('yolo', 'enabled', { session = session_id })")
                .eval_async(),
        )
        .unwrap();
        assert!(ok);
        assert_eq!(error, None);
        assert!(rx.is_empty());
        assert!(
            coordinator
                .read()
                .options()
                .options
                .iter()
                .any(|option| option.definition.id.as_ref() == "yolo"
                    && option.current_value.as_ref() == "enabled")
        );
        smol::block_on(coordinator.close()).unwrap();
    }

    #[test]
    fn options_rejects_closed_explicit_session() {
        let id = MakiId::generate();
        let (_, coordinator) = live_mailbox(id);
        smol::block_on(coordinator.close()).unwrap();
        let lua = lua_with_session(None);
        lua.globals().set("session_id", id.to_string()).unwrap();

        let (value, error): (Value, Option<String>) = smol::block_on(
            lua.load("return session.options({ session = session_id })")
                .eval_async(),
        )
        .unwrap();

        assert!(value.is_nil());
        assert_eq!(error, Some(format!("session not live: {id}")));
    }

    #[test]
    fn notify_is_synchronous_and_queues_an_observation() {
        let id = MakiId::generate();
        let (mailbox, coordinator) = live_mailbox(id);
        let (tx, rx) = flume::unbounded::<UiAction>();
        let lua = lua_with_session(Some(tx));
        lua.globals().set("session_id", id.to_string()).unwrap();

        let (value, error): (bool, Option<String>) = lua
            .load("return session.notify('built', { session = session_id })")
            .eval()
            .unwrap();

        assert!(value);
        assert_eq!(error, None);
        let messages = mailbox.drain();
        assert_eq!(messages.len(), 1);
        assert!(messages[0].is_observation());
        assert_eq!(messages[0].user_text(), Some("built"));
        assert!(rx.try_recv().is_err());
        smol::block_on(coordinator.close()).unwrap();
    }

    #[test]
    fn waking_notify_sets_the_mailbox_wake_flag() {
        let id = MakiId::generate();
        let (mailbox, coordinator) = live_mailbox(id);
        let lua = lua_with_session(None);
        lua.globals().set("session_id", id.to_string()).unwrap();

        let (value, error): (bool, Option<String>) = lua
            .load("return session.notify('failed', { session = session_id, wake = true })")
            .eval()
            .unwrap();

        assert!(value);
        assert_eq!(error, None);
        assert_eq!(mailbox.claim_wake().len(), 1);
        smol::block_on(coordinator.close()).unwrap();
    }

    #[test]
    fn notify_rejects_missing_and_non_live_sessions() {
        let lua = lua_with_session(None);
        let (_, missing): (Value, Option<String>) = lua
            .load("return session.notify('built', {})")
            .eval()
            .unwrap();
        assert_eq!(missing.as_deref(), Some(SESSION_REQUIRED_ERR));

        let id = MakiId::generate();
        lua.globals().set("session_id", id.to_string()).unwrap();
        let (_, not_live): (Value, Option<String>) = lua
            .load("return session.notify('built', { session = session_id })")
            .eval()
            .unwrap();
        assert_eq!(not_live, Some(format!("session not live: {id}")));
    }

    #[test]
    fn notify_rejects_blank_text_and_invalid_session_ids() {
        let lua = lua_with_session(None);
        let (_, blank): (Value, Option<String>) = lua
            .load("return session.notify(' ', { session = 'invalid' })")
            .eval()
            .unwrap();
        assert_eq!(blank.as_deref(), Some(BLANK_NOTIFY_ERR));

        let (_, invalid): (Value, Option<String>) = lua
            .load("return session.notify('built', { session = 'invalid' })")
            .eval()
            .unwrap();
        assert!(invalid.is_some_and(|error| error.contains("invalid base58")));
    }

    #[test]
    fn set_title_with_wrong_type_throws() {
        let lua = lua_with_session(None);
        let result: LuaResult<Value> =
            smol::block_on(lua.load("return session.set_title('oops')").eval_async());
        assert!(result.unwrap_err().to_string().contains("table"));
    }
}
