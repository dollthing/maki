+++
title = "Plugins"
weight = 23
[extra]
group = "Guides"
+++

# Writing makima plugins

Makima plugins are plain Lua files (Luau) that run inside makima. A plugin can
register tools the LLM calls, slash commands, keymaps, prompt hints, and
custom UI. Everything lives under the global `maki` table. The full API
reference is at the end of this document.

## Where plugin code goes

Plugins live in the makima config dir. There are two of them, same layout:

- `~/.config/makima/` - global, every project (if `~/.makima/` exists, makima reads
  that one instead)
- `<project>/.makima/` - this project only

```
init.lua        the only file makima runs; require()s plugins, calls maki.setup()
lua/<name>.lua  plugin modules, loaded by require("<name>")
plugin.toml     permission grants for every Lua file in the dir
```

Nothing under `lua/` loads on its own. A module name is its path under `lua/`
without the extension: `lua/browser.lua` is `require("browser")`,
`lua/acme/tools.lua` is `require("acme.tools")`. `require` is sandboxed to
that directory, you cannot reach files outside it.

## Creating a plugin

1. Write the code in `~/.config/makima/lua/<name>.lua`. The `maki` global is
   already there, nothing to import. For a project-only plugin use
   `<project>/.makima/` here and in every step below.

```lua
maki.api.register_tool({
  name = "hello",
  description = "Say hello to a name.",
  parameters = { type = "object", properties = { name = { type = "string" } }, required = { "name" } },
  handler = function(args)
    return { llm_output = "hello " .. args.name }
  end,
})
```

2. Load it from `~/.config/makima/init.lua`, creating that file if missing:

```lua
require("hello")
```

3. Grant the permissions it needs in `~/.config/makima/plugin.toml`, creating
   that file if missing. Without the file every gated call is denied.

```toml
[permissions]
fs_read = true
run = true
```

4. Run `/reload`, then read the log as described below, to see that it loaded
   and what it printed.

Leave `maki.api.register_options` to bundled plugins: maki rejects a
`plugins.<name>` table for a plugin it does not ship, and startup fails. Keep
settings in a local table, or export a `setup(opts)` function `init.lua` calls.

## Permissions and plugin.toml

Sensitive APIs are gated per plugin file, and a plugin without a
`plugin.toml` next to it gets nothing. The gates and the file format are
in [the reference](/docs/lua-api/#plugin-permissions).

## Development loop

`/reload` rebuilds plugins and config in place, no restart needed. Until it
runs, an edited plugin is still the old one.

To debug, add `maki.log.info|warn|error(...)` calls. They write to `maki.log`
in the dir `maki.env.logs_dir()` returns (Linux: `~/.local/logs/makima/`). The
log keeps `info` and above. Set `MAKI_LOG=debug` to also keep `maki.log.debug`,
or `MAKI_LOG=maki_lua=trace` to narrow it to one target. When a backtrace comes
out useless, start makima with `--no-jit`: plugins then run on the interpreter,
with full debug info.

## Conventions

- Fallible runtime calls return a `(value, err)` pair; check `err` before using `value`.
- Tool handlers report failures with `{ llm_output = "error: ...", is_error = true }`, not by raising.
- The model picks tools by reading `description`, so state precisely what the tool does and when to use it.
- Reusable helpers ship with makima; see "Shared helper modules" in the API reference.

## Agent context and lifetime

Tool handlers receive `handler(input, ctx)`. Slash-command handlers receive `handler(opts, ctx)`. Keymap callbacks receive `callback(nil, ctx)`. A command or keymap context can be nil when the host has no agent target. Existing callbacks that omit the second argument remain valid.

`maki.agent.current(ctx)` and `ctx:agent()` resolve the invocation's agent. A tool context refers to the executing agent, not UI focus. A trusted command context pins its target. A keymap context is pinned to the agent selected when the key was pressed, so later focus changes do not retarget it. If the pinned agent closes, the context keeps its plugin authority: `maki.agent.root`, `list` and `get` still work, and only calls that address the closed agent, such as `maki.agent.current` or `spawn` under it, fail. `maki.ui.focused_agent()` resolves the agent that has UI focus at call time as a visibility-only reference; pass it to `maki.agent.get(ctx, ref)` for control. `ctx:defer_close(agent)` registers authorized host-held cleanup at callback exit, including cancellation. Context authority expires when its invocation ends or is cancelled, or when its plugin generation is revoked. Storing a context for a later handler does not extend that authority.

In a tool handler, `ctx:model()` reads the selection admitted for the executing turn. `agent:model(ctx)` reads the agent's committed selection. These can differ after a configuration update while the turn is running. Both selections contain `spec`, `thinking`, and `fast`; descriptive model metadata is separate. `ctx:mode()` similarly reads the admitted mode, including on non-TUI roots whose committed `Agent:mode` operation is unsupported.

`Agent` handles do not close their agents when garbage-collected. In managed graphs, children spawned by delegated tools survive normal completion of the creating turn. UI cancellation stops only the viewed agent's captured current work and clears its pending ordinary messages accepted before the cancellation cut. Descendants, siblings, pending child reports, configuration/control work, and later submissions survive. Explicit close/despawn still closes the selected subtree; other cancellation APIs retain their documented ownership rules. Children spawned by trusted plugin handlers have independent ownership and survive unrelated turn cancellation; explicit close or plugin revocation retires them. `AgentSubscription` handles do not unsubscribe when garbage-collected. A subscription requires trusted authority; delegated tool handlers cannot register one. Call `subscription:close()` to revoke future deliveries and cancel active handlers. Plugin unload also revokes registrations. Delegated tool handlers cannot enter the trusted `maki.api.run_command()` route. They also cannot register commands, keymaps, autocmds, completion sources, expanders, usage callbacks, notify handlers, slots, timers, or deferred callbacks, because those later run with host authority. Mutators that address the focused session or the global mode registry, such as `maki.api.mode.set`, `maki.session.set_thinking`, and `maki.session.set_option` without a `session`, refuse them as well, because they address no explicit target. Callbacks a delegated tool fires synchronously, such as autocmds from `exec_autocmds`, run with the same delegated restrictions. These checks confine what a delegated invocation can register. They do not sandbox the plugin's own code, which other callbacks of the same plugin can still call.

The compatibility `Session` API has different lifetime and admission rules. A trusted-created Session remains usable only by same-plugin handlers with current authority in the same host generation. A tool-created Session expires with its creating invocation. Passing userdata to another coroutine does not transfer authority. Context-free Session methods validate current coroutine authority against the creation policy ceiling. Plugin revocation cancels owned work. `Session:send(message)` queues and returns a boolean; `Agent:send(ctx, message)` requires idle admission and returns an exact-turn ticket. `Agent:enqueue(ctx, message)` queues a distinct FIFO turn and returns its ticket. `Agent:submit(ctx, message)` uses the same ordinary-message runtime as main and subagent UI input. Compatible messages join eligible open work and share its ticket. Idle or admission-closed submissions start a successor. Queued configuration, working-directory changes, control operations, and exact-work boundaries prevent merging across them. Pending messages drain in order before the next eligible model request, after current model/tool work completes. Mid-turn messages use the `<user-interrupt>` wrapper; initial messages remain ordinary user messages. Messages and attachments remain separate. Acceptance does not mean consumption. Queue limits reject further instructions without cancelling accepted work.

`Agent:prompt(ctx, message, { timeout = seconds })` admits an idle-only turn and waits. A timeout stops observation, not execution. Its error table contains `kind = "timeout"`, `message`, and the exact `ticket`. `timeout = 0` is nonblocking and returns that table while the turn is pending. The ticket can be waited on or cancelled from a current valid context. `send` or `enqueue` followed by `ticket:wait` keeps observation and execution ownership explicit.

### Migration from the previous task and Session APIs

Both `maki.agent.spawn` and the retained `maki.agent.session` constructor use the shared model selection. Replace the old constructor options:

```lua
{ model_spec = "provider/model", thinking = "high", fast = false }
```

with:

```lua
{ model = { spec = "provider/model", thinking = "high", fast = false } }
```

A plain `model = "provider/model"` is also accepted. The old `model_spec` and top-level `thinking` and `fast` fields are rejected, including by the compatibility Session constructor.

The blocking `task` and polling `task_get` tools are removed. Callers use `task_spawn`, retain its `task_id` and `turn_id`, and pass both to `task_wait` for the exact turn. Omitting `turn_id` snapshots the latest turn once. A settled wait returns `{ status = "done", result = ..., error = ... }`; an observation timeout returns `{ status = "running" }`. `timeout = 0` observes without blocking or cancelling. `task_send` uses `Agent:submit`: compatible sends join eligible open work and return its turn ID. Idle, admission-closed, or boundary-separated sends receive a successor ID. Acceptance does not mean the instructions have been processed. An exact wait includes later instructions accepted into that turn, but never follows a successor; `task_despawn` closes the task. SDK clients naming `Task` now select `task_spawn` and receive the asynchronous spawn result rather than the completed child answer.

Rename `ui.tool_output_lines.task` to `ui.tool_output_lines.task_spawn` in configuration. The old key fails configuration loading. Rename task-keyed permission rules and saved allow entries to `task_spawn`; old entries no longer match. See [Configuration](/docs/configuration/) and [Permissions](/docs/permissions/). Resumed transcripts with old `task` calls use the default tool view because the removed tool no longer supplies a restore view.

A task's `output_schema` describes its structured final report. `task_wait` returns the validated object in `result.captured`; `result.text` remains the assistant reply. Consuming new instructions invalidates older structured and local captured results; only a fresh report can satisfy completion and appear as `captured`. Child observations do not invalidate reports. UI cancellation preserves managed delegated tasks as described above; explicit subtree close retires them. Standalone children use their host's ownership and cancellation services; this does not make their root a managed graph.

Managed child completions are internal observations, not user instructions. Active shared-actor parents receive them after outstanding tool results and before the next eligible request. Idle parents receive a bounded batch in one continuation, not a new turn per child. Exact waits claim matching pending reports; retained result reads and observation timeouts do not acknowledge them. The main agent may complete while children run. Non-main agents join reporting children before completing, subject to execution limits. Late child reports can wake an idle or locally cancelled agent. A continuation produces a new immutable turn result and can report that result once to its own parent. It never revises an earlier completed or cancelled ticket. Delivery remains child-to-parent; acknowledged reports do not trigger another continuation. Failed continuations retain unconsumed reports without automatic retry loops; explicit work or a newly completed child turn can trigger recovery. Cancellation preserves pending reports and permits a later continuation. Exact waits cannot retract reports already committed to model history. Inboxes are in memory, not durable.

### Focused-agent UI

Main and subagent views share input, queue, status, and controls. The optional subagent label identifies the viewed agent. Model, mode, plan, compact, rewind, shell, reference expansion, and file completion target that agent. Each agent has its own working directory. New children inherit the parent's committed directory; later changes remain local and are ordered with message admission. Delayed commands and pickers retain their original target. Closed targets reject mutation rather than falling back to the main agent. Existing authority and mode ceilings still apply.

The draft, attachments, and input history remain shared across view switches. Pending-message recall or removal addresses one submission before consumption, not its entire turn. A stale selection cannot remove another message after the queue drains. Reset/new-session and `maki.session.*` retain session scope.

### Host-root operations

Non-TUI hosts expose a stable root reference without a managed graph. The current host-root adapter supports the following operations:

| Operation | ACP and SDK root | Fixed print/headless root |
|-----------|------------------|---------------------------|
| `current`, `root`, `get`, `ctx:agent` | Supported | Supported |
| `list` | Root and accessible live plugin-owned standalone children | Root and accessible live plugin-owned standalone children |
| Tool `ctx:model()` and `ctx:mode()` admitted configuration | Supported | Supported |
| `Agent:model` committed selection and `available_models` | Supported | Supported |
| `Agent:set_model`, including options-only updates | Supported through the session coordinator | Rejected before mutation |
| Root turns, transcript, mode, cancellation, close, subscriptions | Unsupported | Unsupported |
| Child spawn | Standalone child | Standalone child |

Host-root `status` contains only `id` and `status = "running"`. It does not expose actor queue or usage fields. Unsupported operations return `operation is unavailable for this host root`. Fixed-root model updates return `model changes are unavailable for this fixed host root`. Host shutdown retires the reference; it cannot resolve a replacement runtime. Standalone children support `submit`; this does not imply managed completion delivery or root steering for their host. Concurrent ACP root prompts still reject.

The [Lua API reference](/docs/lua-api/) defines the method signatures and returned fields. The Context concepts page describes model context windows, not Lua invocation authority.

## A complete real example

The bundled `glob` tool, verbatim: schema, header and restore hooks, error
handling, LLM output truncation, collapsible UI view. It is a bundled plugin,
so it opens with `register_options`, which your own plugin skips:

```lua
local ToolView = require("maki.tool_view")
local shorten_path = require("maki.shorten_path")
local output_limits = require("maki.output_limits")

local NO_FILES_FOUND = "No files found"

local opts = maki.api.register_options(output_limits.extend({
  search_result_limit = { default = 100, min = 10, desc = "Max files returned per search." },
}))

local function glob_view_opts(ctx)
  local tol = ctx:tool_output_lines()
  return { max_lines = (tol and tol.other) or 3, keep = "head" }
end

maki.api.register_tool({
  name = "glob",
  kind = "search",
  description = [[Find files by glob pattern.

- Respects .gitignore.
- Returns absolute paths sorted by modification time (newest first).
- Prefer speculative parallel searches over sequential rounds of glob+grep.]],

  schema = {
    type = "object",
    properties = {
      pattern = { type = "string", description = "Glob pattern (e.g. **/*.rs, src/**/*.ts)", required = true },
      path = { type = "string", description = "Directory to search in (default: cwd)" },
    },
  },

  header = function(input)
    local buf = maki.ui.buf()
    local spans = { { shorten_path(input.pattern or ""), "tool" } }
    if input.path then
      spans[#spans + 1] = { " in ", "dim" }
      spans[#spans + 1] = { shorten_path(input.path), "path" }
    end
    buf:line(spans)
    return buf
  end,

  restore = function(_input, output, _is_error, ctx)
    return ToolView.restore(output, glob_view_opts(ctx))
  end,

  handler = function(input, ctx)
    local pattern = input.pattern
    if not pattern then
      return { llm_output = "error: pattern is required", is_error = true }
    end

    local limit = opts.search_result_limit
    local max_lines, max_bytes = output_limits.resolve(opts, ctx)
    local path, path_err = ctx:resolve_path(input.path or ".")
    if not path then
      return { llm_output = "error: " .. tostring(path_err), is_error = true }
    end

    local files, err = maki.fs.glob(pattern, {
      path = path,
      gitignore = true,
      sort = "mtime",
      limit = limit,
    })

    if not files then
      return { llm_output = "error: " .. err, is_error = true }
    end

    if #files == 0 then
      return { llm_output = NO_FILES_FOUND }
    end

    local lines = {}
    for i, f in ipairs(files) do
      lines[i] = shorten_path(f)
    end
    local text = table.concat(lines, "\n")
    local llm_output = text

    local buf = maki.ui.buf()
    local view = ToolView.new(buf, glob_view_opts(ctx))
    for _, line in ipairs(lines) do
      view:append(line)
    end
    view:finish()
    buf:on("click", function()
      view:toggle()
    end)

    return {
      llm_output = llm_output,
      output_limits = { max_lines = max_lines, max_bytes = max_bytes },
      body = buf,
    }
  end,
})
```

## Full API reference

Every module, function, and method is in the [Lua API reference](/docs/lua-api/).
The agent gets the same document on disk through the builtin
`maki-plugin-dev` skill, so asking it to write a plugin for you works
without pasting any of this.
