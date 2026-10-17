+++
title = "Quick Start"
weight = 1
[extra]
group = "Getting Started"
+++

# Quick Start

Install Makima, connect a provider, run a first session. A few minutes, start to finish.

## Install

### Linux / macOS

```sh
# Download and read the script first (don't blindly trust shell scripts).
curl -fsSL https://makima.ln4.net/install.sh -o install.sh
cat install.sh

# Then run.
chmod +x install.sh && sh install.sh
```

One-liner:

```sh
curl -fsSL https://makima.ln4.net/install.sh | sh
```

Installs to `~/.local/bin`. Override with `MAKIMA_INSTALL_DIR`.

### Windows (PowerShell)

```powershell
# Download and read the script first (don't blindly trust remote scripts).
irm https://makima.ln4.net/install.ps1 -OutFile install.ps1
Get-Content install.ps1

# Then run.
.\install.ps1
```

One-liner:

```powershell
irm https://makima.ln4.net/install.ps1 | iex
```

### Windows (Git Bash)

```sh
curl -fsSL https://makima.ln4.net/install.sh | sh
```

Both install to `%LOCALAPPDATA%\makima` and add it to your user PATH. Override with `MAKIMA_INSTALL_DIR` / `$env:MAKIMA_INSTALL_DIR`.

### Living on the edge (main branch)

```sh
cargo install --locked --git https://github.com/lun-4/makima.git makima
```

### With Nix

```sh
nix run github:lun-4/makima
```

Or download a pre-built binary from [GitHub Releases](https://github.com/lun-4/makima/releases/latest).

## Connect a provider

```bash
makima auth login              # interactive picker (OAuth or API key)
export ANTHROPIC_API_KEY=... # or just export a key
```

Anthropic, OpenAI, Google, Ollama, and friends all work; multiple keys in one var rotate on rate limits. Every env var and model catalog is in [Providers](/docs/providers/).

## First session

From a repo:

```bash
makima
```

Type what you want done, press Enter, watch it work. Worth knowing on day one:

- **Permissions.** File edits inside the repo run freely. `bash` and web tools ask first: `y` allows once, `s` for the session, `a` for the project. Deny rules always win; `/yolo` skips the prompts. Details in [Permissions](/docs/permissions/).
- **Plan mode.** `Tab` toggles it. The agent may only write the plan file until you approve, then back to build mode.
- **Models.** `/model` switches mid-session. Type `@` in the input to reference a file, skill, subagent, or model inline; see [References](/docs/references/).
- **Sessions.** `/new` starts a second session while the first keeps working in the background; `/sessions` jumps between them. Tomorrow, `makima -l` resumes where you left off, `makima sessions --json` lists stored sessions, and `makima -c` opens the session picker for this directory (or `makima -c <ID>` jumps straight in).
- **Your shell.** Prefix input with `!` to run a command yourself (`!cargo test`). `!!` hides command and output from the agent.
- Cancellation. `Esc Esc` stops the viewed agent's current work and clears its pending ordinary messages. Descendants continue. When idle, it opens rewind instead. See [Agent input and cancellation](#agent-input-and-cancellation).
- **Help.** `Ctrl+H` lists every keybinding, or see [Keybindings](/docs/keybindings/).

Middle-click the conversation transcript to start continuous scrolling, then release the button. A marker shows the starting position. Move above it to scroll up or below it to scroll down; greater vertical distance increases speed, while the starting row and one row on either side pause scrolling. Scrolling continues when the pointer stops. Middle-click again or press Escape to stop; other key presses, paste, mouse clicks and wheel scrolling also stop it and retain their usual action. This works only in the transcript, not input fields, pickers or overlays. The terminal must report middle-clicks and pointer movement without a held button; some terminals or multiplexers paste on middle-click or do not report these events.

## Agent input and cancellation

Main and subagent views share the same input, queue, status, and controls. The subagent label identifies the viewed agent. The draft, attachments, and input history remain shared when the view changes.

Ordinary messages start idle work or join compatible open work. Pending messages drain in order before the next eligible model request, after current model/tool work completes. Sending a message does not interrupt an in-flight provider request or tool. Configuration changes, working-directory changes, control operations, and explicitly queued turns prevent messages from joining earlier work across those boundaries. Pending messages can be recalled or removed individually before consumption. Removing one message does not cancel its whole turn.

Model, mode, plan, compact, rewind, shell commands, reference expansion, and file completion target the viewed agent. Each agent has its own working directory. A new child inherits the parent's committed directory; later directory changes affect only the target agent and are ordered with message admission. Commands and pickers keep the target captured when they start, even if the view changes. A closed target returns an error rather than redirecting work to the main agent. Agent restrictions still apply. Reset/new-session remains session-wide.

UI cancellation stops the viewed agent's captured current work and clears only its ordinary messages accepted before cancellation. Descendants, siblings, pending child reports, configuration/control work, and later submissions survive. Explicit close through `task_despawn` instead retires the selected task and its descendants.

The main agent may finish while children run. Non-main agents join reporting children before completing, subject to execution limits. Late child reports can resume an idle or locally cancelled agent. Resumed work produces a new immutable turn result and may report it to its parent. Earlier completed or cancelled results remain unchanged. Exact task waits stay attached to their original turn; they do not follow a successor. See [Task tools](/docs/tools/#task_wait) for wait and retention rules.

## Default model (optional)

```lua
-- ~/.config/makima/init.lua
maki.setup({
    provider = {
        default_model = "anthropic/claude-sonnet-4-6",
    },
})
```

Without it, Makima remembers the last model you used.

## Teach it your project

Makima loads `AGENTS.md` (or `CLAUDE.md`, `.cursorrules`, and friends) from your repo automatically. Per-project settings live under `.makima/`:

```
.makima/
├── init.lua           # overrides global config
├── permissions.toml   # permission rules
├── mcp.toml           # MCP server config
├── commands/          # custom slash commands (.md files)
└── skills/            # project skills (each dir has a SKILL.md)
AGENTS.md              # always in context
AGENTS.local.md        # personal per-project instructions (gitignored)
```

Which instruction file wins, when subdirectory rules load, and how skills and memory fit together: [Context](/docs/context/). All settings: [Configuration](/docs/configuration/).
