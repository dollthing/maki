## Goal

Main and subagents use one ordinary-message runtime path and one focused-agent UI implementation. Queueing, safe-boundary consumption, input handling, and agent controls behave consistently, with an optional subagent label and an explicit main-agent completion policy.

## Implementation Summary

Replace the parallel `RootWork` and instruction-inbox message paths with one actor-owned pending-submission mechanism. Retain the atomic admission/finish protocol from `AgentActorHandle::submit` and the main queue's presentation, full-input support, and configuration ordering. Share the input renderer and handlers instead of copying the main UI into a child implementation.

Affected packages are `maki-agent`, `maki-ui`, `maki-lua`, bundled task/command plugins, and generated references. Existing focused-agent authority and `AgentRef` targeting remain authoritative. No new UI-side message queue, root-versus-child scheduler, or generic mailbox replaces actor ownership.

The following product decisions are fixed:

- Ordinary messages start idle work or join eligible open work. Pending messages drain in order before the next eligible model request, after any current model/tool work completes. Incompatible configuration and explicit-work boundaries prevent merging.
- Mid-turn messages retain the main agent's `<user-interrupt>` wrapper for both agent types. Initial messages remain ordinary user messages. Preserve separate messages and attachments rather than concatenating text.
- Main agents may complete while children run. Non-main agents join reporting children before completing. Express this as an explicit completion policy, independent of the pending-message implementation.
- One UI implementation renders the focused agent's queue, input, status, and controls. Retain the optional top-right subagent input label. A closed agent never falls back to the main agent.
- Model, mode, compact, rewind, shell, reference expansion, and file completion target the viewed agent. Working directories are per-agent. Reset/new-session remains session-wide. Preserve the shared draft, attachments, and history.
- UI cancellation stops only the viewed agent's captured current work and clears that agent's pre-cut pending ordinary messages. Descendants, siblings, child results, configuration/control work, and post-cut submissions survive. Late child results may start a new continuation.
- Continuation results report upward as new immutable turn results. They never revise an earlier completed or cancelled ticket.

Public task tool JSON shapes remain unchanged. `submit` remains start-or-steer, exact Lua `enqueue` remains a separate FIFO execution, and idle-only `send` retains its explicit admission policy. These policies apply to agents regardless of topology. Existing authority, mode ceilings, retention limits, provider limits, and offload rules remain enforced.

Non-goals are per-agent drafts, sibling messaging, persistent exactly-once delivery across process restarts, and unrelated provider or sandbox changes. This change does not convert every session API into a focused-agent API. Session APIs retain session scope; UI agent actions use an explicit agent target.

## Implementation Plan

### 1. Add production-path parity fixtures

Extend existing fixtures rather than creating a separate runtime simulator. Use `maki-agent/src/actor/tests.rs`, `maki-lua/tests/{agent_handles,task_policy,managed_session}.rs`, `maki-ui/src/app/tests.rs`, and event-loop/agent-loop tests.

Add reusable fake-provider gates for request entry, tool completion, pre-request drain, admission close, and cancellation cut. Expose gates only through test support at the real transition. Capture provider inputs, turn IDs, consumption events, terminal events, UI buffers, and per-agent configurations. Add a root/child fixture parameter that changes identity and completion policy without changing submission behavior. Add nested A -> B -> C scenarios for continuation and cancellation.

Prove the fixtures exercise real production adapters: keyboard/root queue submission, child-tab submission, bundled `task_send`, Lua exact admission, and both actor backends. A gate test must stop the actual request/finish transition and resume it deterministically. Do not use sleeps, live models, or only scripted backend polling as evidence of provider-boundary behavior.

### 2. Consolidate ordinary-message admission and execution

Relevant sources are `maki-agent/src/actor/mod.rs:1710` (`submit`), `actor/queue.rs:82` (root batching), `actor/queue.rs:303` (root interrupt extraction), `actor/inbox.rs:121` (finish handshake), and `agent/run.rs:648,982,1039` (consumption). The main adapter selects execution semantics from display state in `maki-ui/src/agent/shared_queue.rs:286`. Remove that coupling.

Generalize actor pending entries to contain a stable submission ID, full `AgentInput`, per-message provenance, presentation metadata, captured effective configuration/admission preparation, and the execution ticket covering the input. Keep turn identity distinct from submission identity because multiple messages can share one turn. Preserve each message's text, images, MCP startup notice, and presentation status.

Use one ordered actor admission mechanism for root input, child input, and `task_send`. Resolve compatibility against effective configuration, mode/workflow, prompt/tool bindings, and explicit operation boundaries. Authorized sender provenance remains attribution and cannot broaden execution authority. Preserve isolation requirements for custom prompts, cancellation ownership, and lease committers. Do not use blanket provenance equality to prevent an authorized user from steering plugin-created work.

An active turn accepts ordinary messages only when no intervening queued configuration, explicit execution, or control boundary requires a successor. A new message behind a boundary receives the appropriate successor ticket and cannot jump ahead into old-policy work. Admission preparation remains outside actor locks and respects the existing reserved-operation order. Attachments and preambles use this same path; remove `submit`'s text-only limitation without dropping provider validation or byte limits.

Preserve explicit admission policies for exact FIFO execution and idle-only execution within the same actor machinery. Pending-compatible ordinary submissions share the ticket that covers their execution. Rejected input creates neither a pending row nor a chat bubble. Bound retained unique tickets and pending payloads without silently evicting accepted input.

Replace root scheduling with the common mechanism and delete `RootWork`, `EarlierRoot`, root variants of `ActorOperation`/`ActorWork`/`WorkKind`, `run_root`, and root-only batching/extraction once adapters migrate. Keep compaction/control ordering as ordered work. Replace the message portion of `InterruptQueue`; do not accidentally delete compaction support. Migrate headless/restored-input consumers of root work rather than leaving a hidden second ordinary-message path.

### 3. Share consumption, queue projection, removal, and completion

Drain eligible submissions through one pre-request consumption function used by both backends. Emit one typed consumption notification per submission, including its stable identity and display metadata. Apply the interruption wrapper only to inputs joining an execution after its initial input. Retain MCP notices, tool-result group integrity, history order, and report invalidation when new instructions arrive.

Keep accepted inputs outstanding until they cross the model-request boundary. Finish and admission use the same actor-state synchronization: a message either joins before closure or receives a successor ticket. No successful outcome may cover an accepted input omitted from provider requests. Cancellation, preparation failure, provider failure, and budget exhaustion resolve observably without reopening an immutable result.

Project all pending ordinary submissions through the same queue API, including deferred preparation and explicit queued turns where their input is pending. The UI reads the selected actor's authoritative projection. Give removal and queue recall a submission-ID operation; a stale ID cannot remove the next row after a concurrent drain. Removing one pending instruction must not cancel its entire active turn or other joined instructions. Define pending as not yet committed to history/request preparation; after drain commits, removal returns a benign unavailable result.

Reuse the shared `TurnTicket`/`TurnResult` finalizer for UI completion, task waits, and parent delivery. Seal terminal results before terminal observers see completion. Keep usage/lifecycle accounting once per execution. Preserve exact wait snapshot semantics, timeout behavior, permit yielding, and wait-versus-automatic-delivery withdrawal.

Make child joining an explicit actor completion policy: main nonjoining, non-main joining. Preserve it for ordinary and resumed work. Do not select separate input consumption code based on that policy.

### 4. Support upward continuation results without report loops

Current suppression is in `maki-agent/src/actor/delivery.rs:351` and continuation scheduling is near `:532`. Replace blanket delivery-only-turn suppression with causal delivery accounting.

Each continuation records the child completion identities it consumes. Its new terminal result can be delivered once to its own parent under its own turn identity. Delivery remains strictly child-to-parent along recorded graph edges. A parent receipt never travels downward as a new child completion, and processing an already acknowledged completion cannot schedule another continuation. Distinct later child turns remain deliverable. Bound previews and batches; retain full-result references and existing offload behavior.

Prevent empty continuations after a successful exact-wait withdrawal. Preserve consumed-report acknowledgment after provider failure so failed execution does not replay the same report indefinitely. If a continuation fails with unconsumed reports, retain those reports and suspend automatic retry for that failed batch until explicit work or a newly completed child turn provides a recovery trigger. Cancellation is different: preserve pending reports and allow one subsequent continuation, as requested. A new turn always gets a new immutable outcome; the cancelled turn stays cancelled.

Verify a finite A -> B -> C chain reaches quiescence after B consumes C and reports to A. Also verify a report arriving during finalization starts or joins legitimate successor work exactly once. Existing `task_wait` and automatic-delivery claim rules remain unchanged; this feature does not promise retraction after a report already committed to model history.

### 5. Implement one target-local UI cancellation cut

Current manager `cancel_turn` closes delegated descendants (`maki-agent/src/manager/mod.rs:1166`). Main UI cancellation also clears descendant channels and chat state (`maki-ui/src/app/mod.rs:1902`). Neither behavior matches the requested UI stop contract.

Add or refine an actor-local cut that captures the selected processing turn, pending ordinary submission IDs, and presentation fence atomically. Settle the captured execution and removed submissions exactly once. Keep configuration/control operations, child deliveries, descendant actors, and submissions accepted after the cut. Retire cancelled-turn authority and permit/wait registrations without closing children. Keep explicit subtree-close/despawn APIs separate and unchanged.

Route root and child stop gestures through this same target-pinned cut. Do not perform a second asynchronous cancellation against whatever turn starts later. Update only the selected chat's in-progress state and local shell execution. Preserve descendant displays, event channels, and queues. Late reports may wake the selected actor and publish a new turn/result upward. If no active work exists, clear only the selected pending ordinary messages.

### 6. Expose focused-agent state and agent-scoped operations

Reuse trusted focused-agent resolution in `maki-ui/src/event_loop.rs:2307` and target-pinned model picker behavior in `maki-ui/src/app/mod.rs:1280`. Avoid a parallel focus registry. Store the main agent's identity explicitly; completion policy must not depend on the currently selected tab.

Provide the UI a common target view of actor lifecycle, busy state, queue, effective model/mode/options, context usage/window, retry state, cwd, and supported operations. Shared input uses this view rather than root `SessionState`. Closed targets reject mutation with a visible error and never route elsewhere. Historical child chats remain viewable without acquiring a live mutation capability.

Add an immutable `AgentContextSnapshot` containing cwd and a context generation to the actor-owned effective configuration. Initialize it for every main/child actor. Make cwd updates ordered actor configuration operations using the existing reservation/commit protocol; do not add an independently mutable cwd slot. Admission captures the committed context snapshot together with model/mode configuration. New children inherit the parent's committed snapshot at construction; later changes remain local. Replace reads from the child's `OnceLock<ToolContext>` cwd and root orchestration cwd slots with this snapshot. Keep the immutable template for services and authority, but derive per-execution tool contexts, prompt inputs, environment, and trusted child-construction templates from the captured context generation.

Order cwd changes with configuration/admission preparation so queued work captures the correct directory, instructions, references, MCP prompts, tool context, and environment. Root cwd persistence remains session persistence; child cwd remains actor state unless existing child restoration can preserve it without a new persistence format. Completion, path rendering, file picker, shell, and reference expansion resolve from the pinned agent cwd.

Route mode/model changes through the selected actor's existing authority/configuration machinery. Retain mode ceilings and plan-write restrictions.

Compact and rewind operate on selected history under an idle/admission barrier, preserving complete tool groups and invalidating stale transcript generations. Introduce an actor-local `rewind` operation instead of calling the current session-replacement implementation in `maki-ui/src/app/session.rs:328`. The picker obtains actor-owned boundaries identified by actor identity, history epoch, and message offset at a valid user-message boundary; the epoch plus offset is stable for that history version and is never interpreted against a different epoch.

The runner serializes the mutation through a typed history-edit operation owned by an idle reservation. Extend reservation dispatch to admit only that owner's history-edit operation while blocking ordinary work and delivery continuations; do not enqueue an ordinary control that the existing reservation blocks. Reject active/pending explicit work with `Busy`, and validate the boundary immediately before mutation. Add `ActorError::StaleHistoryBoundary` for an epoch mismatch, removed/compacted boundary, invalid tool-group cut, or wrong actor. New appends do not change the history epoch; rewrites and compaction do.

The operation stages the history prefix ending immediately before the selected user message and retains that message as the proposed shared draft. Do not publish candidate history or mutate the draft during preparation. On successful checkpoint acknowledgment, install the prefix with `History::replace`, publish a new epoch, apply the proposed draft, and prune only the selected history's orphan display/tool references.

Keep actor identity, graph edges, child channels, and immutable retained ticket results. Do not use `Action::ReplaceSession` for rewind. Pending child deliveries remain outside the rewritten history and can commit only after the reservation releases, including reports arriving during rewind. Publish one typed history-change result to update the selected chat and context usage.

For main history, use the coordinator's staged checkpoint-before-publication ordering: serialize the candidate replacement through the owning storage writer, await acknowledgment, then commit actor history while the reservation still blocks execution. Extend the existing history-replacement checkpoint generation so earlier asynchronous saves cannot overwrite the replacement. On checkpoint failure discard the staged candidate and leave published history unchanged.

For child history, checkpoint the owning session's candidate child-history projection through the same serialized writer before committing the child actor mirror. Child history changes must not overwrite unrelated root or sibling updates; use a child-keyed storage operation rather than a stale full-session clone. Release the reservation only after commit/result publication or failure cleanup. Invalid or failed mutation leaves the picker selection, draft, history, and actor graph unchanged.

Scope plan forms and plan paths to the selected agent rather than mutating root plan state.

Shell output and resulting context belong to the selected chat/agent. Session reset/new-session remains a separate session operation. Capture target identity when commands, editor requests, or pickers start so later focus changes do not retarget them. Audit builtin and bundled command dispatch before enabling it in the shared handler; agent commands use explicit agent operations, session commands use explicit session operations. Preserve `maki.session.*` session-root contracts; correct any accidental selected-tab routing in session prompt APIs and test it.

### 7. Remove child-specific UI pipelines

Unify layout and rendering in `maki-ui/src/app/view.rs:128,274`. Compute queue, panel, input, overlay, and selection rectangles once. Extend the main shared input metadata to include the optional top-right subagent label and closed-state information. Do not retain a second child bottom-panel renderer or copy the main branch.

Remove the child keyboard early return (`maki-ui/src/app/mod.rs:1471`), reduced submission handler, duplicate paste path, duplicate double-Esc timer handling, and child-only mouse/input-zone branches. One pipeline supports editing, history, editor integration, search, model picker, palette, completion, image paste, queue recall/removal, and cancellation. Preserve one shared draft/history across view switches. Scope queue focus by target identity or reset it on switch; stale focus cannot mutate another queue.

Render busy/idle/closed state from the selected actor even when the root is in a different state. Use selected model/options/context/retry information throughout status, help, separator styling, and cadence. Render queued bubbles only on shared consumption events, exactly once for keyboard, Lua, and task submissions. Typed child-completion notices remain distinct from user bubbles.

Remove child string `input_tx` transport and `spawn_ui_input_relay` (`maki-lua/src/api/agent.rs:1863`) after switching child UI to acknowledged actor admission. Preserve question-answer channels and explicit close/despawn operations. Remove the main-only guard around consumed-message rendering (`maki-ui/src/app/mod.rs:2205`).

### 8. Update contracts and run verification

Update existing task/API descriptions, user docs, command metadata, and relevant project guidance. Regenerate generated references through `maki-docgen`. Run scoped remote checks throughout, then full remote CI and independent implementation review. Record actual commands/results only after execution. Do not commit or push unless requested.

## Acceptance Criteria

Every named test below is required new coverage or an explicit strengthening of an existing scenario. Parameterized tests must include main and child targets where the contract is shared.

- AC.1 Ordinary keyboard, Lua submit, and task submissions use the same start-or-steer behavior and preserve all messages/images in eligible provider requests. Tests: `ordinary_submission_paths_have_runtime_parity`, `shared_submission_preserves_images_and_preamble`.
- AC.2 Configuration, cwd, control, and exact-work boundaries preserve FIFO behavior and authority; incompatible input never enters old-policy requests. Tests: `submission_respects_queued_boundaries`, `mixed_sender_submission_preserves_authority`, `cwd_admission_order_is_target_local`. The cwd test asserts prompt cwd, executed tool cwd/environment, reference/file completion roots, and newly spawned child inheritance on both sides of a queued cwd change for both backends.
- AC.3 Finish races never acknowledge omitted input as successful; wrappers, report invalidation, notices, and tool-group order match for both backends. Tests: `shared_submit_finish_race_both_sides`, `shared_consumption_wrapper_and_request_order`, `shared_instruction_failure_and_budget_outcomes`, `steering_invalidates_structured_report`.
- AC.4 Both views expose the selected actor's queue and support message-specific removal/recall without stale-index deletion or duplicate bubbles. Tests: `focused_queue_consumption_and_recall_parity`, `pending_submission_removal_races_drain`, `rejected_admission_has_no_phantom_bubble`.
- AC.5 Exact enqueue, idle-only send, task ticket retention/waits, usage, and lifecycle notifications retain their explicit contracts. Tests: `exact_and_idle_admission_policies_survive_unification`, `task_shared_turn_wait_and_retention`, `shared_turn_events_and_usage_once`, `nested_wait_capacity_one_progress`.
- AC.6 Main completion remains nonjoining; non-main completion joins reporting children, without selecting a different message mechanism. Test: `completion_policy_main_vs_nonmain` through real provider loops.
- AC.7 Local cancellation clears only selected pre-cut ordinary work, leaves descendants/config/results alive, and preserves post-cut input. Tests: `viewed_agent_cancel_is_local`, `local_cancel_submission_and_install_races`, `cancel_preserves_config_and_reports`, `cancel_releases_wait_without_closing_child`.
- AC.8 Late child results wake cancelled agents and publish new immutable results upward exactly once; a finite graph reaches quiescence without report loops. Tests: `cancelled_parent_resumes_and_reports_upward`, `nested_continuations_report_once_and_quiesce`, `continuation_finalization_arrival_and_wait_claim_races`, `failed_continuation_recovery_is_bounded`.
- AC.9 Model, mode, plan, compact, rewind, shell, and cwd operations mutate only their pinned agent; reset/new-session and session APIs retain session scope. Tests: `focused_agent_command_target_matrix`, `focused_history_operations_preserve_identity_and_children`, `rewind_rejects_compacted_picker_boundary`, `rewind_serializes_concurrent_child_report`, `rewind_checkpoint_failure_restores_history`, `focused_plan_state_is_isolated`, `session_operations_do_not_follow_child_focus`. These assert unchanged actor identity/tickets and delayed delivery commit across rewind, including storage acknowledgment failure.
- AC.10 One shared UI provides keyboard, paste, editor, palette, completion, search, mouse, status, and layout parity, with only the optional subagent decoration and target state differing. Tests: `main_child_input_interaction_parity`, `focused_agent_render_layout_matrix`, `focused_agent_status_independent_of_root`, `shared_draft_survives_focus_switch`.
- AC.11 Closed/disconnected targets and delayed commands never reroute to another agent; restricted agents cannot gain permissions through shared controls. Tests: `closed_agent_rejects_without_root_fallback`, `delayed_ui_action_keeps_agent_target`, `focused_controls_enforce_mode_ceiling`, `rewind_rejects_compacted_picker_boundary`. Rewind errors must not fall back to session replacement or another actor.
- AC.12 Pending/restored/headless input remains observable and bounded after root-path removal; reports, notices, and persistence do not duplicate messages. Tests: `restored_and_headless_input_use_shared_submission`, `shared_pending_bounds_and_multibyte_payloads`, `child_completion_notices_remain_typed`.
- AC.13 Production test gates block the real provider/admission/finish/cancel transitions, and nested-graph quiescence waits for drained scheduling and terminal publication rather than elapsed time. Tests: `runtime_gate_controls_real_transition`, `graph_quiescence_probe_detects_extra_continuation`. Add a test-only graph drain probe that takes actor scheduling/publication barriers after all reachable work tickets settle, rechecks pending operations/deliveries and runner work, and repeats to a fixed point. Assert expected continuation/event counts at that barrier; deliberately schedule one extra continuation to prove the probe detects it.
- AC.14 Public descriptions match behavior and repository checks pass. Tests/checks: `task_and_agent_docs_match_shared_runtime`, `generated_docs_current` (`just gen-docs-check`), `formatting_current` (`just fmt-check`), `workspace_ci` (`.ssh/remote-ci.sh`).

## Test Strategy

Actor unit tests cover ordered admission, compatibility, stable message identity, removal, cancellation cuts, finish handshake, accounting, delivery causality, and bounds. Use deterministic transition gates and `#[test_case]` variants. Shared message/status constants prevent divergent test expectations.

Production-path integration tests use real bundled task plugins, managed graph edges, real TUI/Lua actor backends, and fake providers. Assert complete provider request histories and terminal results, not only successful channel sends. Exercise initial/pending/active/finalizing states, preparation errors, provider errors, max-turn exhaustion, cancellation, exact-wait withdrawal, and capacity-one nesting. AC.1-AC.3 and AC.5-AC.8 require these integration assertions in addition to unit tests.

TUI scenario/render tests dispatch real key/paste/mouse events and render through ratatui test buffers. Compare normalized main/child output while allowing the explicit label, identity, and completion policy. Exercise root busy/idle crossed with child busy/idle/closed, multiline drafts, image attachments, float panels, permission/question/plan takeovers, and small terminal dimensions. Command matrix tests pass through actual command resolution and event-loop mutation, not mocked target setters. Assert descendant channels remain usable after local cancellation.

The missing infrastructure is cross-backend gated provider fixtures and nested continuation quiescence probes. Phase 1 builds these. `ordinary_submission_paths_have_runtime_parity`, `shared_submit_finish_race_both_sides`, and `nested_continuations_report_once_and_quiesce` prove the fixtures reach real request/finalization paths. Quiescence checks await actor events and explicit bounded event counts; they do not infer absence from sleeps.

AC mapping is explicit in Acceptance Criteria. Structural removal is also reviewed through the final diff: executable parity tests cannot alone prove the absence of duplicate implementation. Source review must confirm no ordinary RootWork lane, child input relay, or second child UI handler survives. This supplements, rather than replaces, behavioral tests.

No Rust builds run during planning. During implementation, local `just fmt` and `just fmt-check` are allowed. Follow `AGENTS.local.md`; inspect `.ssh/remote-ci.sh` and run scoped `cargo check -p <crate> --tests`, clippy, and nextest on the remote build box. Regenerate docs remotely, then run `.ssh/remote-ci.sh` for final `just ci`. Preserve unrelated untracked files and SSH state.

## Review Strategy

Review the finished plan using `PLAN_REVIEWER_PROMPT` from `plugins/task/init.lua` and `plugins/lib/maki/plan_spec.lua`. The operator explicitly requests a general subagent carrying that read-only reviewer prompt because the dedicated reviewer is unavailable in build mode. The reviewer may read/search files but must not edit files, use shell, or spawn other agents. Fix every finding or record an explicit rebuttal. Repeat review after critical/high findings until no such findings remain. Record the actual verdict after review.

Plan review record, 2026-10-17: a general subagent applied the repository `PLAN_REVIEWER_PROMPT`, shared plan specification, and full plan-mode directive under read-only restrictions. The first pass identified rewind-contract, cwd-ownership, and infrastructure-acceptance gaps. The plan now specifies reservation-owned history edits, checkpoint-before-publication with child-keyed persistence, authoritative agent context snapshots, and explicit fixture acceptance tests. After clarification and re-review, the final pass returned no remaining findings and `VERDICT: pass`. This records plan approval by the reviewer, not implementation approval or test execution.

After implementation and all automatable checks, load the repository `code-review` skill and obtain independent review. Audit shared-path deletion, lock ordering, configuration capture, atomic close/cancel cuts, immutable result publication, local cancellation authority, upward continuation termination, target pinning, history/plan/cwd isolation, restored input, and generated contracts. Fix or explicitly rebut every finding. Repeat after critical findings until none remain or an operator decision blocks progress.

## Documentation Strategy

Edit existing handwritten docs for task completion, cancellation, UI targeting, commands, and agent APIs. Explain the main/non-main child-joining exception, safe-boundary interruption rather than immediate provider interruption, pending-message removal, per-agent cwd, local cancellation, and automatic continuation after cancellation. State that resumed work has a new result and may notify its parent.

Update bundled task descriptions and Rust/Lua API metadata, then regenerate references through `maki-docgen`; never hand-edit generated pages. Update existing architecture guidance to describe the shared pending-submission path and explicit completion policy. The operator-requested plan is the only new documentation artifact required. Do not claim tests or implementation are complete before execution.

## Risks, Blockers, and Required Decisions

All product decisions above are resolved with the operator. The broader scope includes agent-scoped commands/history/cwd, not only queue rendering. No implementation has started and no build/test result is claimed.

- Upward continuation reporting replaces an existing no-echo safeguard. Directional graph delivery, per-turn deduplication, consumed-record acknowledgment, and finite-chain tests are required before enabling it.
- Root cancellation currently bundles descendant closure with exact-turn cancellation. Preserve explicit destructive APIs while changing UI stop to the local atomic cut. A delayed stop must not cancel a successor.
- Replacing root work affects more adapters than the interactive root UI. Restored/headless input and compaction/control dispatch must migrate in the same change; a compatibility shim must not become a permanent second ordinary-message mechanism.
- Current runtime config does not represent every cwd/history/plan concern. Add agent-owned context where needed and preserve admission barriers, mode ceilings, complete tool groups, and immutable ticket results. Do not implement agent commands by transiently swapping root session state.
- Exact-wait withdrawal cannot retract a report already inserted in model history. Preserve that existing limit and test the claim race; no stronger exactly-once model-observation guarantee is claimed.
- Continuous input and child results can prolong work. Preserve execution and queue bounds; failures remain explicit. A new child completion may wake a cancelled agent by deliberate operator choice.
- UI parity can expose formerly unreachable commands on restricted or closed agents. Target capability checks and visible errors must precede mutation. Main/child code sharing must not widen permissions.
- Net UI deletion is expected. Net repository deletion is not an acceptance condition because stable input identity, target-local operations, and production-path test fixtures require new code.
