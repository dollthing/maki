use std::sync::Arc;

use maki_agent::actor::{ConfigChange, ConfigPatch, PendingQueueEntry, PreparedModel};
use maki_commands::{CommandError, CommandOutcome, HostContextRequest, HostRequest};
use test_case::test_case;

use super::tests::{select_child_chat, with_event_loop};
use super::{
    CLOSED_TARGET_ERR, CommandEvent, EventLoop, SESSION_OP_TIMEOUT, SessionRequest, UiAction,
};
use crate::app::shell::ShellEvent;
use crate::chat::Chat;
use crate::components::Action;

const RESTORED_CHAT: &str = "restored task";
const PROMPT: &str = "session prompt";
const SHELL_OUTPUT: &str = "shell output";
const EXPANDER_PLUGIN: &str = "session_prompt_expanders";
const EXPANDER_FAILURE: &str = "reference expansion rejected";
const EXPANDED_PROMPT: &str = "expanded task";
const OVERSIZED_PROMPT_BYTES: usize = 300 * 1024;

fn install_expanders(event_loop: &mut EventLoop<'_>) -> maki_lua::PluginHost {
    let host = maki_lua::PluginHost::with_command_registry(
        Arc::clone(maki_agent::tools::ToolRegistry::global_arc()),
        event_loop.ctx.command_runtime.registry.clone(),
        false,
    )
    .unwrap();
    host.load_source(
        EXPANDER_PLUGIN,
        &format!(
            r#"
        maki.api.register_expander("ok", function(_) return "{EXPANDED_PROMPT}", nil end)
        maki.api.register_expander("reject", function(_) return nil, "{EXPANDER_FAILURE}" end)
    "#
        ),
    )
    .unwrap();
    event_loop.ctx.lua_event_handle = host.event_handle();
    event_loop.sessions[0].app.lua_event_handle = host.event_handle();
    host
}

fn select_restored_chat(event_loop: &mut EventLoop<'_>) {
    let runtime = Arc::clone(&event_loop.ctx.command_runtime);
    let app = &mut event_loop.sessions[0].app;
    app.command_runtime = Arc::clone(&runtime);
    app.command_target = runtime.bind_target();
    event_loop.sync_orchestration();
    assert!(
        runtime
            .target_agent(&event_loop.sessions[0].app.command_target)
            .is_some()
    );
    let app = &mut event_loop.sessions[0].app;
    let mut chat = Chat::new(
        RESTORED_CHAT.into(),
        app.ui_config.clone(),
        app.lua_event_handle.clone(),
        Arc::clone(&app.theme_provider),
    );
    chat.subagent_id = Some(RESTORED_CHAT.into());
    app.chats.push(chat);
    app.active_chat = 1;
    event_loop.sync_orchestration();
    assert!(
        runtime
            .target_agent(&event_loop.sessions[0].app.command_target)
            .is_none()
    );
    assert!(event_loop.selected_target(0).is_none());
}

fn dispatch_slash(event_loop: &mut EventLoop<'_>, command: &str) -> CommandOutcome {
    let runtime = Arc::clone(&event_loop.ctx.command_runtime);
    runtime.dispatch_input(&event_loop.sessions[0].app.command_target, command.into());
    loop {
        let event = runtime.recv_timeout_for_test(SESSION_OP_TIMEOUT).unwrap();
        if let CommandEvent::Outcome { outcome, .. } = event {
            return outcome;
        }
        event_loop.handle_command(event);
    }
}

#[test_case("/compact"; "compact")]
#[test_case("/cd /"; "directory")]
#[test_case("/fast"; "fast")]
#[test_case("/workflow"; "workflow")]
#[test_case("/model"; "model_picker")]
#[test_case("/model anthropic/claude-sonnet-4-6"; "model_selection")]
fn closed_agent_rejects_without_root_fallback(command: &str) {
    with_event_loop(|event_loop| {
        select_restored_chat(event_loop);
        let runtime = Arc::clone(&event_loop.ctx.command_runtime);
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let config = actor.effective_config().unwrap();
        let history = event_loop.sessions[0].handles.history.load_full();
        let _guard = actor.prepare_idle().unwrap();
        runtime.dispatch_input(&event_loop.sessions[0].app.command_target, command.into());
        loop {
            let event = runtime.recv_timeout_for_test(SESSION_OP_TIMEOUT).unwrap();
            let finished = matches!(event, CommandEvent::Outcome { .. });
            if let CommandEvent::Outcome { outcome, .. } = &event {
                assert!(matches!(
                    outcome,
                    CommandOutcome::Failed(CommandError::StaleTarget)
                ));
            }
            event_loop.handle_command(event);
            if finished {
                break;
            }
        }
        assert_eq!(
            event_loop.sessions[0].app.status_bar.flash_text(),
            Some(CommandError::StaleTarget.to_string().as_str())
        );
        assert!(Arc::ptr_eq(&config, &actor.effective_config().unwrap()));
        assert!(Arc::ptr_eq(
            &history,
            &event_loop.sessions[0].handles.history.load_full()
        ));
        assert!(actor.pending_queue().is_empty());
    });
}

#[test]
fn live_compact_commands_preserve_guidance_and_busy_order() {
    const FIRST: &str = "retain implementation details";
    const SECOND: &str = "retain test results";
    with_event_loop(|event_loop| {
        select_restored_chat(event_loop);
        event_loop.sessions[0].app.active_chat = 0;
        event_loop.sync_orchestration();
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let _guard = actor.prepare_idle().unwrap();
        assert!(matches!(
            dispatch_slash(event_loop, &format!("/compact {FIRST}")),
            CommandOutcome::Completed
        ));
        assert!(matches!(
            dispatch_slash(event_loop, &format!("/compact {SECOND}")),
            CommandOutcome::Completed
        ));
        let pending = actor.pending_queue();
        assert_eq!(pending.len(), 2);
        let [
            PendingQueueEntry::Compact {
                operation_id: first,
                instructions: Some(first_text),
                ..
            },
            PendingQueueEntry::Compact {
                operation_id: second,
                instructions: Some(second_text),
                ..
            },
        ] = pending.as_slice()
        else {
            panic!("expected two queued compacts");
        };
        assert_eq!(first_text, FIRST);
        assert_eq!(second_text, SECOND);
        assert_ne!(first, second);
        assert!(actor.remove_pending_compact(*second).is_some());
        assert!(
            matches!(actor.pending_queue().as_slice(), [PendingQueueEntry::Compact { instructions: Some(text), .. }] if text == FIRST)
        );
    });
}

#[test_case("/fast"; "fast")]
#[test_case("/workflow"; "workflow")]
#[test_case("/cd /"; "directory")]
fn live_slash_controls_commit_on_the_admitted_actor(command: &str) {
    with_event_loop(|event_loop| {
        select_restored_chat(event_loop);
        event_loop.sessions[0].app.active_chat = 0;
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        if command == "/fast" {
            let config = actor.effective_config().unwrap();
            let mut model = config.model.clone();
            model.supports_fast_override = Some(maki_providers::model::FastSupport::Supported);
            let ticket = actor.reserve_prepared_operation(None).unwrap();
            smol::block_on(
                ticket.commit_when_ready(Some(ConfigChange::Patch(ConfigPatch {
                    model: Some(PreparedModel {
                        model,
                        provider: Arc::clone(&config.settings.provider),
                    }),
                    ..Default::default()
                }))),
            )
            .unwrap();
        }
        event_loop.sync_orchestration();
        let previous = actor.effective_config().unwrap();
        let commits = actor.subscribe_config_commits();
        assert!(matches!(
            dispatch_slash(event_loop, command),
            CommandOutcome::Completed
        ));
        let commit = commits.recv_timeout(SESSION_OP_TIMEOUT).unwrap();
        match command {
            "/fast" => assert_eq!(commit.config.fast, !previous.fast),
            "/workflow" => assert_eq!(commit.config.workflow, !previous.workflow),
            _ => assert_eq!(commit.config.context.cwd(), std::path::Path::new("/")),
        }
    });
}

#[test_case(HostContextRequest::WorkingDirectory; "directory")]
#[test_case(HostContextRequest::FastModeSupported; "fast")]
#[test_case(HostContextRequest::ModelSpecs; "models")]
fn restored_context_queries_reject_missing_admission(request: HostContextRequest) {
    with_event_loop(|event_loop| {
        select_restored_chat(event_loop);
        let (reply, response) = flume::bounded(1);
        event_loop.handle_command(CommandEvent::Host {
            target: event_loop.sessions[0].app.command_target.id(),
            request: HostRequest::Context(request),
            admission: None,
            reply,
        });
        assert!(matches!(
            response.recv().unwrap(),
            Err(CommandError::StaleTarget)
        ));
    });
}

#[test_case(Action::Compact(None); "compact")]
#[test_case(Action::ChangeDirectory("/".into()); "directory")]
#[test_case(Action::ChangeMode("build".into()); "mode")]
#[test_case(Action::ToggleSessionOption { id: "fast" }; "fast")]
#[test_case(Action::ToggleSessionOption { id: "workflow" }; "workflow")]
fn restored_actions_report_closed_target(action: Action) {
    with_event_loop(|event_loop| {
        select_restored_chat(event_loop);
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let config = actor.effective_config().unwrap();
        let asynchronous = matches!(action, Action::ChangeMode(_));
        event_loop.handle_action(0, action);
        if asynchronous {
            let event = event_loop
                .internal_rx
                .recv_timeout(SESSION_OP_TIMEOUT)
                .unwrap();
            event_loop.handle_internal(event);
        }
        assert!(
            event_loop.sessions[0]
                .app
                .status_bar
                .flash_text()
                .unwrap()
                .contains(CLOSED_TARGET_ERR)
        );
        assert!(Arc::ptr_eq(&config, &actor.effective_config().unwrap()));
        assert!(actor.pending_queue().is_empty());
    });
}

#[test_case(false; "agent_turn")]
#[test_case(true; "isolated_turn")]
fn restored_custom_turn_commands_have_no_root_authority(isolated: bool) {
    struct CustomTurn(bool);
    impl maki_commands::CommandBehavior for CustomTurn {
        fn execute(
            &self,
            _: maki_commands::CommandInvocation,
        ) -> maki_commands::CommandFuture<Result<CommandOutcome, CommandError>> {
            let content = PROMPT.into();
            let outcome = if self.0 {
                CommandOutcome::IsolatedTurn(maki_commands::IsolatedTurn { content })
            } else {
                CommandOutcome::AgentTurn(maki_commands::AgentTurn {
                    content,
                    prompt: None,
                })
            };
            Box::pin(async move { Ok(outcome) })
        }
    }
    with_event_loop(|event_loop| {
        select_restored_chat(event_loop);
        let runtime = Arc::clone(&event_loop.ctx.command_runtime);
        let producer = runtime
            .registry
            .create_producer(maki_commands::ProducerPrecedence::Application);
        producer
            .replace(vec![maki_commands::Registration {
                spec: maki_commands::CommandSpec {
                    name: Arc::from("/restored-turn"),
                    aliases: Arc::from([]),
                    arguments: maki_commands::CommandArguments::Raw { required: false },
                    docs: maki_commands::CommandDocs {
                        summary: Arc::from(PROMPT),
                        argument_hint: None,
                    },
                    required_capabilities: maki_commands::TargetCapabilities::NONE,
                },
                behavior: Arc::new(CustomTurn(isolated)),
                argument_completions: Vec::new(),
            }])
            .unwrap();
        let history = event_loop.sessions[0].handles.history.load_full();
        let (manager, _) = event_loop.sessions[0].handles.manager_and_root();
        let actors = manager.snapshot().len();
        runtime.dispatch_input(
            &event_loop.sessions[0].app.command_target,
            "/restored-turn".into(),
        );
        event_loop.handle_command(runtime.recv_timeout_for_test(SESSION_OP_TIMEOUT).unwrap());
        assert_eq!(
            event_loop.sessions[0].app.status_bar.flash_text(),
            Some(CommandError::StaleTarget.to_string().as_str())
        );
        assert!(event_loop.sessions[0].handles.queue.is_empty());
        assert!(Arc::ptr_eq(
            &history,
            &event_loop.sessions[0].handles.history.load_full()
        ));
        assert_eq!(manager.snapshot().len(), actors);
    });
}

#[test_case(true; "restored_child")]
#[test_case(false; "live_child_pinned_across_focus_switch")]
fn registered_lua_mode_set_uses_its_invocation_agent(restored: bool) {
    const SPAWNED: &str = "mode child spawned";
    const DONE: &str = "public mode setter completed";
    with_event_loop(|event_loop| {
        select_restored_chat(event_loop);
        let host = install_expanders(event_loop);
        host.event_handle()
            .install_orchestration_services(event_loop.orchestration.clone());
        host.load_source(
            "public_mode_command",
            &format!(
                r#"
            maki.api.register_command({{
                name = "/spawn-mode-child", tui_only = false,
                handler = function(_, ctx)
                    assert(maki.agent.spawn(ctx, {{ inherit_provider = true, silent = true }}))
                    maki.ui.flash("{SPAWNED}")
                end,
            }})
            maki.api.register_command({{
                name = "/set-public-mode", tui_only = false,
                handler = function()
                    if not {live} then
                        local focused = maki.ui.focused_agent()
                        assert(focused == nil)
                    end
                    local ok, err = maki.api.mode.set("plan")
                    assert((ok ~= nil) == {live})
                    if not ok then assert(type(err) == "string") end
                    maki.ui.flash("{DONE}")
                end,
            }})
        "#,
                live = !restored
            ),
        )
        .unwrap();
        let command_runtime = Arc::clone(&event_loop.ctx.command_runtime);
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        let root_actor = manager.actor(root).unwrap();
        let original = root_actor.effective_config().unwrap();
        let child = if restored {
            None
        } else {
            event_loop.sessions[0].app.active_chat = 0;
            event_loop.sync_orchestration();
            smol::block_on(command_runtime.registry.dispatch_input(
                &event_loop.sessions[0].app.command_target,
                "/spawn-mode-child".into(),
            ));
            assert!(
                matches!(host.ui_action_rx().recv_timeout(SESSION_OP_TIMEOUT).unwrap(), UiAction::Flash(text) if text == SPAWNED)
            );
            let child = manager
                .snapshot()
                .into_iter()
                .find(|node| node.parent_id == Some(root))
                .unwrap()
                .agent_id;
            select_child_chat(event_loop, child, RESTORED_CHAT);
            Some(child)
        };
        let registry = command_runtime.registry.clone();
        let target = event_loop.sessions[0].app.command_target.clone();
        let command = smol::spawn(async move {
            registry
                .dispatch_input(&target, "/set-public-mode".into())
                .await
        });
        if let Some(child) = child {
            let action = host
                .ui_action_rx()
                .recv_timeout(SESSION_OP_TIMEOUT)
                .unwrap();
            assert!(
                matches!(&action, UiAction::SetAgentMode { target, .. } if target.id() == child)
            );
            event_loop.sessions[0].app.active_chat = 0;
            event_loop.sync_orchestration();
            event_loop.handle_ui_action(action);
        }
        assert!(
            matches!(host.ui_action_rx().recv_timeout(SESSION_OP_TIMEOUT).unwrap(), UiAction::Flash(text) if text == DONE)
        );
        smol::block_on(command);
        assert!(Arc::ptr_eq(
            &original,
            &root_actor.effective_config().unwrap()
        ));
        if let Some(child) = child {
            assert!(
                manager
                    .actor(child)
                    .unwrap()
                    .effective_config()
                    .unwrap()
                    .mode
                    .plan_path()
                    .is_some()
            );
        }
    });
}

#[test_case("   "; "blank")]
#[test_case("@reject:task"; "expander_failure")]
#[test_case("oversized"; "oversized")]
fn rejected_new_prompt_never_publishes_a_runtime(prompt: &str) {
    with_event_loop(|event_loop| {
        let _host = install_expanders(event_loop);
        let original = event_loop.sessions[0].id();
        let prompt = if prompt == "oversized" {
            "x".repeat(OVERSIZED_PROMPT_BYTES)
        } else {
            prompt.into()
        };
        let expected = match prompt.as_str() {
            "   " => crate::app::EMPTY_PROMPT_ERR.to_owned(),
            "@reject:task" => EXPANDER_FAILURE.to_owned(),
            _ => maki_agent::actor::ActorError::InstructionOverflow.to_string(),
        };
        let (reply, response) = flume::bounded(1);
        event_loop.handle_session_request(
            SessionRequest::New {
                prompt: Some(prompt),
                focus: true,
            },
            reply,
        );
        let error = response
            .recv_timeout(SESSION_OP_TIMEOUT)
            .unwrap()
            .unwrap_err();
        assert_eq!(error, expected);
        assert_eq!(event_loop.sessions.len(), 1);
        assert_eq!(event_loop.sessions[event_loop.focused].id(), original);
    });
}

#[test]
fn session_prompt_expands_once_and_busy_submission_is_queued() {
    with_event_loop(|event_loop| {
        let _host = install_expanders(event_loop);
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let _guard = actor.prepare_idle().unwrap();
        assert_eq!(
            event_loop.submit_text(0, "@ok:task".into()).unwrap(),
            serde_json::json!("started")
        );
        assert_eq!(actor.pending_submissions()[0].text, EXPANDED_PROMPT);
        assert_eq!(
            event_loop.submit_text(0, PROMPT.into()).unwrap(),
            serde_json::json!("queued")
        );
        assert_eq!(actor.pending_submissions().len(), 2);
    });
}

#[test_case(false; "expander_failure")]
#[test_case(true; "admission_failure")]
fn rejected_session_prompt_preserves_shell_context(oversized: bool) {
    with_event_loop(|event_loop| {
        let _host = install_expanders(event_loop);
        let id = event_loop.sessions[0].app.shell.reserve_for(None);
        event_loop.sessions[0]
            .app
            .handle_shell_event(ShellEvent::Done {
                id,
                command: PROMPT.into(),
                output: SHELL_OUTPUT.into(),
                is_error: false,
                visible: true,
            });
        let prompt = if oversized {
            "x".repeat(OVERSIZED_PROMPT_BYTES)
        } else {
            "@reject:task".into()
        };
        let error = event_loop.submit_text(0, prompt).unwrap_err();
        if !oversized {
            assert_eq!(error, EXPANDER_FAILURE);
        }
        assert!(
            !event_loop.sessions[0]
                .app
                .shell
                .results_for(None)
                .is_empty()
        );
        assert!(event_loop.sessions[0].handles.queue.is_empty());
    });
}

#[test]
fn idle_session_prompt_starts_on_root_and_preserves_shell_preamble() {
    with_event_loop(|event_loop| {
        select_restored_chat(event_loop);
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let guard = actor.prepare_idle().unwrap();
        let id = event_loop.sessions[0].app.shell.reserve_for(None);
        event_loop.sessions[0]
            .app
            .handle_shell_event(ShellEvent::Done {
                id,
                command: PROMPT.into(),
                output: SHELL_OUTPUT.into(),
                is_error: false,
                visible: true,
            });
        let (reply, response) = flume::bounded(1);
        event_loop.handle_session_request(
            SessionRequest::Prompt {
                id: None,
                text: PROMPT.into(),
            },
            reply,
        );
        assert_eq!(
            response.recv().unwrap().unwrap(),
            serde_json::json!("started")
        );
        let pending = actor.pending_submissions();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].text, PROMPT);
        assert!(
            event_loop.sessions[0]
                .app
                .shell
                .results_for(None)
                .is_empty()
        );
        assert_eq!(event_loop.sessions[0].app.active_chat, 1);
        drop(guard);
        loop {
            let envelope = event_loop.sessions[0]
                .handles
                .agent_rx
                .recv_timeout(SESSION_OP_TIMEOUT)
                .unwrap();
            event_loop.handle_agent(0, Box::new(envelope));
            if event_loop.sessions[0]
                .handles
                .history
                .load()
                .messages
                .iter()
                .any(|message| {
                    message.content.iter().any(|block| {
                        matches!(block, maki_providers::ContentBlock::Text { text } if text.contains(SHELL_OUTPUT))
                    })
                })
            {
                break;
            }
        }
    });
}
