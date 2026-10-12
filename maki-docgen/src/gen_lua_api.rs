use maki_lua::docs_render;

const FRONTMATTER: &str = r#"+++
title = "Lua API"
weight = 10
[extra]
group = "Reference"
+++

"#;

pub fn generate() -> String {
    format!("{FRONTMATTER}{}", docs_render::site_page())
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    #[test_case("maki.api.register_command()", &[
        "`handler(opts, ctx)`",
        "trusted invocation-scoped LuaCtx",
        "pinned agent",
        "Existing one-argument handlers still work.",
    ]; "command_context")]
    #[test_case("maki.keymap.set()", &[
        "`rhs(nil, ctx)`",
        "trusted invocation-scoped LuaCtx",
        "`maki.agent.current(ctx)`",
        "Retaining the context does not extend its authority beyond the callback.",
    ]; "keymap_context")]
    #[test_case("maki.api.run_command()", &[
        "Delegated tool handlers cannot call this trusted command executor.",
        "Nested trusted command calls retain the invoking command's pinned agent target.",
    ]; "nested_command_authority")]
    #[test_case("Agent:status()", &["active_turn", "cumulative integers", "closed snapshot", "Host roots expose only id"]; "agent_status")]
    #[test_case("Agent:model()", &["{spec, thinking, fast}", "Descriptive model metadata is not included."]; "agent_selection")]
    #[test_case("Agent:available_models()", &["Array of available model spec strings."]; "model_discovery")]
    #[test_case("maki.agent.spawn()", &["model_spec", "local_tools", "output_schema", "auto_deliver", "inherit_provider"]; "spawn_options")]
    #[test_case("Agent:transcript()", &["transcript_unavailable", "omitted_tool_groups", "compacted", "max_bytes"]; "transcript_fields")]
    #[test_case("Agent:prompt()", &["kind = \"timeout\"", "exact ticket remains usable", "Timeout does not cancel execution."]; "prompt_ticket")]
    #[test_case("Agent:on_turn_start()", &["handler(payload, ctx)", "source_turn?", "plugin_generation?", "selection"]; "start_event")]
    #[test_case("Agent:on_turn_end()", &["same fields as AgentTurn:result and AgentTurn:wait"]; "end_event")]
    #[test_case("Agent:on_idle()", &["handler({agent_id}, ctx)", "not a reservation"]; "idle_event")]
    #[test_case("Agent:on_config_change()", &["generation", "selection", "mode (registered name)"]; "config_event")]
    #[test_case("Agent:on_close()", &["lifecycle is \"closed\" or \"shutdown\""]; "close_event")]
    #[test_case("LuaCtx:agent()", &["pinned invocation subject", "maki.agent.current(ctx)"]; "context_agent")]
    #[test_case("LuaCtx:model()", &["admitted selection", "{spec, thinking, fast}", "unsupported-capability error"]; "context_model")]
    #[test_case("LuaCtx:defer_close()", &["including cancellation", "host-held cleanup", "Host roots do not support"]; "context_cleanup")]
    #[test_case("Session:status()", &["cumulative session counters", "retryable", "Authority failure"]; "session_status")]
    fn generated_docs_contain_callback_authority_contract(signature: &str, required: &[&str]) {
        let page = generate();
        let heading = format!("### `{signature}`");
        let start = page.find(&heading).expect("API section must be rendered");
        let end = page[start..]
            .find("\n---\n")
            .map(|offset| start + offset)
            .expect("API section must have a separator");
        let rendered = page[start..end]
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        for text in required {
            assert!(
                rendered.contains(text),
                "rendered {signature} docs must contain {text:?}"
            );
        }
    }

    #[test_case("maki.agent.AgentTurn", &["lookup eviction", "cancellation_reason", "user_message", "retryable", "per-turn counters", "last assistant message", "duration_ms", "immutable elapsed milliseconds for this turn"]; "ticket_result_fields")]
    #[test_case("maki.agent.AgentSubscription", &["Garbage collection does not unsubscribe", "Close explicitly"]; "subscription_lifetime")]
    fn generated_class_docs_contain_lifetime_and_result_contract(name: &str, required: &[&str]) {
        let module = maki_lua::docs::api_docs()
            .into_iter()
            .find(|module| module.name == name)
            .expect("class metadata must be registered");
        let page = generate();
        let description = module.desc.split_whitespace().collect::<Vec<_>>().join(" ");
        let rendered = page.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(
            rendered.contains(&description),
            "class description must be rendered"
        );
        for text in required {
            assert!(
                description.contains(text),
                "{name} description must contain {text:?}"
            );
        }
    }

    const MUTABLE_PATH_CONTRACT: &str = "same-path mutation is already in progress";
    const SERIALIZATION_CONTRACT: &str = "same-process per-path mutation serialization";
    #[test]
    fn generated_docs_contain_mutable_path_reentry_contract() {
        let page = generate();
        assert!(
            page.contains(SERIALIZATION_CONTRACT),
            "generated mutable_path docs must state the serialization contract"
        );
        assert!(
            page.contains(MUTABLE_PATH_CONTRACT) && page.contains("unsupported"),
            "generated mutable_path docs must state reentry is unsupported and name its error"
        );
    }

    #[test]
    fn generated_docs_contain_typed_arguments_contract() {
        let page = generate();
        let start = page
            .find("### `maki.api.register_command()`")
            .expect("generated docs must render register_command");
        let end = page[start..]
            .find("\n---\n")
            .map(|offset| start + offset)
            .expect("register_command section must have a separator");
        let section = &page[start..end];
        let rendered = section.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(
            !section.contains("| Completion is declared"),
            "completion prose must remain part of the arguments parameter"
        );
        let required = [
            "A required scalar consumes exactly one value.",
            "An optional scalar consumes zero or one value.",
            "A required variadic consumes one or more values.",
            "An optional variadic consumes zero or more values.",
            "The command parser outer-trims the argument remainder",
            "Unicode whitespace separates tokens outside quotes.",
            "adjacent quoted and unquoted fragments concatenate",
            "Backslashes are literal outside quotes and inside single quotes.",
            r#"only `\\"` and `\\\\` decode to a"#,
            "the outer-trimmed original argument remainder",
            "Raw commands do not set `opts.fargs` or `opts.values`.",
            "A missing optional scalar is `nil`.",
            "A missing optional variadic is an empty array.",
            "The inclusive exact range is `-9007199254740991` to",
            "Defaults are enum choices in the core",
            "`completion = false`,",
            "may set `mode` to `\"replace\"` or",
            "`\"extend\"` (`\"replace\"` is the default).",
            "Provider callbacks receive the typed argument name",
            "Raw commands do not have argument completion providers.",
            "Documentation and test example. It is not bundled and never copies files.",
            "name = \"/copy\"",
            "choices = { \"skip\", \"overwrite\" }",
            "records decoded values.",
        ];
        for text in required {
            assert!(
                rendered.contains(text),
                "rendered register_command docs must contain {text:?}"
            );
        }
        let order = [
            "A required scalar consumes exactly one value.",
            "The command parser outer-trims the argument remainder",
            "the outer-trimmed original argument remainder",
            "Defaults are enum choices in the core",
            "Documentation and test example. It is not bundled and never copies files.",
        ];
        let mut previous = 0;
        for text in order {
            let position = rendered
                .find(text)
                .unwrap_or_else(|| panic!("missing ordered rendered text {text:?}"));
            assert!(
                position >= previous,
                "rendered register_command text {text:?} is out of order"
            );
            previous = position;
        }
    }
}
