//! Errors the Lua agent API reports, shared with tests that assert them.

pub const INVOCATION_EXPIRED: &str = "context invocation expired";
pub const NO_PLUGIN_AUTHORITY: &str = "no active plugin authority";
pub const NO_TASK_AUTHORITY: &str = "no active task authority";
pub const MANAGED_AUTHORITY_INACTIVE: &str =
    "managed agent authority is not active in this invocation";
pub const OTHER_RUNTIME: &str = "agent belongs to another runtime";
pub const NO_ORCHESTRATION_SERVICES: &str = "no host orchestration services";
pub const ORCHESTRATION_SERVICES_EXPIRED: &str = "host orchestration services expired";
pub const TRUSTED_SUBSCRIPTION_REQUIRED: &str = "agent subscriptions require trusted authority";
pub const TRUSTED_RUN_COMMAND_REQUIRED: &str = "run_command requires trusted authority";
pub const TRUSTED_TIMER_REQUIRED: &str = "timers require trusted authority";
pub const DELEGATED_REGISTRATION: &str = "delegated tool handlers cannot register host callbacks";
pub const DELEGATED_FOCUS: &str = "delegated tool handlers cannot read UI focus";
pub const NO_FOCUSED_AGENT: &str = "no agent has UI focus";
pub const TURN_WAIT_TIMEOUT: &str = "agent turn wait timed out";
pub const TRUSTED_MODEL_MUTATION_REQUIRED: &str =
    "host model mutation requires trusted handler authority";
pub const RESTRICTED_MODE_CHANGE: &str = "restricted agents cannot change mode";
