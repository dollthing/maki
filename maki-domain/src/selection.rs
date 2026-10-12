use crate::ThinkingConfig;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::sync::Arc;
use thiserror::Error;

pub const MODEL_SPEC_KEY: &str = "spec";
pub const MODEL_THINKING_KEY: &str = "thinking";
pub const MODEL_FAST_KEY: &str = "fast";
pub const MODEL_OPTION_KEYS: [&str; 2] = [MODEL_THINKING_KEY, MODEL_FAST_KEY];
pub const FAST_VALUES: [&str; 2] = ["true", "false"];

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelSelection {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<Arc<str>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "optional_thinking"
    )]
    pub thinking: Option<ThinkingConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fast: Option<bool>,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum ModelSelectionError {
    #[error("duplicate model option: {0}")]
    Duplicate(String),
    #[error("unknown model option: {0}")]
    Unknown(String),
    #[error("model spec must not be empty")]
    EmptySpec,
    #[error("expected at most one model spec")]
    MultipleSpecs,
    #[error("fast must be true or false")]
    InvalidFast,
    #[error("{0}")]
    InvalidThinking(String),
}

impl ModelSelection {
    pub fn set_spec(&mut self, spec: impl Into<Arc<str>>) -> Result<(), ModelSelectionError> {
        let spec = spec.into();
        if spec.is_empty() {
            return Err(ModelSelectionError::EmptySpec);
        }
        if self.spec.is_some() {
            return Err(ModelSelectionError::MultipleSpecs);
        }
        self.spec = Some(spec);
        Ok(())
    }

    pub fn set_option(&mut self, key: &str, value: &str) -> Result<(), ModelSelectionError> {
        match key {
            MODEL_THINKING_KEY if self.thinking.is_none() => {
                self.thinking = Some(parse_thinking(value)?);
            }
            MODEL_FAST_KEY if self.fast.is_none() => {
                self.fast = Some(parse_fast(value)?);
            }
            MODEL_THINKING_KEY | MODEL_FAST_KEY => {
                return Err(ModelSelectionError::Duplicate(key.into()));
            }
            _ => return Err(ModelSelectionError::Unknown(key.into())),
        }
        Ok(())
    }
}

pub fn parse_thinking(value: &str) -> Result<ThinkingConfig, ModelSelectionError> {
    ThinkingConfig::parse_setting(value)
        .map_err(|error| ModelSelectionError::InvalidThinking(error.to_string()))
}

pub fn parse_fast(value: &str) -> Result<bool, ModelSelectionError> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(ModelSelectionError::InvalidFast),
    }
}

pub mod thinking {
    use super::{Deserialize, Deserializer, Serializer, ThinkingConfig, de, parse_thinking};

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Value {
        Setting(String),
        Budget(u32),
    }

    pub fn serialize<S: Serializer>(
        value: &ThinkingConfig,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            ThinkingConfig::Budget(tokens) => serializer.serialize_u32(*tokens),
            value => serializer.serialize_str(&value.to_string()),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<ThinkingConfig, D::Error> {
        let value = Value::deserialize(deserializer)?;
        let setting = match value {
            Value::Setting(value) => value,
            Value::Budget(tokens) => tokens.to_string(),
        };
        parse_thinking(&setting).map_err(de::Error::custom)
    }
}

mod optional_thinking {
    use super::{Deserializer, Serializer, ThinkingConfig, thinking};

    pub fn serialize<S: Serializer>(
        value: &Option<ThinkingConfig>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(value) => thinking::serialize(value, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<ThinkingConfig>, D::Error> {
        thinking::deserialize(deserializer).map(Some)
    }
}
