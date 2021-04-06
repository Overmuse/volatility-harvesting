use config::{Config, ConfigError, Environment};
use kafka_settings::KafkaSettings;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AppSettings {
    pub initial_equity: f64,
    pub internal_leverage: f64,
    pub batch_seconds: u64,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub app: AppSettings,
    pub kafka: KafkaSettings,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let mut s = Config::new();
        s.merge(Environment::new().separator("__"))?;
        s.try_into()
    }
}
