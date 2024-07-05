use std::sync::LazyLock;

use clap::Parser;
use serde::{Deserialize, Serialize};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub(crate) struct App {
    /// 配置文件路径 e.g. config.toml
    #[arg(short, long, default_value = "config.toml")]
    pub cfg_file_path: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub(crate) struct AppConfig {
    pub src_url: String,
    pub src_index: String,
    pub dest_url: String,
    pub dest_index: String,
    pub worker_count: u32,
    pub size_per_page: u32,
    pub bulk_size: u32,
}

#[cfg(not(test))]
pub(crate) static APP: LazyLock<App> = LazyLock::new(App::parse);

#[cfg(test)]
pub(crate) static APP: LazyLock<App> = LazyLock::new(|| App {
    cfg_file_path: "config.toml".to_string(),
});

pub(crate) static APP_CONFIG: LazyLock<AppConfig> = LazyLock::new(|| {
    toml::from_str::<AppConfig>(&std::fs::read_to_string(&APP.cfg_file_path).unwrap()).unwrap()
});
