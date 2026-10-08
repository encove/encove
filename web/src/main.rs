use core::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use encove_web::serve;
use eyre::Report;
use store::Store;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Report> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let Cli { listen, database } = Cli::parse();
    Ok(serve(Store::new(database), listen).await?)
}

/// An alternative HTML frontend for Gmail
#[derive(Parser)]
#[command(version)]
struct Cli {
    /// Address to listen on
    #[arg(long, default_value = "127.0.0.1:8000")]
    listen: SocketAddr,
    /// Path to the database written by `mail sync`
    #[arg(long, default_value = "encove.redb")]
    database: PathBuf,
}
