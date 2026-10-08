use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use encove_mail::{Account, Sync, import_mbox};
use eyre::{Context, Report};
use indicatif::{ProgressBar, ProgressStyle};
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

    let Cli { command } = Cli::parse();
    match command {
        Command::Sync {
            user,
            token,
            host,
            port,
            days,
            database,
        } => {
            let account = Account {
                host,
                port,
                user,
                token,
            };

            let store = Store::new(database);
            let mut sync = Sync::connect(&account, &store, days).await?;
            let report = sync.sync().await?;
            eprintln!("Synchronized the last {days} days: {report}");
            Ok(())
        }
        Command::Import { input, database } => {
            let file = File::open(&input).context(format!("failed to open {}", input.display()))?;
            let progress = ProgressBar::new(file.metadata()?.len()).with_style(
                ProgressStyle::with_template(
                    "{wide_bar} {binary_bytes}/{binary_total_bytes} ({binary_bytes_per_sec}, {eta})",
                )
                .expect("progress bar template is valid"),
            );

            let mbox = progress.wrap_read(BufReader::with_capacity(READ_BUFFER_SIZE, file));
            let report = import_mbox(&Store::new(database), mbox)?;
            progress.finish_and_clear();
            eprintln!("Imported {}: {report}", input.display());
            Ok(())
        }
    }
}

/// Manages the local mail store
#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Copies recent mail from the IMAP server into the database
    Sync {
        /// The user to log in as, usually the email address
        #[arg(long)]
        user: String,
        /// OAuth access token with the `https://mail.google.com/` scope, as printed by the `token`
        /// command
        #[arg(long)]
        token: String,
        /// The IMAP server, which must support implicit TLS
        #[arg(long, default_value = "imap.gmail.com")]
        host: String,
        /// The port of the IMAP server
        #[arg(long, default_value_t = 993)]
        port: u16,
        /// How many days of mail to keep in the database
        #[arg(long, default_value_t = 7)]
        days: i64,
        /// Path to the database, which is created if it doesn't exist
        #[arg(long, default_value = "encove.redb")]
        database: PathBuf,
    },
    /// Imports the messages from an mbox file into the database
    Import {
        /// The mbox file to import
        input: PathBuf,
        /// Path to the database, which is created if it doesn't exist
        #[arg(long, default_value = "encove.redb")]
        database: PathBuf,
    },
}

/// The size of the buffer for reading mbox files
const READ_BUFFER_SIZE: usize = 1 << 20;
