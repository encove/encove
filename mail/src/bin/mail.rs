use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use encove_mail::{Account, Sync, import_mbox};
use eyre::{Context, Report};
use indicatif::{ProgressBar, ProgressStyle};
use jiff::{ToSpan, Zoned};
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
            server,
            days,
            database,
        } => {
            let account = server.account();
            let store = Store::new(database);
            let mut sync = Sync::connect(&account, &store).await?;
            let since = Zoned::now().date().saturating_sub(days.days());
            let report = sync.sync(since).await?;
            eprintln!("Synchronized the last {days} days: {report}");
            Ok(())
        }
        Command::Import {
            input,
            server,
            database,
        } => {
            let file = File::open(&input).context(format!("failed to open {}", input.display()))?;
            let progress = ProgressBar::new(file.metadata()?.len()).with_style(
                ProgressStyle::with_template(
                    "{wide_bar} {binary_bytes}/{binary_total_bytes} ({binary_bytes_per_sec}, {eta})",
                )
                .expect("progress bar template is valid"),
            );

            let store = Store::new(database);
            let mbox = progress.wrap_read(BufReader::with_capacity(READ_BUFFER_SIZE, file));
            let report = import_mbox(&store, mbox)?;
            progress.finish_and_clear();
            eprintln!("Imported {}: {report}", input.display());

            let account = server.account();
            let mut sync = Sync::connect(&account, &store).await?;
            let report = sync.link().await?;
            eprintln!("Linked stored messages to their mailboxes: {report}");
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
        #[command(flatten)]
        server: Server,
        /// How many days of mail to keep in the database
        #[arg(long, default_value_t = 7)]
        days: i64,
        /// Path to the database, which is created if it doesn't exist
        #[arg(long, default_value = "encove.redb")]
        database: PathBuf,
    },
    /// Imports the messages from an mbox file into the database, then links them to the mailboxes
    /// containing them on the IMAP server
    Import {
        /// The mbox file to import
        input: PathBuf,
        #[command(flatten)]
        server: Server,
        /// Path to the database, which is created if it doesn't exist
        #[arg(long, default_value = "encove.redb")]
        database: PathBuf,
    },
}

/// The IMAP server and the credentials to log in with
#[derive(Args)]
struct Server {
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
}

impl Server {
    fn account(self) -> Account {
        let Self {
            user,
            token,
            host,
            port,
        } = self;

        Account {
            host,
            port,
            user,
            token,
        }
    }
}

/// The size of the buffer for reading mbox files
const READ_BUFFER_SIZE: usize = 1 << 20;
