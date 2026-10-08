//! Obtains an access token through Google's OAuth 2.0 flow for installed applications
//!
//! The flow follows <https://developers.google.com/identity/protocols/oauth2/native-app>: the user
//! opens an authorization URL in their browser, and Google redirects back to a temporary server on
//! the loopback interface with an authorization code, which is then exchanged for an access token.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::{fs, io};

use axum::Router;
use axum::extract::{Query, State};
use axum::response::Html;
use axum::routing::get;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use clap::Parser;
use eyre::{Context, Report};
use reqwest::Url;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

#[tokio::main]
async fn main() -> Result<(), Report> {
    let Cli { credentials } = Cli::parse();
    let client = OAuthClient::from_credentials(&credentials)?;
    let TokenResponse { access_token } = client.fetch().await?;
    println!("{access_token}");
    Ok(())
}

/// Gets an access token by authorizing encove in the browser
#[derive(Parser)]
#[command(version)]
struct Cli {
    /// Path to a file containing OAuth credentials
    credentials: PathBuf,
}

/// The credentials of an OAuth client registered in the Google Cloud console
struct OAuthClient {
    /// The client ID
    id: String,
    /// The client secret
    secret: String,
}

impl OAuthClient {
    fn from_credentials(file: &Path) -> Result<Self, Report> {
        let data = fs::read_to_string(file)
            .context(format!("failed to read from file {}", file.display()))?;
        let credentials = serde_json::from_str::<AppCredentials>(&data)
            .context("failed to parse credentials")?
            .installed;
        Ok(Self {
            id: credentials.client_id,
            secret: credentials.client_secret,
        })
    }

    /// Runs the authorization flow and returns the resulting token
    ///
    /// Instructions for the user are printed to stderr.
    ///
    /// # Errors
    ///
    /// Returns an error if the user denies access, the callback is invalid, or a request fails.
    async fn fetch(self) -> Result<TokenResponse, Error> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let redirect_uri = format!("http://127.0.0.1:{}", listener.local_addr()?.port());

        let verifier = random_string::<32>()?;
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        let state = random_string::<16>()?;

        let url = Url::parse_with_params(
            AUTHORIZATION_URL,
            [
                ("client_id", self.id.as_str()),
                ("redirect_uri", &redirect_uri),
                ("response_type", "code"),
                ("scope", SCOPE),
                ("code_challenge", &challenge),
                ("code_challenge_method", "S256"),
                ("state", &state),
            ],
        )
        .expect("authorization URL is valid");

        eprintln!("Open this URL in your browser to give encove access to Gmail over IMAP:\n");
        eprintln!("{url}\n");
        eprintln!("Waiting for authorization...");

        let (sender, receiver) = oneshot::channel();
        let (stop, stopped) = oneshot::channel::<()>();
        let app = Router::new()
            .route("/", get(callback))
            .with_state(Arc::new(Mutex::new(Some(sender))));

        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = stopped.await;
                })
                .await
        });

        let Callback {
            code,
            state: received_state,
            error,
        } = receiver.await.map_err(|_| Error::MissingCode)?;
        let _ = stop.send(());
        if let Ok(Err(error)) = server.await {
            return Err(Error::Io(error));
        }

        if let Some(error) = error {
            return Err(Error::Denied(error));
        }

        if received_state.as_deref() != Some(state.as_str()) {
            return Err(Error::StateMismatch);
        }

        let Some(code) = code else {
            return Err(Error::MissingCode);
        };

        let response = reqwest::Client::new()
            .post(TOKEN_URL)
            .form(&[
                ("code", code.as_str()),
                ("client_id", self.id.as_str()),
                ("client_secret", self.secret.as_str()),
                ("redirect_uri", &redirect_uri),
                ("grant_type", "authorization_code"),
                ("code_verifier", &verifier),
            ])
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(Error::Exchange(format!("{status}: {body}")));
        }

        Ok(response.json().await?)
    }
}

async fn callback(
    State(sender): State<Arc<Mutex<Option<oneshot::Sender<Callback>>>>>,
    Query(callback): Query<Callback>,
) -> Html<String> {
    let (title, message) = match callback.error.is_some() {
        true => (
            "Authorization failed",
            "Access was not granted. You can close this tab.",
        ),
        false => (
            "Authorization received",
            "You can close this tab and return to the terminal.",
        ),
    };

    if let Some(sender) = sender.lock().unwrap().take() {
        let _ = sender.send(callback);
    }

    Html(format!(
        "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
         <meta name=\"referrer\" content=\"no-referrer\"><title>{title} – Encove</title></head>\
         <body><main><h1>{title}</h1><p>{message}</p></main></body></html>"
    ))
}

/// An error that occurred while obtaining a token
#[derive(Debug, thiserror::Error)]
enum Error {
    /// The user denied access, or Google reported another error
    #[error("authorization failed: {0}")]
    Denied(String),
    /// The callback's state did not match the one in the authorization request
    #[error("authorization callback has an unexpected state parameter")]
    StateMismatch,
    /// The callback did not include an authorization code
    #[error("authorization callback has no authorization code")]
    MissingCode,
    /// The token endpoint rejected the authorization code
    #[error("failed to exchange authorization code: {0}")]
    Exchange(String),
    /// A request to Google failed
    #[error("request failed: {0}")]
    Http(#[from] reqwest::Error),
    /// The local callback server failed
    #[error("callback server failed: {0}")]
    Io(#[from] io::Error),
    /// Random bytes could not be generated
    #[error("failed to generate random bytes: {0}")]
    Random(getrandom::Error),
}

#[derive(Deserialize)]
struct Callback {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

/// A successful response from the token endpoint
#[derive(Deserialize)]
struct TokenResponse {
    /// The access token to authenticate to IMAP with
    access_token: String,
}

#[derive(Deserialize)]
struct AppCredentials {
    installed: Installed,
}

#[derive(Deserialize)]
struct Installed {
    client_id: String,
    client_secret: String,
}

fn random_string<const N: usize>() -> Result<String, Error> {
    let mut bytes = [0; N];
    getrandom::fill(&mut bytes).map_err(Error::Random)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

const AUTHORIZATION_URL: &str = "https://accounts.google.com/o/oauth2/v2/auth";
const TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
/// The scope Gmail requires for IMAP, which grants full access; encove itself only reads
const SCOPE: &str = "https://mail.google.com/";
