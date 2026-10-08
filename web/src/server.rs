//! The HTTP server that renders the mailbox from the local store

use core::net::SocketAddr;
use std::io;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::get;
use mail::{MailboxName, MessageContents, ThreadMessage};
use serde::Deserialize;
use store::Store;
use tokio::net::TcpListener;
use tokio::task::block_in_place;

use crate::html::{Encode, View};
use crate::model::{
    LoadedMessage, Location, MailIndex, Mailbox, Notice, RemoteImages, Thread, ThreadId,
};

/// Serves the mailbox on the given address until the process is stopped
///
/// # Errors
///
/// Returns an error if the address cannot be bound or the server fails.
pub async fn serve(store: Store, address: SocketAddr) -> io::Result<()> {
    let listener = TcpListener::bind(address).await?;
    tracing::info!("listening on http://{}", listener.local_addr()?);
    axum::serve(listener, router(store)).await
}

fn router(store: Store) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/label/{label}", get(label))
        .route("/label/{label}/thread/{thread}", get(thread))
        .route("/style.css", get(stylesheet))
        .with_state(Arc::new(AppState { store }))
}

async fn index() -> Redirect {
    Redirect::to(&Location::new(MailboxName::new(MailboxName::INBOX)).href())
}

async fn label(
    State(state): State<Arc<AppState>>,
    Path(label): Path<MailboxName>,
    Query(params): Query<LocationParams>,
) -> Result<Html<String>, AppError> {
    let LocationParams { page, images } = params;
    state.mailbox(Location {
        label,
        page,
        thread: None,
        images,
    })
}

async fn thread(
    State(state): State<Arc<AppState>>,
    Path((label, thread)): Path<(MailboxName, ThreadId)>,
    Query(params): Query<LocationParams>,
) -> Result<Html<String>, AppError> {
    let LocationParams { page, images } = params;
    state.mailbox(Location {
        label,
        page,
        thread: Some(thread),
        images,
    })
}

#[derive(Deserialize)]
struct LocationParams {
    #[serde(default)]
    page: usize,
    #[serde(default)]
    images: RemoteImages,
}

struct AppState {
    store: Store,
}

impl AppState {
    fn mailbox(&self, location: Location) -> Result<Html<String>, AppError> {
        let mailbox = block_in_place(|| self.load(location))?;
        Ok(Html(mailbox.view().to_string()))
    }

    fn load(&self, location: Location) -> Result<Mailbox, AppError> {
        let reader = self.store.reader()?;
        let index = MailIndex::new(&reader)?;

        let thread = match location.thread {
            Some(id) => {
                let Some(messages) = index.thread_messages(id) else {
                    return Err(AppError::ThreadNotFound);
                };

                let contents = reader.table::<MessageContents>()?;
                let mut loaded = Vec::new();
                for ThreadMessage {
                    key,
                    metadata,
                    read,
                } in messages
                {
                    let Some(content) = contents.get(key)? else {
                        continue;
                    };
                    loaded.push(LoadedMessage {
                        metadata,
                        content: content.value(),
                        read,
                    });
                }

                Some(Thread::new(loaded, location.images))
            }
            None => None,
        };

        let labels = index.labels(&location.label);
        let threads = index.thread_list(&location, labels.selected_name());
        Ok(Mailbox {
            location,
            labels,
            threads,
            thread,
        })
    }
}

async fn stylesheet() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        include_str!("style.css"),
    )
}

enum AppError {
    Store(store::StoreError),
    ThreadNotFound,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, notice) = match self {
            Self::Store(store::StoreError::NotSynced) => (
                StatusCode::SERVICE_UNAVAILABLE,
                Notice {
                    title: "No mail yet".to_owned(),
                    message: "Run `mail sync` to download the last week of mail, then \
                        reload this page."
                        .to_owned(),
                },
            ),
            Self::Store(store::StoreError::Busy) => (
                StatusCode::SERVICE_UNAVAILABLE,
                Notice {
                    title: "Mail is being synchronized".to_owned(),
                    message: "The database is in use by `mail sync`. Reload this page in a \
                        moment."
                        .to_owned(),
                },
            ),
            Self::Store(error @ store::StoreError::Database(_)) => {
                tracing::error!(%error, "failed to read the store");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Notice {
                        title: "Database error".to_owned(),
                        message: error.to_string(),
                    },
                )
            }
            Self::ThreadNotFound => (
                StatusCode::NOT_FOUND,
                Notice {
                    title: "Conversation not found".to_owned(),
                    message: "It may have been removed from the server, or be older than a week."
                        .to_owned(),
                },
            ),
        };

        (status, Html(notice.view().to_string())).into_response()
    }
}

impl From<store::StoreError> for AppError {
    fn from(error: store::StoreError) -> Self {
        Self::Store(error)
    }
}

#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use mail::fixtures;
    use tower::ServiceExt;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn serves_mailboxes() {
        let (directory, store) = fixtures::store();

        let missing = Store::new(directory.path().join("missing.redb"));
        let (status, body) = get(&missing, "/label/INBOX").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.contains("No mail yet"));

        let (status, body) = get(&store, "/").await;
        assert_eq!(status, StatusCode::SEE_OTHER);
        assert!(body.is_empty());

        let (status, body) = get(&store, "/label/%5BGmail%5D%2FSent%20Mail").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("<h2>Sent Mail</h2>"));
        assert!(body.contains("<span class=\"subject\">Plans for Thursday</span>"));
        assert!(body.contains("Alice Example, Dirkjan"));

        let (status, body) = get(&store, "/label/INBOX/thread/1?images=shown").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("https://tracker.example/open.gif"));
        assert!(body.contains("Café at 10?"));

        let (status, body) = get(&store, "/label/INBOX/thread/99").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body.contains("Conversation not found"));
    }

    async fn get(store: &Store, uri: &str) -> (StatusCode, String) {
        let request = Request::get(uri).body(Body::empty()).unwrap();
        let response = router(store.clone()).oneshot(request).await.unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }
}
