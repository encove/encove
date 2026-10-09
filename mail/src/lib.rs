//! Synchronizes mail from an IMAP server, or imports it from mbox files, into a local store

mod imap;
pub use imap::{Account, Flags, ImapError, MailboxName, SpecialUse};

mod import;
pub use import::{ImportError, ImportReport, import_mbox};

mod store;
pub use store::{
    Address, Attachment, Body, Entry, Mailbox, MessageContents, MessageData, MessageEntry,
    MessageKey, MessageSource, ReadState, ThreadMessage,
};

mod sync;
pub use sync::{Error, Report, Sync};

#[cfg(any(test, feature = "fixtures"))]
pub mod fixtures;
