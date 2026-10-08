//! Imports messages from an mbox file into the local store
//!
//! Like synchronized messages, imported messages are only stored once: a message is skipped if a
//! message with the same `Message-ID` header is already stored. A message without a `Message-ID`
//! gets one derived from a hash of its source, so that importing it again doesn't store it twice.
//! Imported messages are marked as read, and are not added to any mailbox.

use core::fmt::{self, Write as _};
use core::str;
use std::io::{self, BufRead};

use jiff::Timestamp;
use jiff::civil::DateTime;
use jiff::tz::TimeZone;
use sha2::{Digest, Sha256};
use store::{Store, StoreError};

use crate::imap::Flags;
use crate::store::{MessageContents, MessageData, MessageKey, MessageSource};

/// Stores the messages read from an mbox file
///
/// The messages are stored in batches, each in its own write transaction, so that other processes
/// can use the database while a large file is imported.
///
/// # Errors
///
/// Returns an error if the file can't be read or the store can't be written. Batches stored
/// before the error are kept.
pub fn import_mbox(store: &Store, mbox: impl BufRead) -> Result<ImportReport, ImportError> {
    let mut report = ImportReport::default();
    let mut batch = Vec::new();
    let mut size = 0;
    for message in Mbox::new(mbox) {
        let MboxMessage { received, source } = message?;
        let (mut data, content) = MessageData::extract(&source, received);
        data.flags = Flags::SEEN;
        if data.message_id.is_none() {
            data.message_id = Some(synthesize_message_id(&source));
        }

        size += source.len();
        batch.push((data, content, source));
        if size >= BATCH_SIZE {
            commit(store, &batch, &mut report)?;
            batch.clear();
            size = 0;
        }
    }

    if !batch.is_empty() {
        commit(store, &batch, &mut report)?;
    }

    Ok(report)
}

/// Reads the messages from an mbox file
///
/// Each message is preceded by a `From ` line. Lines in the message that start with `From `, after
/// any number of `>` characters, are quoted with an extra `>`, which is removed (the mboxrd format).
struct Mbox<R> {
    reader: R,
    line: Vec<u8>,
    message: Option<MboxMessage>,
}

impl<R: BufRead> Mbox<R> {
    fn new(reader: R) -> Self {
        Self {
            reader,
            line: Vec::new(),
            message: None,
        }
    }
}

impl<R: BufRead> Iterator for Mbox<R> {
    type Item = io::Result<MboxMessage>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.line.clear();
            match self.reader.read_until(b'\n', &mut self.line) {
                Ok(0) => return self.message.take().map(Ok),
                Ok(_) => {}
                Err(error) => return Some(Err(error)),
            }

            if let Some(separator) = self.line.strip_prefix(b"From ") {
                let next = MboxMessage {
                    received: parse_received(separator),
                    source: Vec::new(),
                };

                match self.message.replace(next) {
                    Some(message) => return Some(Ok(message)),
                    None => continue,
                }
            }

            let Some(message) = &mut self.message else {
                continue;
            };

            let mut quotes = 0;
            for byte in &self.line {
                if *byte != b'>' {
                    break;
                }
                quotes += 1;
            }

            match quotes > 0 && self.line[quotes..].starts_with(b"From ") {
                true => message.source.extend_from_slice(&self.line[1..]),
                false => message.source.extend_from_slice(&self.line),
            }
        }
    }
}

/// A message read from an mbox file
struct MboxMessage {
    /// When the message was received, according to its `From ` line
    received: Option<Timestamp>,
    /// The source of the message
    source: Vec<u8>,
}

/// Parses the time from a `From ` line, after the `From ` prefix
///
/// The sender's address is followed by the time in the format of C's `asctime()`, which is in
/// UTC. Google Takeout adds a UTC offset before the year.
fn parse_received(separator: &[u8]) -> Option<Timestamp> {
    let (_, time) = str::from_utf8(separator).ok()?.split_once(' ')?;
    let mut normalized = String::new();
    for part in time.split_whitespace() {
        if !normalized.is_empty() {
            normalized.push(' ');
        }
        normalized.push_str(part);
    }

    if let Ok(timestamp) = Timestamp::strptime("%a %b %d %H:%M:%S %z %Y", &normalized) {
        return Some(timestamp);
    }

    let time = DateTime::strptime("%a %b %d %H:%M:%S %Y", &normalized).ok()?;
    TimeZone::UTC.to_timestamp(time).ok()
}

/// Derives a `Message-ID` from the hash of a message's source
fn synthesize_message_id(source: &[u8]) -> String {
    let mut message_id = String::new();
    for byte in Sha256::digest(source) {
        write!(message_id, "{byte:02x}").unwrap();
    }

    message_id.push_str("@imported.encove.eu");
    message_id
}

/// Stores the messages that aren't stored yet in a single transaction
fn commit(
    store: &Store,
    batch: &[(MessageData, MessageContents, Vec<u8>)],
    report: &mut ImportReport,
) -> Result<(), StoreError> {
    let writer = store.writer()?;
    let mut message_ids = writer.table::<MessageKey>()?;
    let mut metadata = writer.table::<MessageData>()?;
    let mut contents = writer.table::<MessageContents>()?;
    let mut sources = writer.table::<MessageSource>()?;
    let mut key = match metadata.last()? {
        Some((last, _)) => last.value().next(),
        None => MessageKey::FIRST,
    };

    for (data, content, source) in batch {
        if let Some(message_id) = &data.message_id {
            if message_ids.get(message_id.as_str())?.is_some() {
                report.duplicates += 1;
                continue;
            }

            message_ids.insert(message_id.as_str(), key)?;
        }

        metadata.insert(key, data)?;
        contents.insert(key, content)?;
        sources.insert(key, source.as_slice())?;
        report.stored += 1;
        key = key.next();
    }

    drop((message_ids, metadata, contents, sources));
    writer.commit()
}

/// What an import changed
#[derive(Debug, Default)]
pub struct ImportReport {
    /// The number of messages stored
    pub stored: usize,
    /// The number of messages skipped because a message with the same `Message-ID` was stored
    pub duplicates: usize,
}

impl fmt::Display for ImportReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { stored, duplicates } = self;
        write!(f, "{stored} stored, {duplicates} duplicates skipped")
    }
}

/// An error that stopped an import
#[derive(Debug, thiserror::Error)]
pub enum ImportError {
    /// The mbox file could not be read
    #[error("failed to read mbox file: {0}")]
    Io(#[from] io::Error),
    /// The store could not be written
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// The total size of the message sources stored in a single transaction
const BATCH_SIZE: usize = 32 << 20;

#[cfg(test)]
mod tests {
    use crate::fixtures::{LUNCH, PLANS, REPLY};

    use super::*;

    #[test]
    fn imports_mbox() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::new(directory.path().join("encove.redb"));

        let mut mbox = String::new();
        for (separator, source) in [
            ("From alice@example.com Thu Oct  8 20:00:00 2026", PLANS),
            ("From 1@xxx Thu Oct 08 22:01:00 +0200 2026", LUNCH),
            ("From 2@xxx Thu Oct 08 20:02:00 +0000 2026", REPLY),
            ("From 3@xxx Thu Oct 08 20:03:00 +0000 2026", LUNCH),
            ("From 4@xxx yesterday", QUOTED),
        ] {
            writeln!(mbox, "{separator}").unwrap();
            mbox.push_str(source);
            mbox.push('\n');
        }

        let report = import_mbox(&store, mbox.as_bytes()).unwrap();
        assert_eq!((report.stored, report.duplicates), (4, 1));

        let report = import_mbox(&store, mbox.as_bytes()).unwrap();
        assert_eq!((report.stored, report.duplicates), (0, 5));

        let reader = store.reader().unwrap();
        let sources = reader.table::<MessageSource>().unwrap();
        let mut stored = Vec::new();
        for row in reader.table::<MessageData>().unwrap().iter().unwrap() {
            let (key, data) = row.unwrap();
            let MessageData {
                received,
                flags,
                message_id,
                subject,
                ..
            } = data.value();
            assert_eq!(flags, Flags::SEEN, "{subject}");

            let message_id = message_id.unwrap();
            stored.push(format!("{received} {subject} <{message_id}>"));

            let source = sources.get(key.value()).unwrap().unwrap();
            let source = String::from_utf8(source.value().to_vec()).unwrap();
            if subject == "Quoting" {
                assert!(source.contains("\r\nFrom the archive\r\n>From the quote\r\n"));
            }
        }

        assert_eq!(
            stored,
            [
                "2026-10-08T20:00:00Z Plans for Thursday <a@example.com>",
                "2026-10-08T20:01:00Z Lunch <b@example.com>",
                "2026-10-08T20:02:00Z Re: Plans for Thursday <c@example.com>",
                "2026-10-07T12:30:00Z Quoting \
                 <533e151dd3246a43c8baff3dfc68271e3f3829753f3923379838cc1a28fb3b61@imported.encove.eu>",
            ]
        );
    }

    #[test]
    fn parses_received_times() {
        for (separator, expected) in [
            (
                "1@xxx Thu Oct 08 20:28:50 +0000 2026\n",
                Some("2026-10-08T20:28:50Z"),
            ),
            (
                "1@xxx Thu Oct 08 22:28:50 +0200 2026\r\n",
                Some("2026-10-08T20:28:50Z"),
            ),
            (
                "MAILER-DAEMON Thu Oct  8 20:28:50 2026\n",
                Some("2026-10-08T20:28:50Z"),
            ),
            ("alice@example.com yesterday\n", None),
        ] {
            let received = parse_received(separator.as_bytes());
            assert_eq!(
                received.map(|received| received.to_string()).as_deref(),
                expected,
                "{separator:?}"
            );
        }
    }

    /// A message without a `Message-ID`, with lines quoted in the mbox file
    const QUOTED: &str = "From: Bob <bob@example.com>\r
Subject: Quoting\r
Date: Wed, 07 Oct 2026 12:30:00 +0000\r
\r
>From the archive\r
>>From the quote\r
";
}
