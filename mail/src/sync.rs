//! Synchronizes recent messages from an IMAP server into the local store
//!
//! For each mailbox, the messages that arrived in the last few days are found with
//! `UID SEARCH SINCE`. New messages are first matched to stored ones by their `Message-ID` header,
//! so that a message in several mailboxes is only downloaded once. The flags of known messages are
//! refreshed, using CONDSTORE (RFC 7162) to only fetch changes if the server supports it. Messages
//! that left the window or the server are removed, unless they are older than every message that is
//! still in a mailbox, like messages imported from an archive.

use core::time::Duration;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;

use jiff::civil::Date;
use jiff::{ToSpan, Zoned};
use mail_parser::MessageParser;
use store::{Store, WriteTable};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::runtime::{Handle, RuntimeFlavor};
use tokio::task::block_in_place;
use tokio::time::timeout;
use tokio_rustls::client::TlsStream;

use crate::imap::{self, Account, Fetched, Flags, ImapClient, ListedMailbox, MailboxName};
use crate::store::{Entry, Mailbox, MessageContents, MessageData, MessageKey, MessageSource};

pub struct Sync<'a, S: AsyncRead + AsyncWrite + Unpin> {
    client: ImapClient<S>,
    store: &'a Store,
    since: Date,
    observed: ObservedFlags,
}

impl<'a> Sync<'a, TlsStream<TcpStream>> {
    /// Creates a new `Sync` instance
    pub async fn connect(
        account: &'a Account,
        store: &'a Store,
        days: i64,
    ) -> Result<Sync<'a, TlsStream<TcpStream>>, Error> {
        let mut client = ImapClient::connect(account).await?;
        client.authenticate(account).await?;

        Ok(Self {
            client,
            store,
            since: Zoned::now().date().saturating_sub(days.days()),
            observed: HashMap::new(),
        })
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Sync<'_, S> {
    pub async fn sync(&mut self) -> Result<Report, Error> {
        block_in_place(|| {
            let writer = self.store.writer()?;
            writer.table::<Mailbox>()?;
            writer.table::<Entry>()?;
            writer.table::<MessageKey>()?;
            writer.table::<MessageData>()?;
            writer.table::<MessageContents>()?;
            writer.table::<MessageSource>()?;
            writer.commit()
        })?;

        let mut report = Report::default();
        let mut listed = Vec::new();
        for mailbox in self.client.list().await? {
            if !mailbox.selectable {
                continue;
            }

            listed.push(mailbox.name.clone());
            match self.sync_mailbox(&mailbox).await {
                Ok(changes) => {
                    tracing::info!(mailbox = %mailbox.name.decoded(), %changes, "synchronized");
                    report.add(changes);
                }
                Err(Error::Imap(
                    error @ (imap::ImapError::Rejected { .. } | imap::ImapError::Unquotable(_)),
                )) => {
                    tracing::warn!(mailbox = %mailbox.name.decoded(), %error, "skipping mailbox");
                }
                Err(error) => return Err(error),
            }
        }

        report.removed = block_in_place(|| self.remove_stale(&listed))?;
        Ok(report)
    }

    async fn sync_mailbox(&mut self, mailbox: &ListedMailbox) -> Result<Report, Error> {
        let ListedMailbox {
            name,
            delimiter,
            selectable: _,
            special_use,
        } = mailbox;

        let selected = self.client.examine(name).await?;
        let (previous_modseq, known) =
            block_in_place(|| self.known_entries(name, selected.uid_validity))?;

        let window = self.client.search_since(self.since).await?;
        let mut report = Report {
            mailboxes: 1,
            ..Report::default()
        };

        let mut departed = Vec::new();
        let mut retained = Vec::new();
        for uid in known.keys() {
            match window.contains(uid) {
                true => retained.push(*uid),
                false => departed.push(*uid),
            }
        }

        let mut added = Vec::new();
        for uid in &window {
            if !known.contains_key(uid) {
                added.push(*uid);
            }
        }

        if !departed.is_empty() {
            block_in_place(|| self.remove_entries(name, &departed))?;
            report.departed = departed.len();
        }

        let mut downloads = BTreeMap::new();
        for chunk in added.chunks(HEADER_BATCH) {
            let fetched = self.client.fetch_message_ids(chunk).await?;
            report.linked += block_in_place(|| self.link_messages(name, fetched, &mut downloads))?;
        }

        let mut uids = Vec::new();
        for uid in downloads.keys() {
            uids.push(*uid);
        }

        for chunk in uids.chunks(SOURCE_BATCH) {
            let fetched = self.client.fetch_sources(chunk).await?;
            report.downloaded += block_in_place(|| self.store_sources(fetched, &downloads, name))?;
        }

        let refresh = match (previous_modseq, selected.highest_modseq) {
            (Some(previous), Some(current)) if previous == current => FlagRefresh::Unchanged,
            (Some(previous), Some(_)) => FlagRefresh::ChangedSince(previous),
            (Some(_), None) | (None, _) => FlagRefresh::All,
        };

        report.updated = match refresh {
            FlagRefresh::Unchanged => 0,
            FlagRefresh::ChangedSince(modseq) => {
                self.refresh_flags(name, &known, &retained, Some(modseq))
                    .await?
            }
            FlagRefresh::All => self.refresh_flags(name, &known, &retained, None).await?,
        };

        let stored = Mailbox {
            name: name.clone(),
            uid_validity: selected.uid_validity,
            highest_modseq: selected.highest_modseq,
            special_use: *special_use,
            delimiter: delimiter.clone(),
        };
        block_in_place(|| self.save_mailbox(&stored))?;

        Ok(report)
    }

    fn save_mailbox(&self, mailbox: &Mailbox) -> Result<(), store::StoreError> {
        let writer = self.store.writer()?;
        writer
            .table::<Mailbox>()?
            .insert(mailbox.name.as_str(), mailbox)?;
        writer.commit()
    }

    /// Returns the stored modification sequence and entries of a mailbox
    ///
    /// If the mailbox has a new UIDVALIDITY, its stored entries are removed instead.
    fn known_entries(
        &self,
        mailbox: &MailboxName,
        uid_validity: u32,
    ) -> Result<(Option<u64>, Entries), store::StoreError> {
        let writer = self.store.writer()?;
        let stored = writer
            .table::<Mailbox>()?
            .get(mailbox.as_str())?
            .map(|stored| stored.value());

        let mut entries = writer.table::<Entry>()?;
        let name = mailbox.as_str();
        let known = match stored {
            Some(stored) if stored.uid_validity == uid_validity => {
                let mut known = BTreeMap::new();
                for row in entries.range((name, 0)..=(name, u32::MAX))? {
                    let (_, entry) = row?;
                    let Entry {
                        mailbox: _,
                        uid,
                        message,
                    } = entry.value();
                    known.insert(uid, message);
                }
                (stored.highest_modseq, known)
            }
            Some(_) | None => {
                entries.retain_in((name, 0)..=(name, u32::MAX), |_, _| false)?;
                (None, BTreeMap::new())
            }
        };

        drop(entries);
        writer.commit()?;
        Ok(known)
    }

    /// Updates the stored flags of known messages, returning the number of messages that changed
    async fn refresh_flags(
        &mut self,
        mailbox: &MailboxName,
        known: &Entries,
        uids: &[u32],
        changed_since: Option<u64>,
    ) -> Result<usize, Error> {
        let mut updated = 0;
        for chunk in uids.chunks(FLAG_BATCH) {
            let fetched = self.client.fetch_flags(chunk, changed_since).await?;
            updated += block_in_place(|| self.update_flags(mailbox, fetched, known))?;
        }

        Ok(updated)
    }

    fn remove_entries(&self, mailbox: &MailboxName, uids: &[u32]) -> Result<(), store::StoreError> {
        let writer = self.store.writer()?;
        let mut entries = writer.table::<Entry>()?;
        for uid in uids {
            entries.remove((mailbox.as_str(), *uid))?;
        }

        drop(entries);
        writer.commit()
    }

    /// Adds entries for fetched messages that are already stored, returning how many there were
    ///
    /// The UIDs and flags of the other messages are added to `downloads`.
    fn link_messages(
        &mut self,
        mailbox: &MailboxName,
        fetched: Vec<Fetched>,
        downloads: &mut BTreeMap<u32, Flags>,
    ) -> Result<usize, store::StoreError> {
        let writer = self.store.writer()?;
        let message_ids = writer.table::<MessageKey>()?;
        let mut metadata = writer.table::<MessageData>()?;
        let mut entries = writer.table::<Entry>()?;
        let mut linked = 0;

        let parser = MessageParser::default();
        for message in fetched {
            let Fetched {
                uid,
                flags,
                internal_date: _,
                body,
            } = message;

            let flags = flags.unwrap_or_default();
            let key = match body.as_deref().and_then(|header| {
                parser
                    .parse_headers(header)?
                    .message_id()
                    .map(str::to_owned)
            }) {
                Some(message_id) => message_ids.get(message_id.as_str())?.map(|key| key.value()),
                None => None,
            };

            match key {
                Some(key) => {
                    observe_flags(&mut self.observed, &mut metadata, key, flags, mailbox)?;
                    insert_entry(&mut entries, mailbox, uid, key)?;
                    linked += 1;
                }
                None => {
                    downloads.insert(uid, flags);
                }
            }
        }

        drop((message_ids, metadata, entries));
        writer.commit()?;
        Ok(linked)
    }

    /// Stores fetched messages and adds their entries, returning how many there were
    ///
    /// A message is only stored once: if a message with the same `Message-ID` is already stored, the
    /// entry refers to that one instead. New messages get the key after the highest stored key.
    fn store_sources(
        &mut self,
        fetched: Vec<Fetched>,
        downloads: &BTreeMap<u32, Flags>,
        mailbox: &MailboxName,
    ) -> Result<usize, store::StoreError> {
        let mut messages = Vec::new();
        for Fetched {
            uid,
            flags: _,
            internal_date,
            body,
        } in fetched
        {
            let Some(source) = body else {
                continue;
            };
            let (mut data, content) = MessageData::extract(&source, internal_date);
            data.flags = downloads.get(&uid).copied().unwrap_or_default();
            messages.push((uid, data, content, source));
        }

        let writer = self.store.writer()?;
        let mut message_ids = writer.table::<MessageKey>()?;
        let mut metadata = writer.table::<MessageData>()?;
        let mut contents = writer.table::<MessageContents>()?;
        let mut sources = writer.table::<MessageSource>()?;
        let mut entries = writer.table::<Entry>()?;
        for (uid, message, content, source) in &messages {
            let stored = match &message.message_id {
                Some(message_id) => message_ids.get(message_id.as_str())?.map(|key| key.value()),
                None => None,
            };

            let key = match stored {
                Some(key) => {
                    observe_flags(
                        &mut self.observed,
                        &mut metadata,
                        key,
                        message.flags,
                        mailbox,
                    )?;
                    key
                }
                None => {
                    let key = match metadata.last()? {
                        Some((last, _)) => last.value().next(),
                        None => MessageKey::FIRST,
                    };

                    metadata.insert(key, message)?;
                    contents.insert(key, content)?;
                    sources.insert(key, source.as_slice())?;
                    if let Some(message_id) = &message.message_id {
                        message_ids.insert(message_id.as_str(), key)?;
                    }

                    self.observed.insert(key, (message.flags, mailbox.clone()));
                    key
                }
            };

            insert_entry(&mut entries, mailbox, *uid, key)?;
        }

        drop((message_ids, metadata, contents, sources, entries));
        writer.commit()?;
        Ok(messages.len())
    }

    /// Stores the flags of fetched messages, returning how many messages changed
    fn update_flags(
        &mut self,
        mailbox: &MailboxName,
        fetched: Vec<Fetched>,
        known: &Entries,
    ) -> Result<usize, store::StoreError> {
        let mut observed = Vec::new();
        for Fetched {
            uid,
            flags,
            internal_date: _,
            body: _,
        } in fetched
        {
            let (Some(flags), Some(key)) = (flags, known.get(&uid)) else {
                continue;
            };
            observed.push((*key, flags));
        }

        if observed.is_empty() {
            return Ok(0);
        }

        let writer = self.store.writer()?;
        let mut metadata = writer.table::<MessageData>()?;
        let mut updated = 0;
        for (key, flags) in observed {
            if observe_flags(&mut self.observed, &mut metadata, key, flags, mailbox)? {
                updated += 1;
            }
        }

        drop(metadata);
        writer.commit()?;
        Ok(updated)
    }

    /// Removes mailboxes that are no longer listed and messages that are no longer in any mailbox
    ///
    /// Only messages received after the oldest message that is still in a mailbox are removed.
    /// Returns the number of messages removed.
    fn remove_stale(&self, listed: &[MailboxName]) -> Result<usize, store::StoreError> {
        let writer = self.store.writer()?;
        writer
            .table::<Mailbox>()?
            .retain(|name, _| is_listed(listed, name))?;

        let mut entries = writer.table::<Entry>()?;
        entries.retain(|(name, _), _| is_listed(listed, name))?;

        let mut referenced = HashSet::new();
        for row in entries.iter()? {
            let (_, entry) = row?;
            let Entry {
                mailbox: _,
                uid: _,
                message,
            } = entry.value();
            referenced.insert(message);
        }

        let mut metadata = writer.table::<MessageData>()?;
        let mut oldest = None;
        for key in &referenced {
            let Some(data) = metadata.get(*key)? else {
                continue;
            };

            let MessageData { received, .. } = data.value();
            if oldest.is_none_or(|oldest| received < oldest) {
                oldest = Some(received);
            }
        }

        let mut removed = Vec::new();
        metadata.retain(|key, data| {
            let MessageData {
                received,
                message_id,
                ..
            } = data;
            let keep = referenced.contains(&key) || oldest.is_none_or(|oldest| received <= oldest);
            if !keep {
                removed.push((key, message_id));
            }
            keep
        })?;

        let mut contents = writer.table::<MessageContents>()?;
        let mut sources = writer.table::<MessageSource>()?;
        let mut message_ids = writer.table::<MessageKey>()?;
        for (key, message_id) in &removed {
            contents.remove(*key)?;
            sources.remove(*key)?;
            if let Some(message_id) = message_id {
                message_ids.remove(message_id.as_str())?;
            }
        }

        drop((entries, metadata, contents, sources, message_ids));
        writer.commit()?;
        Ok(removed.len())
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Drop for Sync<'_, S> {
    fn drop(&mut self) {
        let Ok(runtime) = Handle::try_current() else {
            return;
        };

        if runtime.runtime_flavor() != RuntimeFlavor::MultiThread {
            return;
        }

        let logout = || runtime.block_on(timeout(LOGOUT_TIMEOUT, self.client.logout()));
        match block_in_place(logout) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!(%error, "failed to log out"),
            Err(_) => tracing::warn!("timed out logging out"),
        }
    }
}

/// Records the flags of a stored message as seen in a mailbox, returning whether they changed
///
/// Flags are assumed to be the same in every mailbox containing the message, so a warning is
/// logged if this synchronization saw different flags for it in another mailbox. The flags seen
/// last are stored.
fn observe_flags(
    observed: &mut ObservedFlags,
    metadata: &mut WriteTable<'_, MessageData>,
    key: MessageKey,
    flags: Flags,
    mailbox: &MailboxName,
) -> Result<bool, store::StoreError> {
    if let Some((previous, other)) = observed.insert(key, (flags, mailbox.clone()))
        && previous != flags
    {
        tracing::warn!(
            message = key.get(),
            first = %other.decoded(),
            second = %mailbox.decoded(),
            ?previous,
            ?flags,
            "message has different flags in different mailboxes"
        );
    }

    let Some(mut stored) = metadata.get(key)?.map(|stored| stored.value()) else {
        return Ok(false);
    };

    if stored.flags == flags {
        return Ok(false);
    }

    stored.flags = flags;
    metadata.insert(key, &stored)?;
    Ok(true)
}

/// Records that a mailbox contains a message at the given UID
fn insert_entry(
    entries: &mut WriteTable<'_, Entry>,
    mailbox: &MailboxName,
    uid: u32,
    message: MessageKey,
) -> Result<(), store::StoreError> {
    let entry = Entry {
        mailbox: mailbox.clone(),
        uid,
        message,
    };

    entries.insert((mailbox.as_str(), uid), &entry)
}

fn is_listed(listed: &[MailboxName], name: &str) -> bool {
    for mailbox in listed {
        if mailbox.as_str() == name {
            return true;
        }
    }

    false
}

/// The message key of each UID in a mailbox
type Entries = BTreeMap<u32, MessageKey>;

/// The flags seen for each message during a synchronization, and the mailbox they were seen in
type ObservedFlags = HashMap<MessageKey, (Flags, MailboxName)>;

/// How to bring the stored flags of known messages up to date
enum FlagRefresh {
    /// The mailbox has not changed since the last synchronization
    Unchanged,
    /// Fetch the flags of messages changed after this modification sequence
    ChangedSince(u64),
    /// Fetch the flags of all known messages
    All,
}

/// What a synchronization changed
#[derive(Debug, Default)]
pub struct Report {
    /// The number of mailboxes synchronized
    pub mailboxes: usize,
    /// The number of messages downloaded
    pub downloaded: usize,
    /// The number of new mailbox entries for messages that were already stored
    pub linked: usize,
    /// The number of messages with changed flags
    pub updated: usize,
    /// The number of messages that left a mailbox or the window
    pub departed: usize,
    /// The number of messages removed because they are no longer in any mailbox
    pub removed: usize,
}

impl Report {
    fn add(&mut self, other: Self) {
        let Self {
            mailboxes,
            downloaded,
            linked,
            updated,
            departed,
            removed,
        } = other;

        self.mailboxes += mailboxes;
        self.downloaded += downloaded;
        self.linked += linked;
        self.updated += updated;
        self.departed += departed;
        self.removed += removed;
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            mailboxes,
            downloaded,
            linked,
            updated,
            departed,
            removed,
        } = self;

        write!(
            f,
            "{mailboxes} mailboxes, {downloaded} downloaded, {linked} linked, {updated} updated, \
             {departed} departed, {removed} removed"
        )
    }
}

/// An error that stopped the synchronization
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The IMAP connection failed
    #[error(transparent)]
    Imap(#[from] imap::ImapError),
    /// The store could not be read or written
    #[error(transparent)]
    Store(#[from] store::StoreError),
}

const HEADER_BATCH: usize = 500;
const SOURCE_BATCH: usize = 25;
const FLAG_BATCH: usize = 500;
const LOGOUT_TIMEOUT: Duration = Duration::from_secs(1);

#[cfg(test)]
mod tests {
    use core::fmt::Write as _;
    use std::time::Instant;

    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;
    use jiff::Timestamp;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream};

    use crate::{
        Address, Body,
        fixtures::{LUNCH, PLANS, REPLY},
    };

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn syncs_mailboxes() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::new(directory.path().join("encove.redb"));

        let inbox = FakeMailbox::new("INBOX", "\\HasNoChildren", 7)
            .message(1, "\\Seen", 5, PLANS)
            .message(2, "", 6, LUNCH);
        let all = FakeMailbox::new("[Gmail]/All Mail", "\\All \\HasNoChildren", 13)
            .message(11, "\\Seen", 11, PLANS)
            .message(12, "", 12, LUNCH)
            .message(13, "\\Seen", 13, REPLY);
        let sent = FakeMailbox::new("[Gmail]/Sent Mail", "\\Sent \\HasNoChildren", 21)
            .message(21, "\\Seen", 21, REPLY);

        let (report, commands) = run(&store, vec![inbox, all, sent.clone()]).await;
        assert_eq!(
            (report.mailboxes, report.downloaded, report.linked),
            (3, 3, 3)
        );
        for command in &commands {
            assert!(!command.starts_with("SELECT"), "{command}");
            assert!(!command.contains("BODY["), "{command}");
        }
        assert_eq!(commands.last().map(String::as_str), Some("LOGOUT"));

        let inbox = FakeMailbox::new("INBOX", "\\HasNoChildren", 8).message(2, "\\Seen", 8, LUNCH);
        let all = FakeMailbox::new("[Gmail]/All Mail", "\\All \\HasNoChildren", 14)
            .message(11, "\\Seen", 11, PLANS)
            .message(12, "\\Seen", 14, LUNCH)
            .message(13, "\\Seen", 13, REPLY);

        let (report, commands) = run(&store, vec![inbox, all, sent]).await;
        assert_eq!(report.downloaded, 0);
        assert_eq!(report.updated, 1);
        assert_eq!(report.departed, 1);
        assert_eq!(report.removed, 0);
        assert!(commands.contains(&"UID FETCH 2 (UID FLAGS) (CHANGEDSINCE 7)".to_owned()));
        assert!(commands.contains(&"UID FETCH 11:13 (UID FLAGS) (CHANGEDSINCE 13)".to_owned()));
        assert!(!commands.contains(&"UID FETCH 21 (UID FLAGS) (CHANGEDSINCE 21)".to_owned()));

        let reader = store.reader().unwrap();
        let metadata = reader.table::<MessageData>().unwrap();
        let mut stored = Vec::new();
        for row in reader.table::<Entry>().unwrap().iter().unwrap() {
            let (_, entry) = row.unwrap();
            let Entry {
                mailbox,
                uid,
                message,
            } = entry.value();
            let MessageData { subject, flags, .. } =
                metadata.get(message).unwrap().unwrap().value();
            assert_eq!(flags, Flags::SEEN, "{subject}");
            stored.push(format!("{} {uid}: {subject}", mailbox.as_str()));
        }
        drop((metadata, reader));

        assert_eq!(
            stored,
            [
                "INBOX 2: Lunch",
                "[Gmail]/All Mail 11: Plans for Thursday",
                "[Gmail]/All Mail 12: Lunch",
                "[Gmail]/All Mail 13: Re: Plans for Thursday",
                "[Gmail]/Sent Mail 21: Re: Plans for Thursday",
            ]
        );

        let all = FakeMailbox::new("[Gmail]/All Mail", "\\All \\HasNoChildren", 15)
            .message(12, "\\Seen", 14, LUNCH);
        let (report, _) = run(&store, vec![all]).await;
        assert_eq!(report.departed, 2);
        assert_eq!(report.removed, 1);

        let reader = store.reader().unwrap();
        let mut subjects = Vec::new();
        for row in reader.table::<MessageData>().unwrap().iter().unwrap() {
            let (_, data) = row.unwrap();
            let MessageData { subject, .. } = data.value();
            subjects.push(subject);
        }
        assert_eq!(subjects, ["Plans for Thursday", "Lunch"]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn gives_up_logging_out() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::new(directory.path().join("encove.redb"));

        let (client, mut server) = tokio::io::duplex(1 << 16);
        server
            .write_all(b"* OK [CAPABILITY IMAP4rev1] ready\r\n")
            .await
            .unwrap();
        let sync = Sync {
            client: ImapClient::new(client).await.unwrap(),
            store: &store,
            since: Date::constant(2026, 10, 1),
            observed: HashMap::new(),
        };

        let start = Instant::now();
        drop(sync);
        assert!(start.elapsed() < LOGOUT_TIMEOUT * 2);
        drop(server);
    }

    async fn run(store: &Store, mailboxes: Vec<FakeMailbox>) -> (Report, Vec<String>) {
        let account = Account {
            host: "imap.example.com".to_owned(),
            port: 993,
            user: "me@example.com".to_owned(),
            token: "secret".to_owned(),
        };

        let (client, server) = tokio::io::duplex(1 << 16);
        let server = tokio::spawn(serve(server, mailboxes));
        let mut client = ImapClient::new(client).await.unwrap();
        client.authenticate(&account).await.unwrap();

        let mut sync = Sync {
            client,
            store,
            since: Date::constant(2026, 10, 1),
            observed: HashMap::new(),
        };
        let report = sync.sync().await.unwrap();
        drop(sync);
        (report, server.await.unwrap())
    }

    /// Answers the commands the client sends with the contents of the given mailboxes
    async fn serve(stream: DuplexStream, mailboxes: Vec<FakeMailbox>) -> Vec<String> {
        let (read, mut write) = tokio::io::split(stream);
        let mut lines = BufReader::new(read).lines();
        write
            .write_all(b"* OK [CAPABILITY IMAP4rev1 AUTH=OAUTHBEARER SASL-IR] ready\r\n")
            .await
            .unwrap();

        let mut commands = Vec::new();
        let mut selected = None;
        while let Some(line) = lines.next_line().await.unwrap() {
            let (tag, command) = line.split_once(' ').unwrap();
            commands.push(command.to_owned());

            let mut response = String::new();
            if let Some(encoded) = command.strip_prefix("AUTHENTICATE OAUTHBEARER ") {
                let decoded = String::from_utf8(STANDARD.decode(encoded).unwrap()).unwrap();
                assert!(decoded.contains("auth=Bearer secret"));
                write!(
                    response,
                    "{tag} OK [CAPABILITY IMAP4rev1 CONDSTORE SPECIAL-USE LIST-EXTENDED] done\r\n"
                )
                .unwrap();
            } else if command == "LIST \"\" \"*\" RETURN (SPECIAL-USE)" {
                response.push_str("* LIST (\\Noselect \\HasChildren) \"/\" \"[Gmail]\"\r\n");
                for mailbox in &mailboxes {
                    write!(
                        response,
                        "* LIST ({}) \"/\" \"{}\"\r\n",
                        mailbox.attributes, mailbox.name
                    )
                    .unwrap();
                }
                write!(response, "{tag} OK done\r\n").unwrap();
            } else if let Some(name) = command.strip_prefix("EXAMINE \"") {
                let name = name.strip_suffix("\" (CONDSTORE)").unwrap();
                let mut found = None;
                for mailbox in &mailboxes {
                    if mailbox.name == name {
                        found = Some(mailbox);
                    }
                }

                let mailbox = found.unwrap();
                write!(
                    response,
                    "* FLAGS (\\Seen)\r\n* OK [UIDVALIDITY 1] UIDs valid\r\n\
                     * OK [HIGHESTMODSEQ {}] Highest\r\n{tag} OK [READ-ONLY] done\r\n",
                    mailbox.modseq
                )
                .unwrap();
                selected = Some(mailbox);
            } else if command == "UID SEARCH SINCE 1-Oct-2026" {
                response.push_str("* SEARCH");
                for message in &selected.unwrap().messages {
                    write!(response, " {}", message.uid).unwrap();
                }
                write!(response, "\r\n{tag} OK done\r\n").unwrap();
            } else if let Some(arguments) = command.strip_prefix("UID FETCH ") {
                let (set, items) = arguments.split_once(' ').unwrap();
                let uids = parse_set(set);
                let changed_since = match items.split_once("(CHANGEDSINCE ") {
                    Some((_, modseq)) => modseq.trim_end_matches(')').parse().unwrap(),
                    None => 0,
                };

                for (index, message) in selected.unwrap().messages.iter().enumerate() {
                    if !uids.contains(&message.uid) || message.modseq <= changed_since {
                        continue;
                    }

                    let FakeMessage {
                        uid,
                        flags,
                        modseq,
                        source,
                    } = message;
                    let number = index + 1;
                    if items.contains("HEADER.FIELDS") {
                        let header = format!("{}\r\n\r\n", source.lines().nth(3).unwrap());
                        write!(
                            response,
                            "* {number} FETCH (UID {uid} FLAGS ({flags}) \
                             BODY[HEADER.FIELDS (MESSAGE-ID)] {{{}}}\r\n{header})\r\n",
                            header.len()
                        )
                        .unwrap();
                    } else if items.contains("BODY.PEEK[]") {
                        write!(
                            response,
                            "* {number} FETCH (UID {uid} INTERNALDATE \"07-Oct-2026 10:{:02}:00 +0000\" \
                             BODY[] {{{}}}\r\n{source})\r\n",
                            uid % 60,
                            source.len()
                        )
                        .unwrap();
                    } else {
                        write!(
                            response,
                            "* {number} FETCH (UID {uid} FLAGS ({flags}) MODSEQ ({modseq}))\r\n"
                        )
                        .unwrap();
                    }
                }
                write!(response, "{tag} OK done\r\n").unwrap();
            } else if command == "LOGOUT" {
                write!(response, "* BYE logging out\r\n{tag} OK done\r\n").unwrap();
                write.write_all(response.as_bytes()).await.unwrap();
                break;
            } else {
                panic!("unexpected command: {command}");
            }

            write.write_all(response.as_bytes()).await.unwrap();
        }

        commands
    }

    fn parse_set(set: &str) -> Vec<u32> {
        let mut uids = Vec::new();
        for part in set.split(',') {
            match part.split_once(':') {
                Some((start, end)) => {
                    for uid in start.parse().unwrap()..=end.parse().unwrap() {
                        uids.push(uid);
                    }
                }
                None => uids.push(part.parse().unwrap()),
            }
        }
        uids
    }

    #[derive(Clone)]
    struct FakeMailbox {
        name: &'static str,
        attributes: &'static str,
        modseq: u64,
        messages: Vec<FakeMessage>,
    }

    impl FakeMailbox {
        fn new(name: &'static str, attributes: &'static str, modseq: u64) -> Self {
            Self {
                name,
                attributes,
                modseq,
                messages: Vec::new(),
            }
        }

        fn message(
            mut self,
            uid: u32,
            flags: &'static str,
            modseq: u64,
            source: &'static str,
        ) -> Self {
            self.messages.push(FakeMessage {
                uid,
                flags,
                modseq,
                source,
            });
            self
        }
    }

    #[derive(Clone)]
    struct FakeMessage {
        uid: u32,
        flags: &'static str,
        modseq: u64,
        source: &'static str,
    }

    #[test]
    fn extracts_messages() {
        let received = Timestamp::from_second(1_791_000_000).unwrap();
        let (metadata, content) = MessageData::extract(REPLY.as_bytes(), Some(received));
        assert_eq!(metadata.received, received);
        assert_eq!(metadata.message_id.as_deref(), Some("c@example.com"));
        assert_eq!(metadata.date, Some("2026-10-07T12:00:00Z".parse().unwrap()));
        assert_eq!(metadata.subject, "Re: Plans for Thursday");
        assert_eq!(
            metadata.from.as_ref().map(Address::display_name),
            Some("Dirkjan")
        );
        let alice = Address {
            name: Some("Alice Example".to_owned()),
            address: "alice@example.com".to_owned(),
        };
        assert_eq!(metadata.to, [alice]);
        assert!(metadata.cc.is_empty() && metadata.bcc.is_empty());
        assert_eq!(metadata.snippet, "Café at 10?");
        assert_eq!(metadata.parents, ["a@example.com"]);
        assert!(matches!(&content.body, Body::Text(text) if text.starts_with("Café at 10?")));
        assert!(content.attachments.is_empty());

        let (metadata, content) = MessageData::extract(PLANS.as_bytes(), None);
        assert_eq!(metadata.received, metadata.date.unwrap());
        assert_eq!(
            metadata.from.as_ref().map(Address::display_name),
            Some("Alice Example")
        );
        assert_eq!(metadata.snippet, "Hi Dirkjan, here is the plan.");
        assert!(metadata.parents.is_empty());
        assert!(matches!(&content.body, Body::Html(html) if html.contains("tracker.example")));
        assert_eq!(content.attachments.len(), 1);
        assert_eq!(content.attachments[0].filename, "plan.pdf");
        assert_eq!(content.attachments[0].mime_type, "application/pdf");
    }
}
