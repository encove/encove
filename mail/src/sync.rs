//! Synchronizes recent messages from an IMAP server into the local store
//!
//! For each mailbox, the messages that arrived in the last few days are found with
//! `UID SEARCH SINCE`. New messages are first matched to stored ones by their `Message-ID` header,
//! so that a message in several mailboxes is only downloaded once. The flags of stored messages
//! are refreshed, using CONDSTORE (RFC 7162) to only fetch changes if the server supports it.
//!
//! The state of each mailbox records where the window started at the last synchronization. The
//! entries from there on that are no longer in the window are checked with `UID SEARCH UID`: the
//! ones the server still has aged out of the window and are kept, like all entries of messages
//! that arrived before it, while the others are removed. A message is removed when it loses its
//! last entry, so messages that never had one, like messages imported from an archive that aren't
//! on the server, are kept. This keeps the work proportional to the window and the changes, rather
//! than to the number of stored messages.
//!
//! [`Sync::link()`] looks at all messages instead, to add the entries for stored messages, like
//! messages imported from an archive, without downloading any messages.

use core::time::Duration;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;

use jiff::civil::Date;
use mail_parser::MessageParser;
use store::{Store, StoreError, WriteTable};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::runtime::{Handle, RuntimeFlavor};
use tokio::task::block_in_place;
use tokio::time::timeout;
use tokio_rustls::client::TlsStream;

use crate::imap::{self, Account, Fetched, Flags, ImapClient, ListedMailbox, MailboxName};
use crate::store::{
    Entry, Mailbox, MessageContents, MessageData, MessageEntry, MessageKey, MessageSource,
};

pub struct Sync<'a, S: AsyncRead + AsyncWrite + Unpin> {
    client: ImapClient<S>,
    store: &'a Store,
    observed: ObservedFlags,
    /// The messages that lost an entry, which are removed if no other entry refers to them
    unlinked: HashSet<MessageKey>,
}

impl<'a> Sync<'a, TlsStream<TcpStream>> {
    /// Creates a new `Sync` instance
    pub async fn connect(
        account: &'a Account,
        store: &'a Store,
    ) -> Result<Sync<'a, TlsStream<TcpStream>>, Error> {
        let mut client = ImapClient::connect(account).await?;
        client.authenticate(account).await?;

        Ok(Self {
            client,
            store,
            observed: HashMap::new(),
            unlinked: HashSet::new(),
        })
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Sync<'_, S> {
    /// Synchronizes the messages that arrived on or after `since`
    ///
    /// Messages that aren't stored yet are downloaded, and messages that left their last mailbox
    /// are removed.
    pub async fn sync(&mut self, since: Date) -> Result<Report, Error> {
        let (mut report, listed) = self.sync_mailboxes(Scope::Since(since)).await?;
        report.removed = block_in_place(|| self.remove_stale(&listed))?;
        Ok(report)
    }

    /// Links stored messages to all mailboxes containing them, and updates their flags
    ///
    /// Unlike [`Sync::sync()`], this looks at all messages, but it only fetches the `Message-ID`
    /// header and flags of messages that don't have an entry yet. Messages that aren't stored are
    /// not downloaded, and no messages are removed.
    pub async fn link(&mut self) -> Result<Report, Error> {
        let (report, _) = self.sync_mailboxes(Scope::All).await?;
        Ok(report)
    }

    /// Synchronizes each selectable mailbox, returning the report and the names of the mailboxes
    async fn sync_mailboxes(&mut self, scope: Scope) -> Result<(Report, Vec<MailboxName>), Error> {
        block_in_place(|| {
            let writer = self.store.writer()?;
            writer.table::<Mailbox>()?;
            writer.table::<Entry>()?;
            writer.table::<MessageEntry>()?;
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
            match self.sync_mailbox(&mailbox, scope).await {
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

        Ok((report, listed))
    }

    async fn sync_mailbox(
        &mut self,
        mailbox: &ListedMailbox,
        scope: Scope,
    ) -> Result<Report, Error> {
        let ListedMailbox {
            name,
            delimiter,
            selectable: _,
            special_use,
        } = mailbox;

        let selected = self.client.examine(name).await?;
        let window = match scope {
            Scope::Since(since) => self.client.search_since(since).await?,
            Scope::All => self.client.search_all().await?,
        };

        let Known {
            mailbox: previous,
            entries: known,
            highest,
        } = block_in_place(|| self.known_entries(name, selected.uid_validity, scope, &window))?;

        let mut report = Report {
            mailboxes: 1,
            ..Report::default()
        };

        let mut retained = Vec::new();
        let mut outside = Vec::new();
        for uid in known.keys() {
            match window.contains(uid) {
                true => retained.push(*uid),
                false => outside.push(*uid),
            }
        }

        let departed = match scope {
            Scope::Since(_) => {
                let present = self.client.search_uids(&outside).await?;
                let mut departed = Vec::new();
                for uid in outside {
                    if !present.contains(&uid) {
                        departed.push(uid);
                    }
                }
                departed
            }
            Scope::All => outside,
        };

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

        match scope {
            Scope::Since(_) => {
                let mut uids = Vec::new();
                for uid in downloads.keys() {
                    uids.push(*uid);
                }

                for chunk in uids.chunks(SOURCE_BATCH) {
                    let fetched = self.client.fetch_sources(chunk).await?;
                    report.downloaded +=
                        block_in_place(|| self.store_sources(fetched, &downloads, name))?;
                }
            }
            Scope::All => {}
        }

        let (previous_modseq, previous_start) = match previous {
            Some(Mailbox {
                name: _,
                uid_validity: _,
                highest_modseq,
                window_start,
                special_use: _,
                delimiter: _,
            }) => (highest_modseq, window_start),
            None => (None, None),
        };

        let refresh = match (previous_modseq, selected.highest_modseq) {
            (Some(previous), Some(current)) if previous == current => FlagRefresh::Unchanged,
            (Some(previous), Some(_)) => FlagRefresh::ChangedSince(previous),
            (Some(_), None) | (None, _) => FlagRefresh::All,
        };

        report.updated = match refresh {
            FlagRefresh::Unchanged => 0,
            FlagRefresh::ChangedSince(modseq) => {
                self.refresh_changed_flags(name, highest, modseq).await?
            }
            FlagRefresh::All => self.refresh_flags(name, &retained).await?,
        };

        let window_start = match (scope, window.first()) {
            (Scope::Since(_), Some(first)) => Some(*first),
            (Scope::Since(_), None) => selected.uid_next.or(previous_start),
            (Scope::All, _) => previous_start,
        };

        let stored = Mailbox {
            name: name.clone(),
            uid_validity: selected.uid_validity,
            highest_modseq: selected.highest_modseq,
            window_start,
            special_use: *special_use,
            delimiter: delimiter.clone(),
        };
        block_in_place(|| self.save_mailbox(&stored))?;

        Ok(report)
    }

    fn save_mailbox(&self, mailbox: &Mailbox) -> Result<(), StoreError> {
        let writer = self.store.writer()?;
        writer
            .table::<Mailbox>()?
            .insert(mailbox.name.as_str(), mailbox)?;
        writer.commit()
    }

    /// Returns the stored state of a mailbox, and the entries that synchronizing it looks at
    ///
    /// When synchronizing recent messages, these are the entries from the start of the previous
    /// window or the current window, whichever is lower. Entries with lower UIDs belong to
    /// messages that arrived before both windows, which are left alone.
    ///
    /// If the mailbox has a new UIDVALIDITY, its entries are removed and its state is ignored. The
    /// messages they referred to are kept even if no other entry refers to them, since they may
    /// still be in the mailbox with a new UID, which [`Sync::link()`] finds.
    fn known_entries(
        &self,
        mailbox: &MailboxName,
        uid_validity: u32,
        scope: Scope,
        window: &BTreeSet<u32>,
    ) -> Result<Known, StoreError> {
        let writer = self.store.writer()?;
        let stored = writer
            .table::<Mailbox>()?
            .get(mailbox.as_str())?
            .map(|stored| stored.value());

        let name = mailbox.as_str();
        let mut entries = writer.table::<Entry>()?;
        let previous = match stored {
            Some(stored) if stored.uid_validity == uid_validity => Some(stored),
            Some(_) | None => {
                let mut index = writer.table::<MessageEntry>()?;
                remove_mailbox_entries(&mut entries, &mut index, name)?;
                None
            }
        };

        let start = match scope {
            Scope::Since(_) => {
                let previous_start = previous.as_ref().and_then(|mailbox| mailbox.window_start);
                match (previous_start, window.first()) {
                    (Some(previous), Some(first)) => Some(previous.min(*first)),
                    (Some(previous), None) => Some(previous),
                    (None, Some(first)) => Some(*first),
                    (None, None) => None,
                }
            }
            Scope::All => Some(0),
        };

        let mut known = BTreeMap::new();
        if let Some(start) = start {
            for row in entries.range((name, start)..=(name, u32::MAX))? {
                let (_, entry) = row?;
                let Entry {
                    mailbox: _,
                    uid,
                    message,
                } = entry.value();
                known.insert(uid, message);
            }
        }

        let highest = match entries.range((name, 0)..=(name, u32::MAX))?.next_back() {
            Some(row) => {
                let (key, _) = row?;
                let (_, uid) = key.value();
                Some(uid)
            }
            None => None,
        };

        drop(entries);
        writer.commit()?;
        Ok(Known {
            mailbox: previous,
            entries: known,
            highest,
        })
    }

    /// Updates the stored flags of messages that changed after `modseq`, returning the number of
    /// messages that changed
    ///
    /// The flags are fetched for all UIDs up to the highest UID with an entry, including messages
    /// outside the window, but the server only returns the messages that changed.
    async fn refresh_changed_flags(
        &mut self,
        mailbox: &MailboxName,
        highest: Option<u32>,
        modseq: u64,
    ) -> Result<usize, Error> {
        let Some(highest) = highest else {
            return Ok(0);
        };

        let fetched = self.client.fetch_changed_flags(1..=highest, modseq).await?;
        Ok(block_in_place(|| self.update_flags(mailbox, fetched))?)
    }

    /// Updates the stored flags of the messages with the given UIDs, returning the number of
    /// messages that changed
    async fn refresh_flags(&mut self, mailbox: &MailboxName, uids: &[u32]) -> Result<usize, Error> {
        let mut updated = 0;
        for chunk in uids.chunks(FLAG_BATCH) {
            let fetched = self.client.fetch_flags(chunk).await?;
            updated += block_in_place(|| self.update_flags(mailbox, fetched))?;
        }

        Ok(updated)
    }

    /// Removes the entries for UIDs that are no longer in a mailbox
    fn remove_entries(&mut self, mailbox: &MailboxName, uids: &[u32]) -> Result<(), StoreError> {
        let writer = self.store.writer()?;
        let mut entries = writer.table::<Entry>()?;
        let mut index = writer.table::<MessageEntry>()?;
        for uid in uids {
            let Some(entry) = entries.remove((mailbox.as_str(), *uid))? else {
                continue;
            };

            let Entry {
                mailbox: _,
                uid: _,
                message,
            } = entry.value();
            index.remove((message, mailbox.as_str(), *uid))?;
            self.unlinked.insert(message);
        }

        drop((entries, index));
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
    ) -> Result<usize, StoreError> {
        let writer = self.store.writer()?;
        let message_ids = writer.table::<MessageKey>()?;
        let mut metadata = writer.table::<MessageData>()?;
        let mut entries = writer.table::<Entry>()?;
        let mut index = writer.table::<MessageEntry>()?;
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
                    insert_entry(&mut entries, &mut index, mailbox, uid, key)?;
                    linked += 1;
                }
                None => {
                    downloads.insert(uid, flags);
                }
            }
        }

        drop((message_ids, metadata, entries, index));
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
    ) -> Result<usize, StoreError> {
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
        let mut index = writer.table::<MessageEntry>()?;
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

            insert_entry(&mut entries, &mut index, mailbox, *uid, key)?;
        }

        drop((message_ids, metadata, contents, sources, entries, index));
        writer.commit()?;
        Ok(messages.len())
    }

    /// Stores the flags of fetched messages that have an entry, returning how many messages
    /// changed
    fn update_flags(
        &mut self,
        mailbox: &MailboxName,
        fetched: Vec<Fetched>,
    ) -> Result<usize, StoreError> {
        if fetched.is_empty() {
            return Ok(0);
        }

        let writer = self.store.writer()?;
        let entries = writer.table::<Entry>()?;
        let mut metadata = writer.table::<MessageData>()?;
        let mut updated = 0;
        for Fetched {
            uid,
            flags,
            internal_date: _,
            body: _,
        } in fetched
        {
            let Some(flags) = flags else {
                continue;
            };

            let Some(entry) = entries.get((mailbox.as_str(), uid))? else {
                continue;
            };

            let Entry {
                mailbox: _,
                uid: _,
                message,
            } = entry.value();
            if observe_flags(&mut self.observed, &mut metadata, message, flags, mailbox)? {
                updated += 1;
            }
        }

        drop((entries, metadata));
        writer.commit()?;
        Ok(updated)
    }

    /// Removes mailboxes that are no longer listed, and messages that left their last mailbox
    ///
    /// Only messages that lost an entry are removed, so messages that never had one, like messages
    /// imported from an archive that aren't on the server, are kept. Returns the number of
    /// messages removed.
    fn remove_stale(&mut self, listed: &[MailboxName]) -> Result<usize, StoreError> {
        let writer = self.store.writer()?;
        let mut mailboxes = writer.table::<Mailbox>()?;
        let mut unlisted = Vec::new();
        for row in mailboxes.iter()? {
            let (name, _) = row?;
            let name = name.value();
            if !is_listed(listed, name) {
                unlisted.push(name.to_owned());
            }
        }

        let mut entries = writer.table::<Entry>()?;
        let mut index = writer.table::<MessageEntry>()?;
        for name in &unlisted {
            mailboxes.remove(name.as_str())?;
            for message in remove_mailbox_entries(&mut entries, &mut index, name)? {
                self.unlinked.insert(message);
            }
        }

        let mut metadata = writer.table::<MessageData>()?;
        let mut contents = writer.table::<MessageContents>()?;
        let mut sources = writer.table::<MessageSource>()?;
        let mut message_ids = writer.table::<MessageKey>()?;
        let mut removed = 0;
        for message in self.unlinked.drain() {
            if let Some(row) = index
                .range((message, "", 0)..(message.next(), "", 0))?
                .next()
            {
                row?;
                continue;
            }

            let Some(data) = metadata.remove(message)? else {
                continue;
            };

            let MessageData { message_id, .. } = data.value();
            contents.remove(message)?;
            sources.remove(message)?;
            if let Some(message_id) = message_id {
                message_ids.remove(message_id.as_str())?;
            }
            removed += 1;
        }

        drop((
            mailboxes,
            entries,
            index,
            metadata,
            contents,
            sources,
            message_ids,
        ));
        writer.commit()?;
        Ok(removed)
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
) -> Result<bool, StoreError> {
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
    index: &mut WriteTable<'_, MessageEntry>,
    mailbox: &MailboxName,
    uid: u32,
    message: MessageKey,
) -> Result<(), StoreError> {
    let entry = Entry {
        mailbox: mailbox.clone(),
        uid,
        message,
    };

    entries.insert((mailbox.as_str(), uid), &entry)?;
    index.insert((message, mailbox.as_str(), uid), ())
}

/// Removes all entries of a mailbox, returning the keys of the messages they referred to
fn remove_mailbox_entries(
    entries: &mut WriteTable<'_, Entry>,
    index: &mut WriteTable<'_, MessageEntry>,
    mailbox: &str,
) -> Result<Vec<MessageKey>, StoreError> {
    let mut removed = Vec::new();
    entries.retain_in(
        (mailbox, 0)..=(mailbox, u32::MAX),
        |_,
         Entry {
             mailbox: _,
             uid,
             message,
         }| {
            removed.push((uid, message));
            false
        },
    )?;

    let mut messages = Vec::new();
    for (uid, message) in removed {
        index.remove((message, mailbox, uid))?;
        messages.push(message);
    }

    Ok(messages)
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

/// The stored state of a mailbox at the start of its synchronization
struct Known {
    /// The state of the mailbox, unless it is new or has a new UIDVALIDITY
    mailbox: Option<Mailbox>,
    /// The entries that the synchronization looks at
    entries: Entries,
    /// The highest UID with an entry
    highest: Option<u32>,
}

/// The flags seen for each message during a synchronization, and the mailbox they were seen in
type ObservedFlags = HashMap<MessageKey, (Flags, MailboxName)>;

/// Which messages of each mailbox to synchronize
#[derive(Clone, Copy)]
enum Scope {
    /// Messages that arrived on or after the date, which are downloaded if they aren't stored
    Since(Date),
    /// All messages, which are only linked if they are stored
    All,
}

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
    Store(#[from] StoreError),
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
        import_mbox,
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

        let (report, commands) = run(&store, vec![inbox, all, sent.clone()], Operation::Sync).await;
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

        let (report, commands) = run(&store, vec![inbox, all, sent], Operation::Sync).await;
        assert_eq!(report.downloaded, 0);
        assert_eq!(report.updated, 1);
        assert_eq!(report.departed, 1);
        assert_eq!(report.removed, 0);
        assert!(commands.contains(&"UID SEARCH UID 1".to_owned()));
        assert!(commands.contains(&"UID FETCH 1:2 (UID FLAGS) (CHANGEDSINCE 7)".to_owned()));
        assert!(commands.contains(&"UID FETCH 1:13 (UID FLAGS) (CHANGEDSINCE 13)".to_owned()));
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
        let (report, _) = run(&store, vec![all], Operation::Sync).await;
        assert_eq!(report.departed, 2);
        assert_eq!(report.removed, 2);

        let reader = store.reader().unwrap();
        let mut subjects = Vec::new();
        for row in reader.table::<MessageData>().unwrap().iter().unwrap() {
            let (_, data) = row.unwrap();
            let MessageData { subject, .. } = data.value();
            subjects.push(subject);
        }
        assert_eq!(subjects, ["Lunch"]);
        drop(reader);

        let all = FakeMailbox::new("[Gmail]/All Mail", "\\All \\HasNoChildren", 16);
        let (report, commands) = run(&store, vec![all], Operation::Sync).await;
        assert_eq!((report.departed, report.removed), (1, 1));
        assert!(commands.contains(&"UID SEARCH UID 12".to_owned()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn links_imported_messages() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::new(directory.path().join("encove.redb"));

        let mut mbox = String::new();
        for source in [PLANS, REPLY] {
            mbox.push_str("From 1@xxx Thu Oct 09 10:00:00 +0000 2025\n");
            mbox.push_str(source);
            mbox.push('\n');
        }
        import_mbox(&store, mbox.as_bytes()).unwrap();

        let inbox = FakeMailbox::new("INBOX", "\\HasNoChildren", 7).archived(1, "", 5, PLANS);
        let all = FakeMailbox::new("[Gmail]/All Mail", "\\All \\HasNoChildren", 13)
            .archived(11, "", 11, PLANS)
            .message(12, "", 12, LUNCH)
            .archived(13, "\\Seen", 13, REPLY);
        let sent = FakeMailbox::new("[Gmail]/Sent Mail", "\\Sent \\HasNoChildren", 21)
            .archived(21, "\\Seen", 21, REPLY);

        let (report, commands) = run(
            &store,
            vec![inbox.clone(), all, sent.clone()],
            Operation::Link,
        )
        .await;
        assert_eq!(
            (report.mailboxes, report.linked, report.downloaded),
            (3, 4, 0)
        );
        assert!(commands.contains(&"UID SEARCH ALL".to_owned()));
        for command in &commands {
            assert!(!command.contains("BODY.PEEK[]"), "{command}");
        }

        let all = FakeMailbox::new("[Gmail]/All Mail", "\\All \\HasNoChildren", 14)
            .archived(11, "\\Seen", 14, PLANS)
            .message(12, "", 12, LUNCH)
            .archived(13, "\\Seen", 13, REPLY);
        let (report, commands) = run(&store, vec![inbox, all, sent], Operation::Sync).await;
        assert_eq!(
            (
                report.departed,
                report.downloaded,
                report.updated,
                report.removed
            ),
            (0, 1, 1, 0)
        );
        assert!(commands.contains(&"UID SEARCH UID 13".to_owned()));
        assert!(commands.contains(&"UID FETCH 1:13 (UID FLAGS) (CHANGEDSINCE 13)".to_owned()));

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
            let read = match flags.contains(Flags::SEEN) {
                true => "read",
                false => "unread",
            };
            stored.push(format!("{} {uid}: {subject} ({read})", mailbox.as_str()));
        }

        assert_eq!(
            stored,
            [
                "INBOX 1: Plans for Thursday (read)",
                "[Gmail]/All Mail 11: Plans for Thursday (read)",
                "[Gmail]/All Mail 12: Lunch (unread)",
                "[Gmail]/All Mail 13: Re: Plans for Thursday (read)",
                "[Gmail]/Sent Mail 21: Re: Plans for Thursday (read)",
            ]
        );
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
            observed: HashMap::new(),
            unlinked: HashSet::new(),
        };

        let start = Instant::now();
        drop(sync);
        assert!(start.elapsed() < LOGOUT_TIMEOUT * 2);
        drop(server);
    }

    async fn run(
        store: &Store,
        mailboxes: Vec<FakeMailbox>,
        operation: Operation,
    ) -> (Report, Vec<String>) {
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
            observed: HashMap::new(),
            unlinked: HashSet::new(),
        };
        let report = match operation {
            Operation::Sync => sync.sync(Date::constant(2026, 10, 1)).await.unwrap(),
            Operation::Link => sync.link().await.unwrap(),
        };
        drop(sync);
        assert_indexed(store);
        (report, server.await.unwrap())
    }

    /// Checks that the entries indexed by message are exactly the stored entries
    fn assert_indexed(store: &Store) {
        let reader = store.reader().unwrap();
        let mut entries = Vec::new();
        for row in reader.table::<Entry>().unwrap().iter().unwrap() {
            let (_, entry) = row.unwrap();
            let Entry {
                mailbox,
                uid,
                message,
            } = entry.value();
            entries.push((message, mailbox.as_str().to_owned(), uid));
        }

        let mut indexed = Vec::new();
        for row in reader.table::<MessageEntry>().unwrap().iter().unwrap() {
            let (key, _) = row.unwrap();
            let (message, mailbox, uid) = key.value();
            indexed.push((message, mailbox.to_owned(), uid));
        }

        entries.sort();
        assert_eq!(entries, indexed);
    }

    enum Operation {
        Sync,
        Link,
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
                let mut uid_next = 1;
                for message in &mailbox.messages {
                    uid_next = uid_next.max(message.uid + 1);
                }

                write!(
                    response,
                    "* FLAGS (\\Seen)\r\n* OK [UIDVALIDITY 1] UIDs valid\r\n\
                     * OK [UIDNEXT {uid_next}] Predicted next UID\r\n\
                     * OK [HIGHESTMODSEQ {}] Highest\r\n{tag} OK [READ-ONLY] done\r\n",
                    mailbox.modseq
                )
                .unwrap();
                selected = Some(mailbox);
            } else if let Some(criteria) = command.strip_prefix("UID SEARCH ") {
                response.push_str("* SEARCH");
                for message in &selected.unwrap().messages {
                    let found = match criteria.strip_prefix("UID ") {
                        Some(set) => parse_set(set).contains(&message.uid),
                        None => match (criteria, message.arrival) {
                            ("ALL", _) | ("SINCE 1-Oct-2026", Arrival::Recent) => true,
                            ("SINCE 1-Oct-2026", Arrival::Archived) => false,
                            _ => panic!("unexpected search: {criteria}"),
                        },
                    };

                    if found {
                        write!(response, " {}", message.uid).unwrap();
                    }
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
                        arrival: _,
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

        /// Adds a message that arrived since the start of the synchronized window
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
                arrival: Arrival::Recent,
            });
            self
        }

        /// Adds a message that arrived before the start of the synchronized window
        fn archived(
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
                arrival: Arrival::Archived,
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
        arrival: Arrival,
    }

    #[derive(Clone, Copy)]
    enum Arrival {
        Recent,
        Archived,
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
