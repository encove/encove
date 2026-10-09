//! Domain models for the mailbox user interface
//!
//! These types are built from the synchronized mail in the store and contain everything
//! needed to present it. The displayed models implement [`View`] to build their HTML representation.

use core::fmt::{self, Write};
use core::sync::atomic::{AtomicUsize, Ordering};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use jiff::Timestamp;
use jiff::tz::TimeZone;
use mail::{
    Address, Attachment, Entry, Flags, MailboxName, MessageContents, MessageData, MessageKey,
    ReadState, SpecialUse, ThreadMessage,
};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use serde::Deserialize;
use store::Reader;

use crate::html::{
    A, Article, Body as BodyElement, Details, Div, Document, FlowContent, GlobalAttributes, H1, H2,
    Head, Header, Html, Li, Link, Main, Meta, Nav, P, Section, Span, Strong,
    Summary as SummaryElement, Time, Title, Ul, Unescaped, View,
};

/// The three-pane mailbox view: labels, threads with the selected label, and the selected thread
pub(crate) struct Mailbox {
    /// The current position in the mailbox
    pub location: Location,
    /// The labels shown in the sidebar
    pub labels: LabelList,
    /// The threads with the selected label
    pub threads: ThreadList,
    /// The selected thread, if any
    pub thread: Option<Thread>,
}

impl View for Mailbox {
    type Output<'a> = Document<'a>;

    fn view(&self) -> Document<'_> {
        let Self {
            location,
            labels,
            threads,
            thread,
        } = self;

        let (title, pane) = match thread {
            Some(thread) => {
                let mut pane = Main::new().class("thread-pane");
                let blocked = thread.blocked_images();
                if blocked > 0 && location.images == RemoteImages::Blocked {
                    pane = pane.child(images_banner(blocked, location));
                }

                (thread.subject.as_str(), pane.child(thread.view()))
            }
            None => (
                labels.selected_name(),
                Main::new()
                    .class("thread-pane")
                    .class("empty")
                    .child(P::new().child("Select a conversation to read it.")),
            ),
        };

        let panes = Div::new()
            .class("panes")
            .child(labels.view())
            .child(threads.view())
            .child(pane);
        page(title, panes)
    }
}

fn images_banner(blocked: usize, location: &Location) -> Div<'static> {
    let images = match blocked {
        1 => "1 remote image",
        _ => "remote images",
    };

    Div::new()
        .class("banner")
        .child(format!("Blocked {images} to prevent tracking. "))
        .child(
            A::new()
                .href(location.with_images(RemoteImages::Shown).href())
                .child("Show images"),
        )
}

/// A position in the mailbox: a label, a page of its threads, and possibly a selected thread
#[derive(Clone, Debug)]
pub(crate) struct Location {
    /// The mailbox of the selected label
    pub label: MailboxName,
    /// The page of threads, starting at 0 for the newest
    pub page: usize,
    /// The selected thread
    pub thread: Option<ThreadId>,
    /// Whether remote images in the selected thread are loaded
    pub images: RemoteImages,
}

impl Location {
    /// Creates a location pointing at the first page of the given label
    pub(crate) fn new(label: MailboxName) -> Self {
        Self {
            label,
            page: 0,
            thread: None,
            images: RemoteImages::Blocked,
        }
    }

    /// Returns this location with the given thread selected
    pub(crate) fn with_thread(&self, thread: ThreadId) -> Self {
        Self {
            label: self.label.clone(),
            page: self.page,
            thread: Some(thread),
            images: RemoteImages::Blocked,
        }
    }

    /// Returns the given page of this location's label, without a selected thread
    pub(crate) fn with_page(&self, page: usize) -> Self {
        Self {
            label: self.label.clone(),
            page,
            thread: None,
            images: RemoteImages::Blocked,
        }
    }

    /// Returns this location with remote images shown
    pub(crate) fn with_images(&self, images: RemoteImages) -> Self {
        Self {
            images,
            ..self.clone()
        }
    }

    /// Returns the path and query for this location
    ///
    /// The format must match the routes defined in [`crate::server`].
    pub(crate) fn href(&self) -> String {
        let Self {
            label,
            page,
            thread,
            images,
        } = self;

        let mut href = format!("/label/{}", encode(label.as_str()));
        if let Some(thread) = thread {
            write!(href, "/thread/{thread}").unwrap();
        }

        let mut separator = '?';
        if *page > 0 {
            write!(href, "{separator}page={page}").unwrap();
            separator = '&';
        }

        match images {
            RemoteImages::Blocked => {}
            RemoteImages::Shown => write!(href, "{separator}images=shown").unwrap(),
        }

        href
    }
}

fn encode(value: &str) -> impl fmt::Display {
    utf8_percent_encode(value, UNRESERVED)
}

const UNRESERVED: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// Whether remote images in messages are loaded
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum RemoteImages {
    /// Remote images are removed, so loading a message can't be tracked
    #[default]
    Blocked,
    /// Remote images are loaded
    Shown,
}

/// The synchronized mail, with messages grouped into threads
pub(crate) struct MailIndex {
    mailboxes: Vec<mail::Mailbox>,
    messages: BTreeMap<MessageKey, IndexedMessage>,
    /// Threads ordered by their latest message, newest first
    threads: Vec<IndexedThread>,
}

impl MailIndex {
    /// Reads the stored messages and groups them into threads, using their `References` and
    /// `In-Reply-To` headers
    pub(crate) fn new(reader: &Reader<'_>) -> Result<Self, store::StoreError> {
        let mut mailboxes = Vec::new();
        for row in reader.table::<mail::Mailbox>()?.iter()? {
            let (_, mailbox) = row?;
            mailboxes.push(mailbox.value());
        }

        let mut entries = Vec::new();
        for row in reader.table::<Entry>()?.iter()? {
            let (_, entry) = row?;
            entries.push(entry.value());
        }

        let mut metadata = BTreeMap::new();
        for row in reader.table::<MessageData>()?.iter()? {
            let (key, value) = row?;
            metadata.insert(key.value(), value.value());
        }

        Ok(Self::build(mailboxes, entries, metadata))
    }

    fn build(
        mailboxes: Vec<mail::Mailbox>,
        entries: Vec<Entry>,
        summaries: BTreeMap<MessageKey, MessageData>,
    ) -> Self {
        let mut placements: HashMap<MessageKey, Vec<MailboxName>> = HashMap::new();
        for Entry {
            mailbox,
            uid: _,
            message,
        } in entries
        {
            placements.entry(message).or_default().push(mailbox);
        }

        let groups = group_threads(&summaries);
        let mut messages = BTreeMap::new();
        for (key, summary) in summaries {
            let Some(mailboxes) = placements.remove(&key) else {
                continue;
            };

            let read = match summary.flags.contains(Flags::SEEN) {
                true => ReadState::Read,
                false => ReadState::Unread,
            };

            messages.insert(
                key,
                IndexedMessage {
                    summary,
                    mailboxes,
                    read,
                },
            );
        }

        let mut threads = Vec::new();
        for group in groups {
            let mut keys = Vec::new();
            for key in group {
                if messages.contains_key(&key) {
                    keys.push(key);
                }
            }

            keys.sort_by_key(|key| (messages[key].summary.received, *key));
            let (Some(first), Some(last)) = (keys.first(), keys.last()) else {
                continue;
            };

            let mut id = *first;
            for key in &keys {
                id = id.min(*key);
            }

            threads.push(IndexedThread {
                id: ThreadId(id.get()),
                latest: messages[last].summary.received,
                messages: keys,
            });
        }

        threads.sort_by(|a, b| b.latest.cmp(&a.latest).then(b.id.0.cmp(&a.id.0)));
        Self {
            mailboxes,
            messages,
            threads,
        }
    }

    /// Returns the labels for the sidebar, with the number of unread threads in each
    pub(crate) fn labels(&self, selected: &MailboxName) -> LabelList {
        let mut unread = HashMap::new();
        for thread in &self.threads {
            let mut counted = Vec::new();
            for key in &thread.messages {
                let message = &self.messages[key];
                if message.read == ReadState::Read {
                    continue;
                }

                for mailbox in &message.mailboxes {
                    if !counted.contains(&mailbox) {
                        counted.push(mailbox);
                    }
                }
            }

            for mailbox in counted {
                *unread.entry(mailbox).or_insert(0) += 1;
            }
        }

        let mut mail = Vec::new();
        let mut user = Vec::new();
        for mailbox in &self.mailboxes {
            let label = Label::new(mailbox, unread.get(&mailbox.name).copied().unwrap_or(0));
            match label.kind {
                LabelKind::Inbox | LabelKind::Special(_) => mail.push(label),
                LabelKind::User => user.push(label),
            }
        }

        mail.sort_by_key(|label| label.kind);
        user.sort_by_key(|label| label.path.to_lowercase());
        LabelList {
            mail,
            user,
            selected: selected.clone(),
        }
    }

    /// Returns a page of the threads with messages in the location's label
    pub(crate) fn thread_list(&self, location: &Location, name: &str) -> ThreadList {
        let mut matching = Vec::new();
        for thread in &self.threads {
            if self.has_label(thread, &location.label) {
                matching.push(thread);
            }
        }

        let start = location.page.saturating_mul(PAGE_SIZE).min(matching.len());
        let end = start.saturating_add(PAGE_SIZE).min(matching.len());
        let mut threads = Vec::new();
        for thread in &matching[start..end] {
            threads.push(self.summarize(thread));
        }

        ThreadList {
            name: name.to_owned(),
            location: location.clone(),
            threads,
            older: end < matching.len(),
        }
    }

    /// Returns the messages in a thread, oldest first, to load for [`Thread::new()`]
    pub(crate) fn thread_messages(&self, id: ThreadId) -> Option<Vec<ThreadMessage<'_>>> {
        for thread in &self.threads {
            if thread.id != id {
                continue;
            }

            let mut messages = Vec::new();
            for key in &thread.messages {
                let message = &self.messages[key];
                messages.push(ThreadMessage {
                    key: *key,
                    metadata: &message.summary,
                    read: message.read,
                });
            }
            return Some(messages);
        }

        None
    }

    fn summarize(&self, thread: &IndexedThread) -> ThreadSummary {
        let mut senders = Vec::new();
        let mut read = ReadState::Read;
        for key in &thread.messages {
            let message = &self.messages[key];
            let sender = match &message.summary.from {
                Some(from) => from.display_name().to_owned(),
                None => "(unknown sender)".to_owned(),
            };

            if !senders.contains(&sender) {
                senders.push(sender);
            }

            if message.read == ReadState::Unread {
                read = ReadState::Unread;
            }
        }

        let first = &self.messages[&thread.messages[0]].summary;
        let last = &self.messages[&thread.messages[thread.messages.len() - 1]].summary;
        ThreadSummary {
            id: thread.id,
            subject: first.subject.clone(),
            senders,
            message_count: thread.messages.len(),
            snippet: last.snippet.clone(),
            date: thread.latest,
            read,
        }
    }

    fn has_label(&self, thread: &IndexedThread, label: &MailboxName) -> bool {
        for key in &thread.messages {
            if self.messages[key].mailboxes.contains(label) {
                return true;
            }
        }

        false
    }
}

struct IndexedMessage {
    summary: MessageData,
    mailboxes: Vec<MailboxName>,
    read: ReadState,
}

struct IndexedThread {
    id: ThreadId,
    /// The keys of the messages, oldest first
    messages: Vec<MessageKey>,
    latest: Timestamp,
}

/// Groups messages that refer to each other, directly or through common ancestors
fn group_threads(summaries: &BTreeMap<MessageKey, MessageData>) -> Vec<Vec<MessageKey>> {
    let mut forest = Forest::default();
    let mut ids = HashMap::new();
    let mut nodes = Vec::new();
    for (key, summary) in summaries {
        let node = match &summary.message_id {
            Some(message_id) => forest.node(&mut ids, message_id),
            None => forest.add(),
        };

        for parent in &summary.parents {
            let parent = forest.node(&mut ids, parent);
            forest.union(node, parent);
        }

        nodes.push((*key, node));
    }

    let mut groups: BTreeMap<usize, Vec<MessageKey>> = BTreeMap::new();
    for (key, node) in nodes {
        groups.entry(forest.find(node)).or_default().push(key);
    }

    let mut threads = Vec::new();
    for (_, group) in groups {
        threads.push(group);
    }
    threads
}

/// A union-find structure over `Message-ID`s
#[derive(Default)]
struct Forest {
    parents: Vec<usize>,
}

impl Forest {
    fn node<'a>(&mut self, ids: &mut HashMap<&'a str, usize>, message_id: &'a str) -> usize {
        if let Some(node) = ids.get(message_id) {
            return *node;
        }

        let node = self.add();
        ids.insert(message_id, node);
        node
    }

    fn union(&mut self, a: usize, b: usize) {
        let (a, b) = (self.find(a), self.find(b));
        if a != b {
            self.parents[a.max(b)] = a.min(b);
        }
    }

    fn find(&mut self, mut node: usize) -> usize {
        while self.parents[node] != node {
            self.parents[node] = self.parents[self.parents[node]];
            node = self.parents[node];
        }
        node
    }

    fn add(&mut self) -> usize {
        self.parents.push(self.parents.len());
        self.parents.len() - 1
    }
}

/// The labels shown in the sidebar, in groups
pub(crate) struct LabelList {
    /// The inbox and mailboxes with a special use, like Sent and Trash
    pub mail: Vec<Label>,
    /// Other mailboxes, usually created by the user
    pub user: Vec<Label>,
    /// The mailbox of the selected label
    pub selected: MailboxName,
}

impl LabelList {
    /// Returns the display name of the selected label
    pub(crate) fn selected_name(&self) -> &str {
        for group in [&self.mail, &self.user] {
            for label in group {
                if label.mailbox == self.selected {
                    return &label.path;
                }
            }
        }

        self.selected.as_str()
    }
}

impl View for LabelList {
    type Output<'a> = Nav<'a>;

    fn view(&self) -> Nav<'_> {
        let Self {
            mail,
            user,
            selected,
        } = self;

        let mut nav = Nav::new().class("labels").child(H1::new().child("encove"));
        for (heading, labels) in [(None, mail), (Some("Labels"), user)] {
            if labels.is_empty() {
                continue;
            }

            if let Some(heading) = heading {
                nav = nav.child(H2::new().child(heading));
            }

            let mut list = Ul::new();
            for label in labels {
                let mut item = Li::new();
                let mut link = label
                    .view()
                    .href(Location::new(label.mailbox.clone()).href());
                if label.mailbox == *selected {
                    item = item.class("selected");
                    link = link.aria_current("page");
                }

                list = list.child(item.child(link));
            }

            nav = nav.child(list);
        }

        nav
    }
}

/// A label in the sidebar, backed by an IMAP mailbox
pub(crate) struct Label {
    /// The mailbox
    pub mailbox: MailboxName,
    /// The name to display: the last component of the path for nested mailboxes
    pub name: String,
    /// The full decoded name of the mailbox, or just the display name for a special-use mailbox
    pub path: String,
    /// How deeply the label is nested below other labels
    pub depth: usize,
    /// The number of unread threads to display, or 0 for none
    pub unread: u32,
    /// What kind of label this is
    pub kind: LabelKind,
}

impl Label {
    fn new(mailbox: &mail::Mailbox, unread: u32) -> Self {
        let mail::Mailbox {
            name,
            uid_validity: _,
            highest_modseq: _,
            window_start: _,
            special_use,
            delimiter,
        } = mailbox;

        let kind = match (name.is_inbox(), special_use) {
            (true, _) => LabelKind::Inbox,
            (false, Some(special_use)) => LabelKind::Special(*special_use),
            (false, None) => LabelKind::User,
        };

        let path = match kind {
            LabelKind::Inbox => "Inbox".to_owned(),
            LabelKind::Special(_) | LabelKind::User => name.decoded(),
        };

        let display = match delimiter.as_deref().and_then(|d| path.rsplit_once(d)) {
            Some((_, last)) => last.to_owned(),
            None => path.clone(),
        };

        let depth = match (kind, delimiter.as_deref()) {
            (LabelKind::User, Some(delimiter)) => path.matches(delimiter).count(),
            (LabelKind::User, None) | (LabelKind::Inbox | LabelKind::Special(_), _) => 0,
        };

        let path = match kind {
            LabelKind::Special(_) => display.clone(),
            LabelKind::Inbox | LabelKind::User => path,
        };

        Self {
            mailbox: name.clone(),
            name: display,
            path,
            depth,
            unread: match kind.counts_unread() {
                true => unread,
                false => 0,
            },
            kind,
        }
    }
}

impl View for Label {
    type Output<'a> = A<'a>;

    fn view(&self) -> A<'_> {
        let Self {
            mailbox: _,
            name,
            path,
            depth,
            unread,
            kind: _,
        } = self;

        let mut link = A::new().child(Span::new().class("name").child(name));
        if *depth > 0 {
            link = link.title(path).style(format!("--depth: {depth}"));
        }

        if *unread > 0 {
            link = link.child(Span::new().class("count").child(unread.to_string()));
        }

        link
    }
}

/// What a label is used for, in display order
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum LabelKind {
    /// The inbox
    Inbox,
    /// A mailbox with a special use, like Sent or Trash
    Special(SpecialUse),
    /// Any other mailbox
    User,
}

impl LabelKind {
    fn counts_unread(self) -> bool {
        match self {
            Self::Inbox | Self::Special(SpecialUse::Junk) | Self::User => true,
            Self::Special(
                SpecialUse::Flagged
                | SpecialUse::Important
                | SpecialUse::Sent
                | SpecialUse::Drafts
                | SpecialUse::All
                | SpecialUse::Archive
                | SpecialUse::Trash,
            ) => false,
        }
    }
}

/// A page of threads with the selected label
pub(crate) struct ThreadList {
    /// The display name of the label
    pub name: String,
    /// The current position in the mailbox, identifying the label, page and selected thread
    pub location: Location,
    /// The threads on this page, newest first
    pub threads: Vec<ThreadSummary>,
    /// Whether there are older threads on later pages
    pub older: bool,
}

impl View for ThreadList {
    type Output<'a> = Section<'a>;

    fn view(&self) -> Section<'_> {
        let Self {
            name,
            location,
            threads,
            older,
        } = self;

        let mut section = Section::new()
            .class("thread-list")
            .child(Header::new().child(H2::new().child(name)));

        if threads.is_empty() {
            section = section.child(P::new().class("empty").child("No conversations."));
        }

        let mut list = Ul::new();
        for summary in threads {
            let mut item = Li::new();
            let mut link = summary.view().href(location.with_thread(summary.id).href());
            if location.thread == Some(summary.id) {
                item = item.class("selected");
                link = link.aria_current("page");
            }

            list = list.child(item.child(link));
        }
        section = section.child(list);

        let mut pager = Nav::new().class("pager");
        if location.page > 0 {
            pager = pager.child(
                A::new()
                    .href(location.with_page(location.page - 1).href())
                    .child("« Newer"),
            );
        }

        if *older {
            pager = pager.child(
                A::new()
                    .class("older")
                    .href(location.with_page(location.page + 1).href())
                    .child("Older »"),
            );
        }

        section.child(pager)
    }
}

/// Identifies a thread by the smallest key of its messages
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub(crate) struct ThreadId(u64);

impl fmt::Display for ThreadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A thread as shown in the thread list
pub(crate) struct ThreadSummary {
    /// The thread's ID
    pub id: ThreadId,
    /// The subject of the first message
    pub subject: String,
    /// The display names of the senders, in order of their first message
    pub senders: Vec<String>,
    /// The number of messages in the thread
    pub message_count: usize,
    /// A short part of the text of the last message
    pub snippet: String,
    /// When the last message was received
    pub date: Timestamp,
    /// Whether the thread contains unread messages
    pub read: ReadState,
}

impl View for ThreadSummary {
    type Output<'a> = A<'a>;

    fn view(&self) -> A<'_> {
        let Self {
            id: _,
            subject,
            senders,
            message_count,
            snippet,
            date,
            read,
        } = self;

        let mut participants = Span::new().class("participants").child(senders.join(", "));
        if *message_count > 1 {
            participants =
                participants.child(Span::new().class("count").child(message_count.to_string()));
        }

        let mut link = A::new()
            .class("thread")
            .child(participants)
            .child(time(*date, DateStyle::Short))
            .child(Span::new().class("subject").child(subject_text(subject)))
            .child(Span::new().class("snippet").child(snippet));
        match read {
            ReadState::Read => {}
            ReadState::Unread => link = link.class("unread"),
        }

        link
    }
}

/// A thread with its full messages
pub(crate) struct Thread {
    /// The subject of the first message
    pub subject: String,
    /// The messages in the thread, oldest first
    pub messages: Vec<Message>,
}

impl Thread {
    /// Builds a thread from its stored messages, oldest first
    pub(crate) fn new(messages: Vec<LoadedMessage<'_>>, images: RemoteImages) -> Self {
        let mut subject = String::new();
        let mut built = Vec::new();
        for LoadedMessage {
            metadata,
            content,
            read,
        } in messages
        {
            if subject.is_empty() {
                subject = metadata.subject.clone();
            }

            built.push(Message::new(metadata, content, read, images));
        }

        Self {
            subject,
            messages: built,
        }
    }

    /// Returns the number of remote images that were blocked across all messages
    pub(crate) fn blocked_images(&self) -> usize {
        let mut blocked = 0;
        for message in &self.messages {
            blocked += message.blocked_images;
        }
        blocked
    }
}

impl View for Thread {
    type Output<'a> = Article<'a>;

    fn view(&self) -> Article<'_> {
        let Self { subject, messages } = self;

        let mut article = Article::new()
            .class("thread")
            .child(Header::new().child(H2::new().child(subject_text(subject))));

        let last = messages.len().saturating_sub(1);
        for (index, message) in messages.iter().enumerate() {
            let mut details = message.view();
            if index == last || message.read == ReadState::Unread {
                details = details.open();
            }

            article = article.child(details);
        }

        article
    }
}

/// A message in a thread, as loaded from the store
pub(crate) struct LoadedMessage<'a> {
    /// The stored metadata of the message
    pub metadata: &'a MessageData,
    /// The stored content of the message
    pub content: MessageContents,
    /// Whether the message has been read
    pub read: ReadState,
}

/// A single message in a thread
pub(crate) struct Message {
    /// Who sent the message
    pub from: Address,
    /// The recipients from the `To` header
    pub to: Vec<Address>,
    /// The recipients from the `Cc` header
    pub cc: Vec<Address>,
    /// The recipients from the `Bcc` header
    pub bcc: Vec<Address>,
    /// When the message was sent according to its `Date` header, or else when it arrived
    pub date: Timestamp,
    /// A short part of the message text
    pub snippet: String,
    /// Whether the message has been read
    pub read: ReadState,
    /// The message content
    pub body: Body,
    /// Files attached to the message
    pub attachments: Vec<Attachment>,
    /// The number of remote images removed from the body
    pub blocked_images: usize,
}

impl Message {
    fn new(
        metadata: &MessageData,
        content: MessageContents,
        read: ReadState,
        images: RemoteImages,
    ) -> Self {
        let MessageData {
            received,
            flags: _,
            message_id: _,
            date,
            subject: _,
            from,
            to,
            cc,
            bcc,
            snippet,
            parents: _,
        } = metadata;
        let MessageContents { body, attachments } = content;

        let from = match from {
            Some(from) => from.clone(),
            None => Address {
                name: None,
                address: "(unknown sender)".to_owned(),
            },
        };

        let (body, blocked_images) = Body::new(body, images);
        Self {
            from,
            to: to.clone(),
            cc: cc.clone(),
            bcc: bcc.clone(),
            date: date.unwrap_or(*received),
            snippet: snippet.clone(),
            read,
            body,
            attachments,
            blocked_images,
        }
    }
}

impl View for Message {
    type Output<'a> = Details<'a>;

    fn view(&self) -> Details<'_> {
        let Self {
            from,
            to,
            cc,
            bcc,
            date,
            snippet,
            read: _,
            body,
            attachments,
            blocked_images: _,
        } = self;

        let summary = SummaryElement::new()
            .child(from.view())
            .child(time(*date, DateStyle::Long))
            .child(Span::new().class("snippet").child(snippet));

        let mut details = Details::new(summary).class("message");
        if !to.is_empty() || !cc.is_empty() || !bcc.is_empty() {
            let mut recipients = Div::new().class("recipients");
            for (label, addresses) in [("To", to), ("Cc", cc), ("Bcc", bcc)] {
                if addresses.is_empty() {
                    continue;
                }

                recipients = recipients.child(
                    Div::new()
                        .child(Span::new().class("field").child(label))
                        .child(format_addresses(addresses)),
                );
            }
            details = details.child(recipients);
        }

        details = details.child(body.view());

        if !attachments.is_empty() {
            let mut list = Ul::new().class("attachments");
            for attachment in attachments {
                list = list.child(attachment.view());
            }
            details = details.child(list);
        }

        details
    }
}

impl View for Address {
    type Output<'a> = Span<'a>;

    fn view(&self) -> Span<'_> {
        let Self { name, address } = self;
        let span = Span::new().class("sender");
        match name {
            Some(name) => span.child(Strong::new().child(name)).child(" ").child(
                Span::new()
                    .class("address")
                    .child("<")
                    .child(address)
                    .child(">"),
            ),
            None => span.child(Strong::new().child(address)),
        }
    }
}

fn format_addresses(addresses: &[Address]) -> String {
    let mut formatted = String::new();
    for Address { name, address } in addresses {
        if !formatted.is_empty() {
            formatted.push_str(", ");
        }

        match name {
            Some(name) => write!(formatted, "{name} <{address}>").unwrap(),
            None => formatted.push_str(address),
        }
    }
    formatted
}

/// The displayable content of a message
pub(crate) enum Body {
    /// Sanitized HTML content
    Html(SanitizedHtml),
    /// Plain text content
    Text(String),
    /// The message has no displayable content
    Empty,
}

impl Body {
    fn new(body: mail::Body, images: RemoteImages) -> (Self, usize) {
        match body {
            mail::Body::Html(html) => {
                let (html, blocked) = SanitizedHtml::new(&html, images);
                (Self::Html(html), blocked)
            }
            mail::Body::Text(text) => (Self::Text(text), 0),
            mail::Body::Empty => (Self::Empty, 0),
        }
    }
}

impl View for Body {
    type Output<'a> = Div<'a>;

    fn view(&self) -> Div<'_> {
        let div = Div::new().class("body");
        match self {
            Self::Html(html) => div.class("html").child(Unescaped::new(html.as_str())),
            Self::Text(text) => div.class("text").child(text),
            Self::Empty => div.class("empty").child("This message has no content."),
        }
    }
}

/// HTML that has been cleaned of scripts, styles and other unsafe content
pub(crate) struct SanitizedHtml(String);

impl SanitizedHtml {
    fn new(html: &str, images: RemoteImages) -> (Self, usize) {
        let blocked = Arc::new(AtomicUsize::new(0));
        let mut builder = ammonia::Builder::default();
        builder
            .set_tag_attribute_value("a", "target", "_blank")
            .set_tag_attribute_value("img", "referrerpolicy", "no-referrer");

        match images {
            RemoteImages::Shown => {}
            RemoteImages::Blocked => {
                let blocked = blocked.clone();
                builder.attribute_filter(move |element, attribute, value| {
                    if element != "img" || attribute != "src" {
                        return Some(value.into());
                    }

                    let scheme = value.split_once(':').map(|(scheme, _)| scheme);
                    if let Some("http" | "https") = scheme.map(str::to_ascii_lowercase).as_deref() {
                        blocked.fetch_add(1, Ordering::Relaxed);
                    }
                    None
                });
            }
        }

        let html = builder.clean(html).to_string();
        (Self(html), blocked.load(Ordering::Relaxed))
    }

    /// Returns the sanitized markup
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl View for Attachment {
    type Output<'a> = Li<'a>;

    fn view(&self) -> Li<'_> {
        let Self {
            filename,
            mime_type,
            size,
        } = self;

        Li::new()
            .title(mime_type)
            .child(Span::new().class("filename").child(filename))
            .child(Span::new().class("size").child(format_size(*size)))
    }
}

/// A page showing a single message, like an error
pub(crate) struct Notice {
    /// The page title and heading
    pub title: String,
    /// The message to show
    pub message: String,
}

impl View for Notice {
    type Output<'a> = Document<'a>;

    fn view(&self) -> Document<'_> {
        let Self { title, message } = self;
        let main = Main::new()
            .class("notice")
            .child(H1::new().child(title))
            .child(P::new().child(message));
        page(title, main)
    }
}

fn page<'a>(title: &str, content: impl Into<FlowContent<'a>>) -> Document<'a> {
    let head = Head::new(Title::new(format!("{title} – Encove")))
        .child(Meta::charset("utf-8"))
        .child(Meta::name(
            "viewport",
            "width=device-width, initial-scale=1",
        ))
        .child(Meta::name("referrer", "no-referrer"))
        .child(Link::new("stylesheet", "/style.css"));

    let body = BodyElement::new().child(content);
    Document::new(Html::new(head, body).lang("en"))
}

fn subject_text(subject: &str) -> &str {
    match subject.is_empty() {
        true => "(no subject)",
        false => subject,
    }
}

fn time(timestamp: Timestamp, style: DateStyle) -> Time<'static> {
    let zone = TimeZone::system();
    let date = timestamp.to_zoned(zone.clone());
    let format = match style {
        DateStyle::Long => "%a, %b %-d, %Y, %H:%M",
        DateStyle::Short => {
            let now = Timestamp::now().to_zoned(zone);
            if date.date() == now.date() {
                "%H:%M"
            } else if date.year() == now.year() {
                "%b %-d"
            } else {
                "%Y-%m-%d"
            }
        }
    };

    Time::new(timestamp.to_string())
        .title(date.strftime("%a, %b %-d, %Y, %H:%M").to_string())
        .child(date.strftime(format).to_string())
}

enum DateStyle {
    Short,
    Long,
}

fn format_size(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["bytes", "KB", "MB", "GB"];
    let mut size = bytes as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }

    match unit {
        0 => format!("{bytes} bytes"),
        _ => format!("{size:.1} {}", UNITS[unit]),
    }
}

const PAGE_SIZE: usize = 25;

#[cfg(test)]
mod tests {
    use mail::fixtures;
    use store::Store;

    use crate::html::Encode;

    use super::*;

    #[test]
    fn blocks_remote_images() {
        let html = r#"<p>Hi<img src="https://tracker.example/p.gif"><img src="cid:logo"></p><script>x()</script>"#;
        let (blocked, count) = SanitizedHtml::new(html, RemoteImages::Blocked);
        assert_eq!(count, 1);
        assert!(!blocked.as_str().contains("tracker.example"));
        assert!(!blocked.as_str().contains("script"));

        let (shown, count) = SanitizedHtml::new(html, RemoteImages::Shown);
        assert_eq!(count, 0);
        assert!(shown.as_str().contains("https://tracker.example/p.gif"));
    }

    #[test]
    fn builds_location_hrefs() {
        let location = Location::new(MailboxName::new("[Gmail]/Sent Mail"));
        assert_eq!(location.href(), "/label/%5BGmail%5D%2FSent%20Mail");

        let location = Location {
            label: MailboxName::new("INBOX"),
            page: 2,
            thread: Some(ThreadId(12)),
            images: RemoteImages::Shown,
        };
        assert_eq!(
            location.href(),
            "/label/INBOX/thread/12?page=2&images=shown"
        );
    }

    #[test]
    fn groups_threads() {
        let (_directory, store) = fixtures::store();
        let index = index(&store);
        let location = Location::new(MailboxName::new("INBOX"));
        let list = index.thread_list(&location, "Inbox");
        assert_eq!(list.threads.len(), 2);
        assert!(!list.older);

        let plans = &list.threads[0];
        assert_eq!(plans.id, ThreadId(1));
        assert_eq!(plans.subject, "Plans for Thursday");
        assert_eq!(plans.senders, ["Alice Example", "Dirkjan"]);
        assert_eq!(plans.message_count, 2);
        assert_eq!(plans.read, ReadState::Read);

        let lunch = &list.threads[1];
        assert_eq!(lunch.senders, ["Bob"]);
        assert_eq!(lunch.read, ReadState::Unread);

        let labels = index.labels(&location.label);
        let mut names = Vec::new();
        for group in [&labels.mail, &labels.user] {
            for label in group {
                names.push((label.name.as_str(), label.depth, label.unread));
            }
        }
        assert_eq!(
            names,
            [
                ("Inbox", 0, 1),
                ("Sent Mail", 0, 0),
                ("All Mail", 0, 0),
                ("Work", 0, 0),
                ("Projects", 1, 1),
            ]
        );
    }

    #[test]
    fn renders_three_panes() {
        let (_directory, store) = fixtures::store();
        let index = index(&store);
        let location = Location::new(MailboxName::new("INBOX")).with_thread(ThreadId(1));
        let labels = index.labels(&location.label);
        let threads = index.thread_list(&location, labels.selected_name());

        let reader = store.reader().unwrap();
        let contents = reader.table::<MessageContents>().unwrap();
        let mut loaded = Vec::new();
        for ThreadMessage {
            key,
            metadata,
            read,
        } in index.thread_messages(ThreadId(1)).unwrap()
        {
            loaded.push(LoadedMessage {
                metadata,
                content: contents.get(key).unwrap().unwrap().value(),
                read,
            });
        }

        let mailbox = Mailbox {
            location,
            labels,
            threads,
            thread: Some(Thread::new(loaded, RemoteImages::Blocked)),
        };

        let html = mailbox.view().to_string();
        assert!(html.starts_with("<!DOCTYPE html><html lang=\"en\">"));
        assert!(html.contains("<title>Plans for Thursday – Encove</title>"));

        let inbox = html.find(">Inbox<").unwrap();
        let sent = html.find(">Sent Mail<").unwrap();
        let projects = html.find(">Projects<").unwrap();
        assert!(inbox < sent && sent < projects);
        assert!(html.contains("<span class=\"count\">1</span>"));
        assert!(html.contains("style=\"--depth: 1\""));
        assert!(html.contains("href=\"/label/%5BGmail%5D%2FSent%20Mail\""));

        assert!(
            html.contains("<li class=\"selected\"><a href=\"/label/INBOX\" aria-current=\"page\"")
        );
        assert!(
            html.contains(
                "<a class=\"thread\" href=\"/label/INBOX/thread/1\" aria-current=\"page\">"
            )
        );
        assert!(html.contains("Alice Example, Dirkjan"));
        assert!(html.contains("<a class=\"thread unread\" href=\"/label/INBOX/thread/2\">"));

        assert!(html.contains("Blocked 1 remote image to prevent tracking."));
        assert!(html.contains("href=\"/label/INBOX/thread/1?images=shown\""));
        assert!(!html.contains("tracker.example"));
        assert!(!html.contains("alert(1)"));
        assert!(html.contains("target=\"_blank\""));
        assert!(html.contains("<span class=\"filename\">plan.pdf</span>"));
        assert!(html.contains("&lt;alice@example.com&gt;"));
        assert!(html.contains("<div class=\"body text\">Café at 10?"));
    }

    fn index(store: &Store) -> MailIndex {
        MailIndex::new(&store.reader().unwrap()).unwrap()
    }
}
