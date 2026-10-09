//! The tables storing synchronized messages
//!
//! Each mailbox records which messages it contains by UID, while the messages themselves are
//! stored once, keyed by a local [`MessageKey`] and deduplicated by their `Message-ID` header. With
//! Gmail, the same message shows up in every mailbox for one of its labels.

use core::cmp::Ordering;

use jiff::Timestamp;
use mail_parser::{HeaderValue, MessageParser, MimeHeaders};
use redb::{Key, TableDefinition, TypeName, Value};
use store::{Decode, Encode, Table};

use crate::{Flags, MailboxName, SpecialUse};

/// A message to load for display in a thread
pub struct ThreadMessage<'a> {
    /// The stored message
    pub key: MessageKey,
    /// The stored metadata of the message
    pub metadata: &'a MessageData,
    /// Whether the message has been read
    pub read: ReadState,
}

/// Whether a message or thread has been read
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadState {
    /// All messages have been read
    Read,
    /// At least one message is unread
    Unread,
}

/// The synchronization state of a mailbox
#[derive(Debug)]
pub struct Mailbox {
    /// The name of the mailbox
    pub name: MailboxName,
    /// The UIDVALIDITY the stored UIDs belong to
    pub uid_validity: u32,
    /// The highest modification sequence seen, if the server supports CONDSTORE
    pub highest_modseq: Option<u64>,
    /// The lowest UID in the window at the last synchronization, or the next UID if the window was
    /// empty, if the mailbox has been synchronized
    ///
    /// Synchronization only looks at entries from here on: lower UIDs belong to messages that
    /// arrived before the window, which keep their entries.
    pub window_start: Option<u32>,
    /// What the mailbox is used for, if it has a special use
    pub special_use: Option<SpecialUse>,
    /// The character separating levels of the hierarchy, if there is one
    pub delimiter: Option<String>,
}

impl Value for Mailbox {
    type SelfType<'a>
        = Self
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(mut data: &'a [u8]) -> Self
    where
        Self: 'a,
    {
        Self::decode(&mut data)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self) -> Vec<u8>
    where
        Self: 'b,
    {
        let mut buf = Vec::new();
        value.encode(&mut buf);
        buf
    }

    fn type_name() -> TypeName {
        TypeName::new("encove::mail::mailbox")
    }
}

/// Mailboxes, by name
impl Table for Mailbox {
    type Key = &'static str;
    const DEFINITION: TableDefinition<'static, &'static str, Self> =
        TableDefinition::new("encove::mail::mailbox");
}

impl Encode for Mailbox {
    fn encode(&self, buf: &mut Vec<u8>) {
        let Self {
            name,
            uid_validity,
            highest_modseq,
            window_start,
            special_use,
            delimiter,
        } = self;

        name.encode(buf);
        uid_validity.encode(buf);
        highest_modseq.encode(buf);
        window_start.encode(buf);
        SpecialUse::to_bits(*special_use).encode(buf);
        delimiter.encode(buf);
    }
}

impl Decode for Mailbox {
    fn decode(buf: &mut &[u8]) -> Self {
        Self {
            name: MailboxName::decode(buf),
            uid_validity: u32::decode(buf),
            highest_modseq: Option::decode(buf),
            window_start: Option::decode(buf),
            special_use: SpecialUse::from_bits(u8::decode(buf)),
            delimiter: Option::decode(buf),
        }
    }
}

/// A message's presence in a mailbox
#[derive(Debug)]
pub struct Entry {
    /// The mailbox containing the message
    pub mailbox: MailboxName,
    /// The UID of the message in the mailbox
    pub uid: u32,
    /// The stored message
    pub message: MessageKey,
}

impl Value for Entry {
    type SelfType<'a>
        = Self
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(mut data: &'a [u8]) -> Self
    where
        Self: 'a,
    {
        Self::decode(&mut data)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self) -> Vec<u8>
    where
        Self: 'b,
    {
        let mut buf = Vec::new();
        value.encode(&mut buf);
        buf
    }

    fn type_name() -> TypeName {
        TypeName::new("encove::mail::entry")
    }
}

/// Entries, by mailbox name and UID
impl Table for Entry {
    type Key = (&'static str, u32);
    const DEFINITION: TableDefinition<'static, (&'static str, u32), Self> =
        TableDefinition::new("encove::mail::entry");
}

impl Encode for Entry {
    fn encode(&self, buf: &mut Vec<u8>) {
        let Self {
            mailbox,
            uid,
            message,
        } = self;

        mailbox.encode(buf);
        uid.encode(buf);
        message.encode(buf);
    }
}

impl Decode for Entry {
    fn decode(buf: &mut &[u8]) -> Self {
        Self {
            mailbox: MailboxName::decode(buf),
            uid: u32::decode(buf),
            message: MessageKey::decode(buf),
        }
    }
}

/// A message's presence in a mailbox, indexed by the message
///
/// This has no value: the key holds the message key, mailbox name and UID of an [`Entry`].
#[derive(Debug)]
pub struct MessageEntry;

impl Value for MessageEntry {
    type SelfType<'a>
        = ()
    where
        Self: 'a;
    type AsBytes<'a>
        = [u8; 0]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(0)
    }

    fn from_bytes<'a>(_: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
    }

    fn as_bytes<'a, 'b: 'a>(_: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        []
    }

    fn type_name() -> TypeName {
        TypeName::new("encove::mail::message::entry")
    }
}

/// Entries, by message key, mailbox name and UID
impl Table for MessageEntry {
    type Key = (MessageKey, &'static str, u32);
    const DEFINITION: TableDefinition<'static, (MessageKey, &'static str, u32), Self> =
        TableDefinition::new("encove::mail::message::entry");
}

/// The parts of a message needed to list it and show its headers, extracted when it is stored
#[derive(Debug)]
pub struct MessageData {
    /// When the message arrived on the server
    pub received: Timestamp,
    /// The flags of the message, which are assumed to be the same in every mailbox containing it
    pub flags: Flags,
    /// The `Message-ID` header, without angle brackets
    pub message_id: Option<String>,
    /// The `Date` header
    pub date: Option<Timestamp>,
    /// The subject of the message
    pub subject: String,
    /// The `From` header
    pub from: Option<Address>,
    /// The `To` header
    pub to: Vec<Address>,
    /// The `Cc` header
    pub cc: Vec<Address>,
    /// The `Bcc` header, which is usually only present on sent messages and drafts
    pub bcc: Vec<Address>,
    /// The start of the message text
    pub snippet: String,
    /// The `Message-ID`s from the `In-Reply-To` and `References` headers
    pub parents: Vec<String>,
}

impl MessageData {
    /// Extracts the parts of a message needed to list and display it
    ///
    /// The flags are left empty, since they are not part of the message source.
    pub(crate) fn extract(source: &[u8], received: Option<Timestamp>) -> (Self, MessageContents) {
        let Some(message) = MessageParser::new().parse(source) else {
            let metadata = Self {
                received: received.unwrap_or_else(Timestamp::now),
                flags: Flags::default(),
                message_id: None,
                date: None,
                subject: String::new(),
                from: None,
                to: Vec::new(),
                cc: Vec::new(),
                bcc: Vec::new(),
                snippet: String::new(),
                parents: Vec::new(),
            };

            let content = MessageContents {
                body: Body::Empty,
                attachments: Vec::new(),
            };
            return (metadata, content);
        };

        let date = message
            .date()
            .and_then(|date| Timestamp::from_second(date.to_timestamp()).ok());

        let mut parents = Vec::new();
        for header in [message.in_reply_to(), message.references()] {
            let ids = match header {
                HeaderValue::Text(id) => core::slice::from_ref(id),
                HeaderValue::TextList(ids) => ids.as_slice(),
                _ => &[],
            };

            for id in ids {
                if !parents.iter().any(|parent: &String| parent == id) {
                    parents.push(id.to_string());
                }
            }
        }

        let metadata = Self {
            received: received.or(date).unwrap_or_else(Timestamp::now),
            flags: Flags::default(),
            message_id: message.message_id().map(str::to_owned),
            date,
            subject: message.subject().unwrap_or_default().to_owned(),
            from: message
                .from()
                .and_then(mail_parser::Address::first)
                .map(Address::from_addr),
            to: Address::parse_list(message.to()),
            cc: Address::parse_list(message.cc()),
            bcc: Address::parse_list(message.bcc()),
            snippet: preview(&message),
            parents,
        };

        let mut attachments = Vec::new();
        for part in message.attachments() {
            attachments.push(attachment(part));
        }

        let content = MessageContents {
            body: Body::from_message(&message),
            attachments,
        };
        (metadata, content)
    }
}

fn preview(message: &mail_parser::Message<'_>) -> String {
    let Some(preview) = message.body_preview(SNIPPET_LENGTH) else {
        return String::new();
    };

    let mut snippet = String::new();
    for word in preview.split_whitespace() {
        if !snippet.is_empty() {
            snippet.push(' ');
        }
        snippet.push_str(word);
    }
    snippet
}

const SNIPPET_LENGTH: usize = 200;

fn attachment(part: &mail_parser::MessagePart<'_>) -> Attachment {
    let mime_type = match part.content_type() {
        Some(content_type) => match content_type.subtype() {
            Some(subtype) => format!("{}/{subtype}", content_type.ctype()),
            None => content_type.ctype().to_owned(),
        },
        None => "application/octet-stream".to_owned(),
    };

    Attachment {
        filename: part.attachment_name().unwrap_or("(unnamed)").to_owned(),
        mime_type,
        size: part.len() as u64,
    }
}

impl Encode for MessageData {
    fn encode(&self, buf: &mut Vec<u8>) {
        let Self {
            received,
            flags,
            message_id,
            date,
            subject,
            from,
            to,
            cc,
            bcc,
            snippet,
            parents,
        } = self;

        received.as_millisecond().encode(buf);
        flags.encode(buf);
        message_id.encode(buf);
        date.map(|date| date.as_millisecond()).encode(buf);
        subject.encode(buf);
        from.encode(buf);
        to.encode(buf);
        cc.encode(buf);
        bcc.encode(buf);
        snippet.encode(buf);
        parents.encode(buf);
    }
}

impl Decode for MessageData {
    fn decode(buf: &mut &[u8]) -> Self {
        Self {
            received: Timestamp::from_millisecond(i64::decode(buf))
                .unwrap_or(Timestamp::UNIX_EPOCH),
            flags: Flags::decode(buf),
            message_id: Option::decode(buf),
            date: Option::<i64>::decode(buf)
                .and_then(|date| Timestamp::from_millisecond(date).ok()),
            subject: String::decode(buf),
            from: Option::decode(buf),
            to: Vec::decode(buf),
            cc: Vec::decode(buf),
            bcc: Vec::decode(buf),
            snippet: String::decode(buf),
            parents: Vec::decode(buf),
        }
    }
}

impl Value for MessageData {
    type SelfType<'a>
        = Self
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(mut data: &'a [u8]) -> Self
    where
        Self: 'a,
    {
        Self::decode(&mut data)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self) -> Vec<u8>
    where
        Self: 'b,
    {
        let mut buf = Vec::new();
        value.encode(&mut buf);
        buf
    }

    fn type_name() -> TypeName {
        TypeName::new("encove::mail::message::data")
    }
}

/// Summaries, by message key
impl Table for MessageData {
    type Key = MessageKey;
    const DEFINITION: TableDefinition<'static, MessageKey, Self> =
        TableDefinition::new("encove::mail::message::data");
}

/// An email address, with the display name from the header if it has one
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Address {
    /// The display name
    pub name: Option<String>,
    /// The email address
    pub address: String,
}

impl Address {
    pub(crate) fn parse_list(addresses: Option<&mail_parser::Address<'_>>) -> Vec<Self> {
        let mut list = Vec::new();
        let Some(addresses) = addresses else {
            return list;
        };

        for address in addresses.iter() {
            list.push(Self::from_addr(address));
        }
        list
    }

    pub(crate) fn from_addr(address: &mail_parser::Addr<'_>) -> Self {
        let name = address.name.as_deref().map(str::trim).unwrap_or_default();
        Self {
            name: match name.is_empty() {
                true => None,
                false => Some(name.to_owned()),
            },
            address: address.address.as_deref().unwrap_or_default().to_owned(),
        }
    }

    /// Returns the display name, or the address if there is no display name
    pub fn display_name(&self) -> &str {
        self.name.as_deref().unwrap_or(&self.address)
    }
}

impl Encode for Address {
    fn encode(&self, buf: &mut Vec<u8>) {
        let Self { name, address } = self;
        name.encode(buf);
        address.encode(buf);
    }
}

impl Decode for Address {
    fn decode(buf: &mut &[u8]) -> Self {
        Self {
            name: Option::decode(buf),
            address: String::decode(buf),
        }
    }
}

/// The parts of a message needed to display it, extracted when it is stored
#[derive(Debug)]
pub struct MessageContents {
    /// The body to display
    pub body: Body,
    /// The files attached to the message
    pub attachments: Vec<Attachment>,
}

impl Value for MessageContents {
    type SelfType<'a>
        = Self
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(mut data: &'a [u8]) -> Self
    where
        Self: 'a,
    {
        Self::decode(&mut data)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self) -> Vec<u8>
    where
        Self: 'b,
    {
        let mut buf = Vec::new();
        value.encode(&mut buf);
        buf
    }

    fn type_name() -> TypeName {
        TypeName::new("encove::mail::message::contents")
    }
}

/// Message contents, by message key
impl Table for MessageContents {
    type Key = MessageKey;
    const DEFINITION: TableDefinition<'static, MessageKey, Self> =
        TableDefinition::new("encove::mail::message::contents");
}

impl Encode for MessageContents {
    fn encode(&self, buf: &mut Vec<u8>) {
        let Self { body, attachments } = self;
        body.encode(buf);
        attachments.encode(buf);
    }
}

impl Decode for MessageContents {
    fn decode(buf: &mut &[u8]) -> Self {
        Self {
            body: Body::decode(buf),
            attachments: Vec::decode(buf),
        }
    }
}

/// The body of a message, as it is displayed
#[derive(Debug)]
pub enum Body {
    /// HTML from the message, which must be sanitized before it is displayed
    Html(String),
    /// Plain text
    Text(String),
    /// The message has no displayable content
    Empty,
}

impl Body {
    pub(super) fn from_message(message: &mail_parser::Message<'_>) -> Self {
        if let Some(part) = message.html_part(0)
            && part.is_text_html()
            && let Some(html) = message.body_html(0)
        {
            return Self::Html(html.into_owned());
        }

        match message.body_text(0) {
            Some(text) => Self::Text(text.into_owned()),
            None => Self::Empty,
        }
    }
}

impl Encode for Body {
    fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            Self::Html(html) => {
                1u8.encode(buf);
                html.encode(buf);
            }
            Self::Text(text) => {
                2u8.encode(buf);
                text.encode(buf);
            }
            Self::Empty => 0u8.encode(buf),
        }
    }
}

impl Decode for Body {
    fn decode(buf: &mut &[u8]) -> Self {
        match u8::decode(buf) {
            1 => Self::Html(String::decode(buf)),
            2 => Self::Text(String::decode(buf)),
            _ => Self::Empty,
        }
    }
}

/// A file attached to a message
#[derive(Debug)]
pub struct Attachment {
    /// The name of the file
    pub filename: String,
    /// The MIME type of the file
    pub mime_type: String,
    /// The size of the file in bytes
    pub size: u64,
}

impl Encode for Attachment {
    fn encode(&self, buf: &mut Vec<u8>) {
        let Self {
            filename,
            mime_type,
            size,
        } = self;

        filename.encode(buf);
        mime_type.encode(buf);
        size.encode(buf);
    }
}

impl Decode for Attachment {
    fn decode(buf: &mut &[u8]) -> Self {
        Self {
            filename: String::decode(buf),
            mime_type: String::decode(buf),
            size: u64::decode(buf),
        }
    }
}

/// The local key of a stored message
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MessageKey(u64);

impl MessageKey {
    /// Returns the key that follows this one
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }

    /// Returns the key as a number
    pub fn get(self) -> u64 {
        self.0
    }

    /// The key of the first stored message
    pub const FIRST: Self = Self(1);
}

impl Value for MessageKey {
    type SelfType<'a>
        = Self
    where
        Self: 'a;
    type AsBytes<'a>
        = [u8; 8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(8)
    }

    fn from_bytes<'a>(mut data: &'a [u8]) -> Self
    where
        Self: 'a,
    {
        Self::decode(&mut data)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self) -> [u8; 8]
    where
        Self: 'b,
    {
        value.0.to_le_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::new("encove::mail::message::key")
    }
}

impl Key for MessageKey {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        Self::from_bytes(data1).cmp(&Self::from_bytes(data2))
    }
}

/// Keys of stored messages, by their `Message-ID` header
impl Table for MessageKey {
    type Key = &'static str;
    const DEFINITION: TableDefinition<'static, &'static str, Self> =
        TableDefinition::new("encove::mail::message::key");
}

impl Encode for MessageKey {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.0.encode(buf);
    }
}

impl Decode for MessageKey {
    fn decode(buf: &mut &[u8]) -> Self {
        Self(u64::decode(buf))
    }
}

/// The full source of a stored message, read and written as a byte slice
#[derive(Debug)]
pub struct MessageSource;

impl Value for MessageSource {
    type SelfType<'a>
        = &'a [u8]
    where
        Self: 'a;
    type AsBytes<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> &'a [u8]
    where
        Self: 'a,
    {
        data
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a &'b [u8]) -> &'a [u8]
    where
        Self: 'b,
    {
        value
    }

    fn type_name() -> TypeName {
        TypeName::new("encove::mail::message::source")
    }
}

/// Message sources, by message key
impl Table for MessageSource {
    type Key = MessageKey;
    const DEFINITION: TableDefinition<'static, MessageKey, Self> =
        TableDefinition::new("encove::mail::message::source");
}

impl Encode for MailboxName {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.as_str().encode(buf);
    }
}

impl Decode for MailboxName {
    fn decode(buf: &mut &[u8]) -> Self {
        Self::new(String::decode(buf))
    }
}

impl Encode for Flags {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.bits().encode(buf);
    }
}

impl Decode for Flags {
    fn decode(buf: &mut &[u8]) -> Self {
        Self::from_bits(u8::decode(buf))
    }
}
