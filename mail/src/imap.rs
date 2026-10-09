//! A minimal IMAP client for read-only synchronization
//!
//! Commands are formatted here, while responses are parsed with `imap-proto`. Mailboxes are only
//! opened with `EXAMINE` and messages are only fetched with `BODY.PEEK`, so the client never changes
//! anything on the server.

use core::fmt::Write as _;
use core::ops::RangeInclusive;
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::io;
use std::sync::Arc;

use base64::Engine;
use base64::alphabet::IMAP_MUTF7;
use base64::engine::DecodePaddingMode;
use base64::engine::general_purpose::{GeneralPurpose, GeneralPurposeConfig, STANDARD};
use imap_proto::{
    AttributeValue, Capability, MailboxDatum, MailboxListData, NameAttribute, Outcome, Response,
    ResponseCode, Status,
};
use jiff::Timestamp;
use jiff::civil::Date;
use rustls_platform_verifier::ConfigVerifierExt;
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::ClientConfig;
use tokio_rustls::rustls::pki_types::{InvalidDnsNameError, ServerName};

/// A connection to an IMAP server
pub(crate) struct ImapClient<S> {
    stream: S,
    buffer: Vec<u8>,
    next_tag: u32,
    capabilities: Vec<String>,
}

impl ImapClient<TlsStream<TcpStream>> {
    /// Connects to the server over TLS and reads its greeting
    pub(crate) async fn connect(account: &Account) -> Result<Self, ImapError> {
        Self::new(
            TlsConnector::from(Arc::new(ClientConfig::with_platform_verifier()?))
                .connect(
                    ServerName::try_from(account.host.as_str())?.to_owned(),
                    TcpStream::connect((account.host.as_str(), account.port)).await?,
                )
                .await?,
        )
        .await
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> ImapClient<S> {
    /// Reads the server greeting from an established connection
    pub(crate) async fn new(stream: S) -> Result<Self, ImapError> {
        let mut client = Self {
            stream,
            buffer: Vec::new(),
            next_tag: 1,
            capabilities: Vec::new(),
        };

        match client.read_response().await? {
            Response::Data {
                status: Status::Ok,
                outcome,
            } => {
                if let Some(ResponseCode::Capabilities(capabilities)) = outcome.code {
                    client.set_capabilities(capabilities);
                }
            }
            Response::Data {
                status: Status::Bye,
                outcome,
            } => return Err(ImapError::Bye(information(outcome))),
            response => return Err(ImapError::Unexpected(format!("greeting {response:?}"))),
        }

        if client.capabilities.is_empty() {
            client.run("CAPABILITY").await?;
        }

        Ok(client)
    }

    /// Authenticates with an OAuth 2.0 access token, using the `OAUTHBEARER` mechanism (RFC 7628)
    pub(crate) async fn authenticate(&mut self, account: &Account) -> Result<(), ImapError> {
        if !self.has_capability("AUTH=OAUTHBEARER") {
            return Err(ImapError::Unsupported("AUTH=OAUTHBEARER"));
        }

        let response = STANDARD.encode(account.token());
        let (tag, mut pending) = match self.has_capability("SASL-IR") {
            true => {
                let command = format!("AUTHENTICATE OAUTHBEARER {response}");
                (self.send(&command).await?, None)
            }
            false => (self.send("AUTHENTICATE OAUTHBEARER").await?, Some(response)),
        };

        loop {
            match self.read_response().await? {
                Response::Continue(_) => {
                    let line = pending.take().unwrap_or_else(|| ABORT_SASL.to_owned());
                    self.write_line(&line).await?;
                }
                Response::Done {
                    tag: done,
                    status,
                    outcome,
                } if done.0 == tag => {
                    let refreshed = matches!(outcome.code, Some(ResponseCode::Capabilities(_)));
                    match self.done("AUTHENTICATE", status, outcome) {
                        Ok(()) => {}
                        Err(ImapError::Rejected { message, .. }) => {
                            return Err(ImapError::Authentication(message));
                        }
                        Err(error) => return Err(error),
                    }

                    if !refreshed {
                        self.run("CAPABILITY").await?;
                    }

                    return Ok(());
                }
                response => self.unsolicited(response)?,
            }
        }
    }

    /// Lists all mailboxes, with their special-use attributes (RFC 6154) if the server has them
    pub(crate) async fn list(&mut self) -> Result<Vec<ListedMailbox>, ImapError> {
        let command =
            match self.has_capability("SPECIAL-USE") && self.has_capability("LIST-EXTENDED") {
                true => "LIST \"\" \"*\" RETURN (SPECIAL-USE)",
                false => "LIST \"\" \"*\"",
            };

        let mut mailboxes = Vec::new();
        for response in self.run(command).await? {
            let Response::MailboxData(MailboxDatum::List(list)) = response else {
                continue;
            };
            mailboxes.push(ListedMailbox::new(list));
        }

        Ok(mailboxes)
    }

    /// Opens a mailbox read-only, enabling CONDSTORE (RFC 7162) if the server supports it
    pub(crate) async fn examine(
        &mut self,
        mailbox: &MailboxName,
    ) -> Result<SelectedMailbox, ImapError> {
        let mut command = format!("EXAMINE {}", quote(mailbox.as_str())?);
        if self.has_capability("CONDSTORE") {
            command.push_str(" (CONDSTORE)");
        }

        let mut uid_validity = None;
        let mut uid_next = None;
        let mut highest_modseq = None;
        for response in self.run(&command).await? {
            let Response::Data {
                status: Status::Ok,
                outcome:
                    Outcome {
                        code: Some(code),
                        information: _,
                    },
            } = response
            else {
                continue;
            };

            match code {
                ResponseCode::UidValidity(value) => uid_validity = Some(value),
                ResponseCode::UidNext(value) => uid_next = Some(value),
                ResponseCode::HighestModSeq(value) => highest_modseq = Some(value),
                _ => {}
            }
        }

        let Some(uid_validity) = uid_validity else {
            return Err(ImapError::Unexpected(format!(
                "no UIDVALIDITY for {}",
                mailbox.as_str()
            )));
        };

        Ok(SelectedMailbox {
            uid_validity,
            uid_next,
            highest_modseq,
        })
    }

    /// Returns the UIDs of messages in the selected mailbox that arrived on or after `date`
    pub(crate) async fn search_since(&mut self, date: Date) -> Result<BTreeSet<u32>, ImapError> {
        self.search(&format!("UID SEARCH SINCE {}", date.strftime("%-d-%b-%Y")))
            .await
    }

    /// Returns the UIDs of all messages in the selected mailbox
    pub(crate) async fn search_all(&mut self) -> Result<BTreeSet<u32>, ImapError> {
        self.search("UID SEARCH ALL").await
    }

    /// Returns which of the given sorted UIDs are still in the selected mailbox
    pub(crate) async fn search_uids(&mut self, uids: &[u32]) -> Result<BTreeSet<u32>, ImapError> {
        if uids.is_empty() {
            return Ok(BTreeSet::new());
        }

        self.search(&format!("UID SEARCH UID {}", sequence_set(uids)))
            .await
    }

    async fn search(&mut self, command: &str) -> Result<BTreeSet<u32>, ImapError> {
        let mut uids = BTreeSet::new();
        for response in self.run(command).await? {
            let Response::MailboxData(MailboxDatum::Search(found)) = response else {
                continue;
            };
            uids.extend(found);
        }

        Ok(uids)
    }

    /// Fetches the flags and `Message-ID` header of the messages with the given sorted UIDs
    pub(crate) async fn fetch_message_ids(
        &mut self,
        uids: &[u32],
    ) -> Result<Vec<Fetched>, ImapError> {
        self.uid_fetch(uids, "(UID FLAGS BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)])")
            .await
    }

    /// Fetches the full source and arrival time of the messages with the given sorted UIDs
    pub(crate) async fn fetch_sources(&mut self, uids: &[u32]) -> Result<Vec<Fetched>, ImapError> {
        self.uid_fetch(uids, "(UID INTERNALDATE BODY.PEEK[])").await
    }

    /// Fetches the flags of the messages with the given sorted UIDs
    pub(crate) async fn fetch_flags(&mut self, uids: &[u32]) -> Result<Vec<Fetched>, ImapError> {
        self.uid_fetch(uids, "(UID FLAGS)").await
    }

    /// Fetches the flags of the messages in a range of UIDs that changed after `modseq`
    ///
    /// Only the messages that changed are returned, so the range can be large. This requires
    /// CONDSTORE.
    pub(crate) async fn fetch_changed_flags(
        &mut self,
        uids: RangeInclusive<u32>,
        modseq: u64,
    ) -> Result<Vec<Fetched>, ImapError> {
        let mut set = String::new();
        push_range(&mut set, *uids.start(), *uids.end());
        self.fetch(&format!(
            "UID FETCH {set} (UID FLAGS) (CHANGEDSINCE {modseq})"
        ))
        .await
    }

    async fn uid_fetch(&mut self, uids: &[u32], items: &str) -> Result<Vec<Fetched>, ImapError> {
        if uids.is_empty() {
            return Ok(Vec::new());
        }

        self.fetch(&format!("UID FETCH {} {items}", sequence_set(uids)))
            .await
    }

    async fn fetch(&mut self, command: &str) -> Result<Vec<Fetched>, ImapError> {
        let mut fetched = Vec::new();
        for response in self.run(command).await? {
            let Response::Fetch(_, attributes) = response else {
                continue;
            };
            let Some(message) = Fetched::new(attributes) else {
                continue;
            };
            fetched.push(message);
        }

        Ok(fetched)
    }

    /// Ends the session and closes the connection
    pub(crate) async fn logout(&mut self) -> Result<(), ImapError> {
        let tag = self.send("LOGOUT").await?;
        loop {
            match self.read_response().await {
                Ok(Response::Done { tag: done, .. }) if done.0 == tag => return Ok(()),
                Ok(_) => {}
                Err(ImapError::Closed) => return Ok(()),
                Err(error) => return Err(error),
            }
        }
    }

    async fn run(&mut self, command: &str) -> Result<Vec<Response<'static>>, ImapError> {
        let tag = self.send(command).await?;
        let mut responses = Vec::new();
        loop {
            match self.read_response().await? {
                Response::Done {
                    tag: done,
                    status,
                    outcome,
                } if done.0 == tag => {
                    self.done(command, status, outcome)?;
                    return Ok(responses);
                }
                Response::Capabilities(capabilities) => self.set_capabilities(capabilities),
                Response::Data {
                    status: Status::Bye,
                    outcome,
                } => return Err(ImapError::Bye(information(outcome))),
                Response::Continue(_) => {
                    return Err(ImapError::Unexpected(format!(
                        "continuation request for {}",
                        command_name(command)
                    )));
                }
                response => responses.push(response),
            }
        }
    }

    fn done(
        &mut self,
        command: &str,
        status: Status,
        outcome: Outcome<'_>,
    ) -> Result<(), ImapError> {
        let Outcome { code, information } = outcome;
        if let Some(ResponseCode::Capabilities(capabilities)) = code {
            self.set_capabilities(capabilities);
        }

        match status {
            Status::Ok => Ok(()),
            Status::No | Status::Bad | Status::PreAuth | Status::Bye => Err(ImapError::Rejected {
                command: command_name(command).to_owned(),
                message: information.map(Cow::into_owned).unwrap_or_default(),
            }),
        }
    }

    fn unsolicited(&mut self, response: Response<'_>) -> Result<(), ImapError> {
        match response {
            Response::Capabilities(capabilities) => self.set_capabilities(capabilities),
            Response::Data {
                status: Status::Bye,
                outcome,
            } => return Err(ImapError::Bye(information(outcome))),
            _ => {}
        }

        Ok(())
    }

    async fn send(&mut self, command: &str) -> Result<String, ImapError> {
        let tag = format!("A{:04}", self.next_tag);
        self.next_tag += 1;
        tracing::debug!(%tag, command = command_name(command), "sending IMAP command");
        self.write_line(&format!("{tag} {command}")).await?;
        Ok(tag)
    }

    async fn write_line(&mut self, line: &str) -> Result<(), ImapError> {
        self.stream.write_all(line.as_bytes()).await?;
        self.stream.write_all(b"\r\n").await?;
        self.stream.flush().await?;
        Ok(())
    }

    async fn read_response(&mut self) -> Result<Response<'static>, ImapError> {
        loop {
            if !self.buffer.is_empty() {
                match Response::parse(&self.buffer) {
                    Ok((rest, response)) => {
                        let consumed = self.buffer.len() - rest.len();
                        let response = response.into_owned();
                        self.buffer.drain(..consumed);
                        return Ok(response);
                    }
                    Err(nom::Err::Incomplete(_)) => {}
                    Err(nom::Err::Error(_) | nom::Err::Failure(_)) => self.skip_unparsed()?,
                }
            }

            if self.stream.read_buf(&mut self.buffer).await? == 0 {
                return Err(ImapError::Closed);
            }
        }
    }

    /// Skips an untagged response that `imap-proto` can't parse, like one from an unknown extension
    fn skip_unparsed(&mut self) -> Result<(), ImapError> {
        let Some(end) = self.buffer.windows(2).position(|pair| pair == b"\r\n") else {
            return Ok(());
        };

        let line = String::from_utf8_lossy(&self.buffer[..end]).into_owned();
        if !line.starts_with("* ") || line.ends_with('}') {
            return Err(ImapError::Parse(line));
        }

        tracing::warn!(%line, "skipping unparsed IMAP response");
        self.buffer.drain(..end + 2);
        Ok(())
    }

    fn set_capabilities(&mut self, capabilities: Vec<Capability<'_>>) {
        self.capabilities.clear();
        for capability in capabilities {
            self.capabilities.push(match capability {
                Capability::Imap4rev1 => "IMAP4REV1".to_owned(),
                Capability::Auth(mechanism) => format!("AUTH={}", mechanism.to_ascii_uppercase()),
                Capability::Atom(atom) => atom.to_ascii_uppercase(),
            });
        }
    }

    fn has_capability(&self, name: &str) -> bool {
        for capability in &self.capabilities {
            if capability.eq_ignore_ascii_case(name) {
                return true;
            }
        }

        false
    }
}

/// A mailbox as returned by `LIST`
#[derive(Debug)]
pub(crate) struct ListedMailbox {
    /// The name of the mailbox
    pub name: MailboxName,
    /// The character separating levels of the hierarchy, if there is one
    pub delimiter: Option<String>,
    /// Whether the mailbox can be opened; some only exist to contain other mailboxes
    pub selectable: bool,
    /// What the mailbox is used for, if it has a special use
    pub special_use: Option<SpecialUse>,
}

impl ListedMailbox {
    fn new(list: MailboxListData<'_>) -> Self {
        let MailboxListData {
            name_attributes,
            delimiter,
            name,
        } = list;

        let mut selectable = true;
        let mut special_use = None;
        for attribute in name_attributes {
            match attribute {
                NameAttribute::NoSelect => selectable = false,
                NameAttribute::All => special_use = Some(SpecialUse::All),
                NameAttribute::Archive => special_use = Some(SpecialUse::Archive),
                NameAttribute::Drafts => special_use = Some(SpecialUse::Drafts),
                NameAttribute::Flagged => special_use = Some(SpecialUse::Flagged),
                NameAttribute::Junk => special_use = Some(SpecialUse::Junk),
                NameAttribute::Sent => special_use = Some(SpecialUse::Sent),
                NameAttribute::Trash => special_use = Some(SpecialUse::Trash),
                NameAttribute::Extension(extension) => {
                    let extension = extension.trim_start_matches('\\');
                    if extension.eq_ignore_ascii_case("NonExistent") {
                        selectable = false;
                    } else if extension.eq_ignore_ascii_case("Important") {
                        special_use = Some(SpecialUse::Important);
                    }
                }
                _ => {}
            }
        }

        Self {
            name: MailboxName(name.into_owned()),
            delimiter: delimiter.map(Cow::into_owned),
            selectable,
            special_use,
        }
    }
}

/// What a mailbox is used for, from RFC 6154 and RFC 8457, in the order they are displayed
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SpecialUse {
    /// Messages marked with the `\Flagged` flag
    Flagged,
    /// Messages deemed important to the user
    Important,
    /// Messages sent by the user
    Sent,
    /// Messages being composed
    Drafts,
    /// All messages in the user's message store
    All,
    /// Messages moved out of the inbox
    Archive,
    /// Messages deemed to be junk mail
    Junk,
    /// Deleted messages
    Trash,
}

impl SpecialUse {
    pub(crate) fn to_bits(special_use: Option<Self>) -> u8 {
        match special_use {
            None => 0,
            Some(Self::Flagged) => 1,
            Some(Self::Important) => 2,
            Some(Self::Sent) => 3,
            Some(Self::Drafts) => 4,
            Some(Self::All) => 5,
            Some(Self::Archive) => 6,
            Some(Self::Junk) => 7,
            Some(Self::Trash) => 8,
        }
    }

    pub(crate) fn from_bits(bits: u8) -> Option<Self> {
        Some(match bits {
            1 => Self::Flagged,
            2 => Self::Important,
            3 => Self::Sent,
            4 => Self::Drafts,
            5 => Self::All,
            6 => Self::Archive,
            7 => Self::Junk,
            8 => Self::Trash,
            _ => return None,
        })
    }
}

/// The name of a mailbox, as sent over the wire
///
/// Non-ASCII characters are encoded as modified UTF-7; use [`MailboxName::decoded()`] to display
/// the name.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct MailboxName(String);

impl MailboxName {
    /// Wraps a mailbox name
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Decodes the name from modified UTF-7 (RFC 3501, section 5.1.3)
    pub fn decoded(&self) -> String {
        let mut decoded = String::new();
        let mut rest = self.0.as_str();
        while let Some((before, after)) = rest.split_once('&') {
            decoded.push_str(before);
            let Some((encoded, after)) = after.split_once('-') else {
                decoded.push('&');
                rest = after;
                break;
            };

            rest = after;
            if encoded.is_empty() {
                decoded.push('&');
                continue;
            }

            let Ok(bytes) = MODIFIED_BASE64.decode(encoded) else {
                write!(decoded, "&{encoded}-").unwrap();
                continue;
            };

            let mut units = Vec::new();
            for pair in bytes.as_chunks::<2>().0 {
                units.push(u16::from_be_bytes(*pair));
            }
            decoded.push_str(&String::from_utf16_lossy(&units));
        }

        decoded.push_str(rest);
        decoded
    }

    /// Returns whether this is the inbox
    pub fn is_inbox(&self) -> bool {
        self.0.eq_ignore_ascii_case(Self::INBOX)
    }

    /// Returns the name as sent over the wire
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The name of the inbox, which every server has
    pub const INBOX: &str = "INBOX";
}

/// The state of a mailbox opened with [`ImapClient::examine()`]
#[derive(Debug)]
pub(crate) struct SelectedMailbox {
    /// Changes when UIDs in the mailbox are no longer valid
    pub uid_validity: u32,
    /// The UID that the next message added to the mailbox will at least have
    pub uid_next: Option<u32>,
    /// The highest modification sequence of any message, if the server supports CONDSTORE
    pub highest_modseq: Option<u64>,
}

/// The data returned for a single message by `UID FETCH`
#[derive(Debug)]
pub(crate) struct Fetched {
    /// The UID of the message
    pub uid: u32,
    /// The flags of the message, if requested
    pub flags: Option<Flags>,
    /// When the message arrived on the server, if requested
    pub internal_date: Option<Timestamp>,
    /// The contents of the requested body section
    pub body: Option<Vec<u8>>,
}

impl Fetched {
    fn new(attributes: Vec<AttributeValue<'_>>) -> Option<Self> {
        let mut uid = None;
        let mut flags = None;
        let mut internal_date = None;
        let mut body = None;
        for attribute in attributes {
            match attribute {
                AttributeValue::Uid(value) => uid = Some(value),
                AttributeValue::Flags(value) => flags = Some(Flags::new(&value)),
                AttributeValue::InternalDate(value) => internal_date = parse_internal_date(&value),
                AttributeValue::BodySection { data, .. } => body = data.map(Cow::into_owned),
                _ => {}
            }
        }

        Some(Self {
            uid: uid?,
            flags,
            internal_date,
            body,
        })
    }
}

/// The system flags of a message (RFC 3501, section 2.3.2)
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Flags(u8);

impl Flags {
    fn new(flags: &[Cow<'_, str>]) -> Self {
        let mut bits = 0;
        for flag in flags {
            for (known, name) in Self::NAMES {
                if flag.eq_ignore_ascii_case(name) {
                    bits |= known.0;
                }
            }
        }

        Self(bits)
    }

    /// Restores flags from [`Flags::bits()`]
    pub fn from_bits(bits: u8) -> Self {
        Self(bits)
    }

    /// Returns whether all flags in `other` are set
    pub fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    /// Returns the flags as a bit set, for storage
    pub fn bits(self) -> u8 {
        self.0
    }

    /// The message has been read
    pub const SEEN: Self = Self(1);
    /// The message has been answered
    pub const ANSWERED: Self = Self(1 << 1);
    /// The message is flagged for urgent or special attention
    pub const FLAGGED: Self = Self(1 << 2);
    /// The message is a draft
    pub const DRAFT: Self = Self(1 << 3);

    const NAMES: [(Self, &str); 4] = [
        (Self::SEEN, "\\Seen"),
        (Self::ANSWERED, "\\Answered"),
        (Self::FLAGGED, "\\Flagged"),
        (Self::DRAFT, "\\Draft"),
    ];
}

/// An error from the IMAP connection
#[derive(Debug, thiserror::Error)]
pub enum ImapError {
    /// Reading from or writing to the connection failed
    #[error("connection failed: {0}")]
    Io(#[from] io::Error),
    /// The TLS configuration could not be built
    #[error("failed to configure TLS: {0}")]
    Tls(#[from] tokio_rustls::rustls::Error),
    /// The host name is not valid for TLS
    #[error("invalid server name: {0}")]
    ServerName(#[from] InvalidDnsNameError),
    /// The server closed the connection
    #[error("server closed the connection")]
    Closed,
    /// The server ended the session
    #[error("server ended the session: {0}")]
    Bye(String),
    /// The server sent a response that could not be parsed
    #[error("failed to parse server response: {0}")]
    Parse(String),
    /// The server sent a response that doesn't fit the command
    #[error("unexpected response: {0}")]
    Unexpected(String),
    /// The server lacks a required capability
    #[error("server does not support {0}")]
    Unsupported(&'static str),
    /// The server rejected the credentials
    #[error("authentication failed: {0}")]
    Authentication(String),
    /// The server rejected a command
    #[error("{command} failed: {message}")]
    Rejected {
        /// The name of the command
        command: String,
        /// The message from the server
        message: String,
    },
    /// A mailbox name contains characters that can't be sent in a quoted string
    #[error("mailbox name {0:?} can't be quoted")]
    Unquotable(String),
}

/// Formats sorted UIDs as a sequence set, collapsing consecutive UIDs into ranges
fn sequence_set(uids: &[u32]) -> String {
    let mut set = String::new();
    let Some((&first, rest)) = uids.split_first() else {
        return set;
    };

    let (mut start, mut end) = (first, first);
    for &uid in rest {
        if uid == end + 1 {
            end = uid;
            continue;
        }

        push_range(&mut set, start, end);
        (start, end) = (uid, uid);
    }

    push_range(&mut set, start, end);
    set
}

fn push_range(set: &mut String, start: u32, end: u32) {
    if !set.is_empty() {
        set.push(',');
    }

    match start == end {
        true => write!(set, "{start}").unwrap(),
        false => write!(set, "{start}:{end}").unwrap(),
    }
}

fn quote(value: &str) -> Result<String, ImapError> {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for c in value.chars() {
        match c {
            '"' | '\\' => {
                quoted.push('\\');
                quoted.push(c);
            }
            '\r' | '\n' => return Err(ImapError::Unquotable(value.to_owned())),
            c if !c.is_ascii() => return Err(ImapError::Unquotable(value.to_owned())),
            c => quoted.push(c),
        }
    }

    quoted.push('"');
    Ok(quoted)
}

/// Returns the command name for logs and errors, leaving out arguments like credentials
fn command_name(command: &str) -> &str {
    let mut words = command.splitn(3, ' ');
    let first = words.next().unwrap_or_default();
    match (first, words.next()) {
        ("UID", Some(second)) => &command[..first.len() + 1 + second.len()],
        _ => first,
    }
}

fn parse_internal_date(value: &str) -> Option<Timestamp> {
    let parsed = jiff::fmt::strtime::parse("%d-%b-%Y %H:%M:%S %z", value.trim()).ok()?;
    parsed.to_timestamp().ok()
}

fn information(outcome: Outcome<'_>) -> String {
    outcome.information.map(Cow::into_owned).unwrap_or_default()
}

/// The IMAP account to synchronize
pub struct Account {
    /// The host name of the IMAP server
    pub host: String,
    /// The port of the IMAP server, which must use implicit TLS
    pub port: u16,
    /// The user name, usually the email address
    pub user: String,
    /// An OAuth 2.0 access token for the user
    pub token: String,
}

impl Account {
    fn token(&self) -> String {
        format!(
            "n,a={},\x01host={}\x01port={}\x01auth=Bearer {}\x01\x01",
            self.user.replace('=', "=3D").replace(',', "=2C"),
            self.host,
            self.port,
            self.token,
        )
    }
}

/// The client response that cancels a failed SASL exchange: a single `0x01` byte (RFC 7628)
const ABORT_SASL: &str = "AQ==";

/// The modified base64 used in mailbox names, which uses `,` instead of `/` and has no padding
const MODIFIED_BASE64: GeneralPurpose = GeneralPurpose::new(
    &IMAP_MUTF7,
    GeneralPurposeConfig::new()
        .with_encode_padding(false)
        .with_decode_padding_mode(DecodePaddingMode::RequireNone)
        .with_decode_allow_trailing_bits(true),
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_sequence_sets() {
        assert_eq!(sequence_set(&[]), "");
        assert_eq!(sequence_set(&[7]), "7");
        assert_eq!(sequence_set(&[1, 2, 3, 5, 7, 8]), "1:3,5,7:8");
    }

    #[test]
    fn decodes_mailbox_names() {
        assert_eq!(MailboxName::new("INBOX").decoded(), "INBOX");
        assert_eq!(MailboxName::new("Tom &- Jerry").decoded(), "Tom & Jerry");
        assert_eq!(
            MailboxName::new("[Gmail]/Verzonden &AOk-l&AOk-ments").decoded(),
            "[Gmail]/Verzonden éléments"
        );
        assert_eq!(MailboxName::new("&ZeVnLIqe-").decoded(), "日本語");
    }

    #[test]
    fn quotes_strings() {
        assert_eq!(quote("[Gmail]/All Mail").unwrap(), "\"[Gmail]/All Mail\"");
        assert_eq!(quote("a\"b\\c").unwrap(), "\"a\\\"b\\\\c\"");
        assert!(quote("a\r\nb").is_err());
    }

    #[test]
    fn hides_command_arguments() {
        assert_eq!(
            command_name("AUTHENTICATE OAUTHBEARER c2VjcmV0"),
            "AUTHENTICATE"
        );
        assert_eq!(command_name("UID FETCH 1:3 (FLAGS)"), "UID FETCH");
        assert_eq!(command_name("LOGOUT"), "LOGOUT");
    }

    #[test]
    fn parses_internal_dates() {
        let parsed = parse_internal_date(" 8-Oct-2026 10:32:25 +0200").unwrap();
        assert_eq!(parsed.to_string(), "2026-10-08T08:32:25Z");
        let parsed = parse_internal_date("18-Oct-2026 10:32:25 +0000").unwrap();
        assert_eq!(parsed.to_string(), "2026-10-18T10:32:25Z");
    }
}
