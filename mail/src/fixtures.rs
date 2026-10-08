//! Messages and a store with them for tests

use jiff::Timestamp;
use store::Store;
use tempfile::TempDir;

use crate::imap::{Flags, MailboxName, SpecialUse};
use crate::store::{Entry, Mailbox, MessageContents, MessageData, MessageKey, MessageSource};

/// Stores the messages as they would appear in Gmail, with a nested user label
///
/// The messages get consecutive keys, starting at [`MessageKey::FIRST`] for [`PLANS`]. The
/// database is removed when the returned directory is dropped.
pub fn store() -> (TempDir, Store) {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::new(directory.path().join("encove.redb"));
    let writer = store.writer().unwrap();

    let mut mailboxes = writer.table::<Mailbox>().unwrap();
    for (name, special_use) in [
        ("INBOX", None),
        ("[Gmail]/All Mail", Some(SpecialUse::All)),
        ("[Gmail]/Sent Mail", Some(SpecialUse::Sent)),
        ("Work", None),
        ("Work/Projects", None),
    ] {
        let mailbox = Mailbox {
            name: MailboxName::new(name),
            uid_validity: 1,
            highest_modseq: None,
            special_use,
            delimiter: Some("/".to_owned()),
        };
        mailboxes.insert(name, &mailbox).unwrap();
    }

    let mut metadata = writer.table::<MessageData>().unwrap();
    let mut contents = writer.table::<MessageContents>().unwrap();
    let mut sources = writer.table::<MessageSource>().unwrap();
    let mut keys = Vec::new();
    let mut key = MessageKey::FIRST;
    for (index, (source, flags)) in [
        (PLANS, Flags::SEEN),
        (LUNCH, Flags::default()),
        (REPLY, Flags::SEEN),
    ]
    .into_iter()
    .enumerate()
    {
        let received = Timestamp::from_second(1_791_000_000 + index as i64 * 3600).unwrap();
        let (mut message, content) = MessageData::extract(source.as_bytes(), Some(received));
        message.flags = flags;
        metadata.insert(key, &message).unwrap();
        contents.insert(key, &content).unwrap();
        sources.insert(key, source.as_bytes()).unwrap();
        keys.push(key);
        key = key.next();
    }

    let mut entries = writer.table::<Entry>().unwrap();
    let (plans, lunch, reply) = (keys[0], keys[1], keys[2]);
    for (mailbox, uid, message) in [
        ("INBOX", 1, plans),
        ("INBOX", 2, lunch),
        ("[Gmail]/All Mail", 1, plans),
        ("[Gmail]/All Mail", 2, lunch),
        ("[Gmail]/All Mail", 3, reply),
        ("[Gmail]/Sent Mail", 1, reply),
        ("Work/Projects", 1, lunch),
    ] {
        let entry = Entry {
            mailbox: MailboxName::new(mailbox),
            uid,
            message,
        };
        entries.insert((mailbox, uid), &entry).unwrap();
    }

    drop((mailboxes, metadata, contents, sources, entries));
    writer.commit().unwrap();
    (directory, store)
}

/// A multipart message with plain text, HTML with a tracking image, and a PDF attachment
pub const PLANS: &str = "From: Alice Example <alice@example.com>\r
To: dirkjan@example.com\r
Subject: Plans for Thursday\r
Message-ID: <a@example.com>\r
Date: Wed, 07 Oct 2026 10:32:25 +0000\r
MIME-Version: 1.0\r
Content-Type: multipart/mixed; boundary=\"outer\"\r
\r
--outer\r
Content-Type: multipart/alternative; boundary=\"inner\"\r
\r
--inner\r
Content-Type: text/plain; charset=UTF-8\r
\r
Hi Dirkjan, here is the plan.\r
--inner\r
Content-Type: text/html; charset=UTF-8\r
\r
<div>Hi Dirkjan,<br>Here is the <a href=\"https://example.com\">plan</a>.<img src=\"https://tracker.example/open.gif\"></div><script>alert(1)</script>\r
--inner--\r
--outer\r
Content-Type: application/pdf; name=\"plan.pdf\"\r
Content-Disposition: attachment; filename=\"plan.pdf\"\r
Content-Transfer-Encoding: base64\r
\r
JVBERi0xLjQK\r
--outer--\r
";

/// An unrelated plain text message
pub const LUNCH: &str = "From: Bob <bob@example.com>\r
To: dirkjan@example.com\r
Subject: Lunch\r
Message-ID: <b@example.com>\r
Date: Wed, 07 Oct 2026 11:00:00 +0000\r
Content-Type: text/plain; charset=UTF-8\r
\r
Lunch tomorrow?\r
";

/// A reply to [`PLANS`], encoded as quoted-printable ISO-8859-1
pub const REPLY: &str = "From: Dirkjan <dirkjan@example.com>\r
To: Alice Example <alice@example.com>\r
Subject: Re: Plans for Thursday\r
Message-ID: <c@example.com>\r
In-Reply-To: <a@example.com>\r
References: <a@example.com>\r
Date: Wed, 07 Oct 2026 12:00:00 +0000\r
Content-Type: text/plain; charset=ISO-8859-1\r
Content-Transfer-Encoding: quoted-printable\r
\r
Caf=E9 at 10?\r
";
