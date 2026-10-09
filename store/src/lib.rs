//! Local storage for synchronized messages, in a redb database
//!
//! Each table is identified by the type of its values, which implements [`Table`].
//! [`Reader::table()`] and [`Writer::table()`] open a table, which provides the basic operations
//! of the underlying redb table. Values can use [`Encode`] and [`Decode`] for their encoding.
//!
//! The database is only open while a [`Reader`] or [`Writer`] is alive. redb only lets a process
//! write to a database while no other process has it open, so keeping these short-lived allows
//! `sync` to write while the web server is running: each waits briefly for the other to finish.

use core::borrow::Borrow;
use core::ops::RangeBounds;
use core::str;
use core::time::Duration;
use std::path::PathBuf;
use std::thread;

use redb::{
    AccessGuard, Database, DatabaseError, Key, Range, ReadOnlyDatabase, ReadOnlyTable,
    ReadTransaction, ReadableDatabase, ReadableTable, TableDefinition, TableError, Value,
    WriteTransaction,
};

/// The local message store
#[derive(Clone)]
pub struct Store {
    path: PathBuf,
}

impl Store {
    /// Uses the database at the given path, which is created on the first write
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Opens the database and starts a read transaction
    ///
    /// Other processes can't write to the database until the `Reader` is dropped.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotSynced` if nothing has been synchronized yet, and `Error::Busy` if
    /// another process keeps the database open for too long.
    pub fn reader(&self) -> Result<Reader<'_>, StoreError> {
        if !self.path.exists() {
            return Err(StoreError::NotSynced);
        }

        for _ in 0..Self::OPEN_ATTEMPTS {
            match ReadOnlyDatabase::open(&self.path) {
                Ok(database) => {
                    return Ok(Reader {
                        transaction: database.begin_read()?,
                        _database: database,
                        _store: self,
                    });
                }
                Err(DatabaseError::DatabaseAlreadyOpen) => thread::sleep(Self::RETRY_DELAY),
                Err(error) => return Err(error.into()),
            }
        }

        Err(StoreError::Busy)
    }

    /// Opens the database, creating it if needed, and starts a write transaction
    ///
    /// Changes are only saved by `Writer::commit()`. Other processes can't open the database
    /// until the `Writer` is committed or dropped.
    ///
    /// # Errors
    ///
    /// Returns `Error::Busy` if another process keeps the database open for too long.
    pub fn writer(&self) -> Result<Writer<'_>, StoreError> {
        for _ in 0..Self::OPEN_ATTEMPTS {
            match Database::create(&self.path) {
                Ok(database) => {
                    let writer = Writer {
                        transaction: database.begin_write()?,
                        _database: database,
                        _store: self,
                    };
                    return Ok(writer);
                }
                Err(DatabaseError::DatabaseAlreadyOpen) => thread::sleep(Self::RETRY_DELAY),
                Err(error) => return Err(error.into()),
            }
        }

        Err(StoreError::Busy)
    }

    const OPEN_ATTEMPTS: usize = 100;
    const RETRY_DELAY: Duration = Duration::from_millis(50);
}

/// A read transaction, which keeps the database open until it is dropped
pub struct Reader<'a> {
    transaction: ReadTransaction,
    _database: ReadOnlyDatabase,
    _store: &'a Store,
}

impl Reader<'_> {
    /// Opens the table storing values of type `T`
    ///
    /// # Errors
    ///
    /// Returns `Error::NotSynced` if the table hasn't been created yet.
    pub fn table<T: Table>(&self) -> Result<ReadTable<T>, StoreError> {
        match self.transaction.open_table(T::DEFINITION) {
            Ok(table) => Ok(ReadTable(table)),
            Err(TableError::TableDoesNotExist(_)) => Err(StoreError::NotSynced),
            Err(error) => Err(error.into()),
        }
    }
}

/// A table opened in a [`Reader`]
pub struct ReadTable<T: Table>(ReadOnlyTable<T::Key, T>);

impl<T: Table> ReadTable<T> {
    /// Returns the value stored for a key
    pub fn get<'k>(
        &self,
        key: impl Borrow<<<T as Table>::Key as Value>::SelfType<'k>>,
    ) -> Result<Option<AccessGuard<'_, T>>, StoreError> {
        Ok(self.0.get(key)?)
    }

    /// Returns all rows, ordered by key
    pub fn iter(&self) -> Result<Range<'_, T::Key, T>, StoreError> {
        Ok(self.0.iter()?)
    }
}

/// A write transaction, which keeps the database open until it is committed or dropped
///
/// Dropping the writer without committing it discards its changes.
#[must_use = "changes are discarded unless the writer is committed"]
pub struct Writer<'a> {
    transaction: WriteTransaction,
    _database: Database,
    _store: &'a Store,
}

impl Writer<'_> {
    /// Opens the table storing values of type `T`, creating it if needed
    ///
    /// The table must be dropped before the writer is committed.
    pub fn table<T: Table>(&self) -> Result<WriteTable<'_, T>, StoreError> {
        Ok(WriteTable(self.transaction.open_table(T::DEFINITION)?))
    }

    /// Saves the changes and closes the database
    pub fn commit(self) -> Result<(), StoreError> {
        let Self { transaction, .. } = self;
        transaction.commit()?;
        Ok(())
    }
}

/// A table opened in a [`Writer`]
pub struct WriteTable<'a, T: Table>(redb::Table<'a, T::Key, T>);

impl<T: Table> WriteTable<'_, T> {
    /// Returns the value stored for a key
    pub fn get<'k>(
        &self,
        key: impl Borrow<<<T as Table>::Key as Value>::SelfType<'k>>,
    ) -> Result<Option<AccessGuard<'_, T>>, StoreError> {
        Ok(self.0.get(key)?)
    }

    /// Returns all rows, ordered by key
    pub fn iter(&self) -> Result<Range<'_, T::Key, T>, StoreError> {
        Ok(self.0.iter()?)
    }

    /// Returns the rows with keys in `range`, ordered by key
    pub fn range<'k, R>(
        &self,
        range: impl RangeBounds<R> + 'k,
    ) -> Result<Range<'_, T::Key, T>, StoreError>
    where
        R: Borrow<<<T as Table>::Key as Value>::SelfType<'k>> + 'k,
    {
        Ok(self.0.range(range)?)
    }

    /// Returns the row with the highest key
    #[allow(clippy::type_complexity)]
    pub fn last(
        &self,
    ) -> Result<Option<(AccessGuard<'_, <T as Table>::Key>, AccessGuard<'_, T>)>, StoreError> {
        Ok(self.0.last()?)
    }

    /// Stores a value for a key, replacing any previous value
    pub fn insert<'k, 'v>(
        &mut self,
        key: impl Borrow<<<T as Table>::Key as Value>::SelfType<'k>>,
        value: impl Borrow<T::SelfType<'v>>,
    ) -> Result<(), StoreError> {
        self.0.insert(key, value)?;
        Ok(())
    }

    /// Removes the row for a key, returning its value if there was one
    pub fn remove<'k>(
        &mut self,
        key: impl Borrow<<<T as Table>::Key as Value>::SelfType<'k>>,
    ) -> Result<Option<AccessGuard<'_, T>>, StoreError> {
        Ok(self.0.remove(key)?)
    }

    /// Removes all rows for which `keep` returns `false`
    pub fn retain(
        &mut self,
        keep: impl for<'f> FnMut(<<T as Table>::Key as Value>::SelfType<'f>, T::SelfType<'f>) -> bool,
    ) -> Result<(), StoreError> {
        Ok(self.0.retain(keep)?)
    }

    /// Removes the rows with keys in `range` for which `keep` returns `false`
    pub fn retain_in<'k, R>(
        &mut self,
        range: impl RangeBounds<R> + 'k,
        keep: impl for<'f> FnMut(<<T as Table>::Key as Value>::SelfType<'f>, T::SelfType<'f>) -> bool,
    ) -> Result<(), StoreError>
    where
        R: Borrow<<<T as Table>::Key as Value>::SelfType<'k>> + 'k,
    {
        Ok(self.0.retain_in(range, keep)?)
    }
}

/// A type stored as the values of a table, which defines that table
pub trait Table: Value + Sized + 'static {
    /// The type of the table's keys
    type Key: Key + 'static;

    /// The name and types of the table
    const DEFINITION: TableDefinition<'static, Self::Key, Self>;
}

/// An error from the message store
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The database has not been created yet
    #[error("no messages have been synchronized yet")]
    NotSynced,
    /// Another process kept the database open for too long
    #[error("the database is in use by another process")]
    Busy,
    /// The database could not be read or written
    #[error("database error: {0}")]
    Database(#[from] redb::Error),
}

impl From<DatabaseError> for StoreError {
    fn from(error: DatabaseError) -> Self {
        Self::Database(error.into())
    }
}
impl From<redb::TransactionError> for StoreError {
    fn from(error: redb::TransactionError) -> Self {
        Self::Database(error.into())
    }
}
impl From<TableError> for StoreError {
    fn from(error: TableError) -> Self {
        Self::Database(error.into())
    }
}
impl From<redb::StorageError> for StoreError {
    fn from(error: redb::StorageError) -> Self {
        Self::Database(error.into())
    }
}
impl From<redb::CommitError> for StoreError {
    fn from(error: redb::CommitError) -> Self {
        Self::Database(error.into())
    }
}

/// A value with a binary encoding for storage
///
/// Integers are encoded in little-endian order. Strings and lists are prefixed with their length,
/// and optional values with whether they are present.
pub trait Encode {
    /// Appends the encoding of the value to `buf`
    fn encode(&self, buf: &mut Vec<u8>);
}

/// A value read from the encoding written by its [`Encode`] implementation
pub trait Decode: Sized {
    /// Reads a value from the start of `buf`, advancing it past the value
    ///
    /// # Panics
    ///
    /// Only [`Encode`] writes the data, so malformed data is a bug and panics.
    fn decode(buf: &mut &[u8]) -> Self;
}

impl Encode for u8 {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_le_bytes());
    }
}

impl Decode for u8 {
    fn decode(buf: &mut &[u8]) -> Self {
        Self::from_le_bytes(take(buf))
    }
}

impl Encode for u32 {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_le_bytes());
    }
}

impl Decode for u32 {
    fn decode(buf: &mut &[u8]) -> Self {
        Self::from_le_bytes(take(buf))
    }
}

impl Encode for u64 {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_le_bytes());
    }
}

impl Decode for u64 {
    fn decode(buf: &mut &[u8]) -> Self {
        Self::from_le_bytes(take(buf))
    }
}

impl Encode for i64 {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_le_bytes());
    }
}

impl Decode for i64 {
    fn decode(buf: &mut &[u8]) -> Self {
        Self::from_le_bytes(take(buf))
    }
}

impl Encode for str {
    fn encode(&self, buf: &mut Vec<u8>) {
        (self.len() as u32).encode(buf);
        buf.extend_from_slice(self.as_bytes());
    }
}

impl Encode for String {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.as_str().encode(buf);
    }
}

impl Decode for String {
    fn decode(buf: &mut &[u8]) -> Self {
        let len = u32::decode(buf) as usize;
        let (bytes, rest) = buf.split_at(len);
        *buf = rest;
        str::from_utf8(bytes)
            .expect("stored strings are UTF-8")
            .to_owned()
    }
}

impl<T: Encode> Encode for Option<T> {
    fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            Some(value) => {
                1u8.encode(buf);
                value.encode(buf);
            }
            None => 0u8.encode(buf),
        }
    }
}

impl<T: Decode> Decode for Option<T> {
    fn decode(buf: &mut &[u8]) -> Self {
        match u8::decode(buf) {
            0 => None,
            _ => Some(T::decode(buf)),
        }
    }
}

impl<T: Encode> Encode for Vec<T> {
    fn encode(&self, buf: &mut Vec<u8>) {
        (self.len() as u32).encode(buf);
        for item in self {
            item.encode(buf);
        }
    }
}

impl<T: Decode> Decode for Vec<T> {
    fn decode(buf: &mut &[u8]) -> Self {
        let mut items = Self::new();
        for _ in 0..u32::decode(buf) {
            items.push(T::decode(buf));
        }
        items
    }
}

fn take<const N: usize>(buf: &mut &[u8]) -> [u8; N] {
    let (bytes, rest) = buf
        .split_first_chunk::<N>()
        .expect("stored value is complete");
    *buf = rest;
    *bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_encoded_values() {
        let mut buf = Vec::new();
        7u8.encode(&mut buf);
        u32::MAX.encode(&mut buf);
        u64::MAX.encode(&mut buf);
        (-1i64).encode(&mut buf);
        "Café".encode(&mut buf);
        Some("x".to_owned()).encode(&mut buf);
        None::<u64>.encode(&mut buf);
        vec![1u32, 2, 3].encode(&mut buf);

        let mut data = buf.as_slice();
        assert_eq!(u8::decode(&mut data), 7);
        assert_eq!(u32::decode(&mut data), u32::MAX);
        assert_eq!(u64::decode(&mut data), u64::MAX);
        assert_eq!(i64::decode(&mut data), -1);
        assert_eq!(String::decode(&mut data), "Café");
        assert_eq!(Option::<String>::decode(&mut data).as_deref(), Some("x"));
        assert_eq!(Option::<u64>::decode(&mut data), None);
        assert_eq!(Vec::<u32>::decode(&mut data), [1, 2, 3]);
        assert!(data.is_empty());
    }
}
