use anyhow::Result;
use rusqlite::{Connection, named_params, params, params_from_iter};

use super::DatId;

/// Escape SQL LIKE special characters (`%`, `_`, `\`) in a string so they
/// match literally when used with `ESCAPE '\'`.
pub(crate) fn escape_like(s: &str) -> String {
    s.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_")
}

// Id type that is generic but bound to a type to prevent accidentally using
// the wrong id and causing unintended consequences. Has Traits to allow it
// to be used in rusqlite transparently and is copy as it boild down to copying
// the captive integer.

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Id<T>(i64, std::marker::PhantomData<T>);

impl<T> Id<T> {
    fn new(id: i64) -> Self {
        Self(id, std::marker::PhantomData)
    }
}

impl<T> Clone for Id<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Id<T> {}

impl<T> From<i64> for Id<T> {
    fn from(id: i64) -> Self {
        Self::new(id)
    }
}

impl<T> HasId for Id<T> {
    fn id(&self) -> i64 {
        self.0
    }
}

impl<T> rusqlite::ToSql for Id<T> {
    fn to_sql(&self) -> Result<rusqlite::types::ToSqlOutput<'_>, rusqlite::Error> {
        Ok(self.0.into())
    }
}

impl<T> rusqlite::types::FromSql for Id<T> {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        i64::column_result(value).map(Self::new)
    }
}

pub trait HasId {
    fn id(&self) -> i64;
}

// SQLite stores integers as signed 64-bit values, but some of our fields
// (e.g. ROM / file sizes) are logically unsigned.  `StoredU64` converts
// between the Rust `u64` and the SQLite `i64` via a bitwise reinterpretation
// so that we never lose the top bit.

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StoredU64(pub u64);

impl From<u64> for StoredU64 {
    fn from(val: u64) -> Self {
        StoredU64(val)
    }
}

impl From<StoredU64> for u64 {
    fn from(val: StoredU64) -> Self {
        val.0
    }
}

impl rusqlite::ToSql for StoredU64 {
    #[inline]
    fn to_sql(&self) -> Result<rusqlite::types::ToSqlOutput<'_>, rusqlite::Error> {
        let str_value = self.0.to_string();
        Ok(rusqlite::types::ToSqlOutput::from(str_value))
    }
}

impl rusqlite::types::FromSql for StoredU64 {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        value.as_str().and_then(|s| {
            s.parse::<u64>()
                .map(StoredU64)
                .map_err(|_| rusqlite::types::FromSqlError::InvalidType)
        })
    }
}

pub trait Queryable: Sized {
    type IdType: HasId + rusqlite::ToSql + Copy;

    fn table_name() -> &'static str;
    fn fields() -> &'static str;
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self>;

    fn get_by_id(conn: &Connection, id: Self::IdType) -> Result<Self> {
        let record = sql_query_one!(conn, Self::table_name(), Self::fields(), where {id = id.id()}, Self::from_row)?;
        Ok(record)
    }

    fn get_all(conn: &Connection) -> Result<Vec<Self>> {
        let mut stmt = conn.prepare(format!("SELECT {} FROM {}", Self::fields(), Self::table_name()).as_str())?;
        let matches = stmt
            .query_map(params![], Self::from_row)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(matches)
    }

    fn get_by_ids<I>(conn: &Connection, ids: &I) -> Result<Vec<Self>>
    where
        for<'a> &'a I: IntoIterator<Item = &'a Self::IdType>,
    {
        let ids: Vec<_> = ids.into_iter().collect();
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let sql =
            format!("SELECT {} FROM {} WHERE id IN ({}) ORDER BY id", Self::fields(), Self::table_name(), placeholders);
        let mut stmt = conn.prepare(&sql)?;
        let matches = stmt
            .query_map(params_from_iter(ids), Self::from_row)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(matches)
    }
}

pub trait Deletable: Queryable {
    fn delete_by_id(conn: &Connection, id: Self::IdType) -> Result<bool> {
        let sql = format!("DELETE FROM {} WHERE id = :id", Self::table_name());
        let num_deleted = conn.execute(&sql, named_params! {":id": id.id()})?;
        Ok(num_deleted != 0)
    }
}

pub trait QueryableByDat: Queryable {
    fn get_by_dat(conn: &Connection, dat_id: DatId) -> Result<Vec<Self>> {
        let matches =
            sql_query!(conn, Self::table_name(), Self::fields(), where {dat_id = dat_id.id()}, Self::from_row)?;
        Ok(matches)
    }

    fn get_num_by_dat(conn: &Connection, dat_id: DatId) -> Result<usize> {
        let sql = format!("SELECT COUNT(*) FROM {} WHERE dat_id = :dat_id", Self::table_name());
        let count: i64 = conn.query_row(&sql, named_params! {":dat_id": dat_id.id()}, |row| row.get(0))?;
        Ok(count as usize)
    }
}

pub trait DeletableByDat: Queryable {
    fn delete_by_dat(conn: &Connection, dat_id: DatId) -> Result<usize> {
        let sql = format!("DELETE FROM {} WHERE dat_id = :dat_id", Self::table_name());
        let num_deleted = conn.execute(&sql, named_params! {":dat_id": dat_id.id()})?;
        Ok(num_deleted)
    }
}

pub trait FindableByName: Queryable {
    fn find_by_name(conn: &Connection, dat_id: DatId, name: &str, exact: bool) -> Result<Vec<Self>> {
        let matches = if exact {
            sql_query!(conn, Self::table_name(), Self::fields(), where {dat_id = dat_id.id(), name}, order by "name", Self::from_row)
        } else {
            let mut stmt = conn.prepare(
                format!(
                    "SELECT {} FROM {} WHERE dat_id = (?1) AND name LIKE (?2) ESCAPE '\\' ORDER BY name",
                    Self::fields(),
                    Self::table_name()
                )
                .as_str(),
            )?;
            stmt.query_map(params![dat_id, format!("%{}%", escape_like(name))], Self::from_row)?
                .collect::<Result<Vec<_>, _>>()
        }?;
        Ok(matches)
    }
}

pub trait Bindable {
    fn bind_params(&self) -> Vec<(&'static str, &dyn rusqlite::ToSql)>;
}

pub trait Insertable: Queryable
where
    Self::IdType: From<i64>,
{
    type NewType: Bindable;

    fn insert(conn: &Connection, new: Self::NewType) -> Result<Self> {
        let params = new.bind_params();
        let values: Vec<&str> = params.iter().map(|(name, _)| *name).collect();
        let columns: Vec<String> = values
            .iter()
            .map(|name| name.strip_prefix(":").unwrap_or(name).to_string())
            .collect();

        let sql = format!("INSERT INTO {} ({}) VALUES ({})", Self::table_name(), columns.join(", "), values.join(", "));

        conn.execute(&sql, params.as_slice())?;
        let raw_id = conn.last_insert_rowid();
        let id = Self::IdType::from(raw_id);
        Self::get_by_id(conn, id)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::escape_like;

    #[test]
    fn escape_like_special_chars() {
        assert_eq!(escape_like("100%"), "100\\%");
        assert_eq!(escape_like("a_b"), "a\\_b");
        assert_eq!(escape_like("a\\b"), "a\\\\b");
        assert_eq!(escape_like("normal"), "normal");
    }
}
