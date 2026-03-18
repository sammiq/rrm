use std::collections::HashMap;

use camino::Utf8Path;

use anyhow::{Result, bail};
use rusqlite::{Connection, Savepoint, Transaction, named_params, params, params_from_iter};

//macro that generates a select statement
macro_rules! sql_query_one {
    ($conn:expr, $table:expr, $fields:expr, where { $($where_field:ident $(= $where_value:expr)?),+ $(,)? }, $mapper:expr) => {{
        let wheres = vec![$(concat!(stringify!($where_field), " = :", stringify!($where_field))),+].join(" AND ");
        let sql = format!("SELECT {} FROM {} WHERE {}", $fields, $table, wheres);

        $conn.query_one(
            &sql,
            &[$(
                (concat!(":", stringify!($where_field)), &sql_query_one!(@value $where_field $(= $where_value)?) as &dyn rusqlite::ToSql)
            ),+] as &[(&str, &dyn rusqlite::ToSql)],
            $mapper
        )
    }};

    (@value $field:ident = $value:expr) => { $value };
    (@value $field:ident) => { $field };
}

macro_rules! sql_query {
    ($conn:expr, $table:expr, $fields:expr, where { $($where_field:ident $(= $where_value:expr)?),+ $(,)? }, order by $order:expr, $mapper:expr) => {{
        let wheres = vec![$(concat!(stringify!($where_field), " = :", stringify!($where_field))),+].join(" AND ");
        let sql = format!("SELECT {} FROM {} WHERE {} ORDER BY {}", $fields, $table, wheres, $order);

        $conn.prepare(&sql).and_then(|mut stmt| stmt.query_map(
            &[$(
                (concat!(":", stringify!($where_field)), &sql_query!(@value $where_field $(= $where_value)?) as &dyn rusqlite::ToSql)
            ),+] as &[(&str, &dyn rusqlite::ToSql)],
            $mapper
        ).and_then(|r| r.collect::<Result<Vec<_>, _>>()))
    }};

    ($conn:expr, $table:expr, $fields:expr, where { $($where_field:ident $(= $where_value:expr)?),+ $(,)? }, $mapper:expr) => {{
        let wheres = vec![$(concat!(stringify!($where_field), " = :", stringify!($where_field))),+].join(" AND ");
        let sql = format!("SELECT {} FROM {} WHERE {}", $fields, $table, wheres);

        $conn.prepare(&sql).and_then(|mut stmt| stmt.query_map(
            &[$(
                (concat!(":", stringify!($where_field)), &sql_query!(@value $where_field $(= $where_value)?) as &dyn rusqlite::ToSql)
            ),+] as &[(&str, &dyn rusqlite::ToSql)],
            $mapper
        ).and_then(|r| r.collect::<Result<Vec<_>, _>>()))
    }};

    (@value $field:ident = $value:expr) => { $value };
    (@value $field:ident) => { $field };
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

pub trait HasId {
    fn id(&self) -> i64;
}

impl<T> HasId for Id<T> {
    fn id(&self) -> i64 {
        self.0
    }
}

impl<T> From<i64> for Id<T> {
    fn from(v: i64) -> Self {
        Self::new(v)
    }
}

impl<T> rusqlite::ToSql for Id<T> {
    #[inline]
    fn to_sql(&self) -> Result<rusqlite::types::ToSqlOutput<'_>, rusqlite::Error> {
        Ok(rusqlite::types::ToSqlOutput::from(self.0))
    }
}

impl<T> rusqlite::types::FromSql for Id<T> {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        value
            .as_i64()
            .map(Self::new)
            .map_err(|_| rusqlite::types::FromSqlError::InvalidType)
    }
}

// Wrapper for unsigned integers that are too big to store directly in SQLite.
// Stores the value as a string in the database and has conversions to allow it
// to be used transparently in the rest of the codebase. Has to be done this way
// because of the rules rust has around orphan traits which prevent
// implementing ToSql and FromSql directly on u64. :(
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StoredU64(pub u64);

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
    type IdType: HasId + rusqlite::ToSql;

    fn table_name() -> &'static str;
    fn fields() -> &'static str;
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self>;

    fn get_by_id(conn: &Connection, id: &Self::IdType) -> Result<Self> {
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
    fn delete_by_id(conn: &Connection, id: &Self::IdType) -> Result<bool> {
        let sql = format!("DELETE FROM {} WHERE id = :id", Self::table_name());
        let num_deleted = conn.execute(&sql, named_params! {":id": id.id()})?;
        Ok(num_deleted != 0)
    }
}

pub trait QueryableByDat: Queryable {
    fn get_by_dat(conn: &Connection, dat_id: &DatId) -> Result<Vec<Self>> {
        let matches =
            sql_query!(conn, Self::table_name(), Self::fields(), where {dat_id = dat_id.id()}, Self::from_row)?;
        Ok(matches)
    }

    fn get_num_by_dat(conn: &Connection, dat_id: &DatId) -> Result<usize> {
        let sql = format!("SELECT COUNT(*) FROM {} WHERE dat_id = :dat_id", Self::table_name());
        let count: i64 = conn.query_row(&sql, named_params! {":dat_id": dat_id.id()}, |row| row.get(0))?;
        Ok(count as usize)
    }
}

pub trait DeletableByDat: Queryable {
    fn delete_by_dat(conn: &Connection, dat_id: &DatId) -> Result<usize> {
        let sql = format!("DELETE FROM {} WHERE dat_id = :dat_id", Self::table_name());
        let num_deleted = conn.execute(&sql, named_params! {":dat_id": dat_id.id()})?;
        Ok(num_deleted)
    }
}

pub trait FindableByName: Queryable {
    fn find_by_name(conn: &Connection, dat_id: &DatId, name: &str, exact: bool) -> Result<Vec<Self>> {
        let matches = if exact {
            sql_query!(conn, Self::table_name(), Self::fields(), where {dat_id = dat_id.id(), name}, order by "name", Self::from_row)
        } else {
            let mut stmt = conn.prepare(
                format!(
                    "SELECT {} FROM {} WHERE dat_id = (?1) AND name LIKE (?2) ORDER BY name",
                    Self::fields(),
                    Self::table_name()
                )
                .as_str(),
            )?;
            stmt.query_map(params![dat_id, format!("%{name}%")], Self::from_row)?
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

    fn insert(conn: &Connection, new: &Self::NewType) -> Result<Self> {
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
        Self::get_by_id(conn, &id)
    }
}

pub type DatId = Id<DatRecord>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DatRecord {
    pub id: DatId,
    pub name: String,
    pub description: String,
    pub version: String,
    pub author: String,
    pub hash_type: String,
}

impl Queryable for DatRecord {
    type IdType = DatId;

    fn table_name() -> &'static str {
        "dats"
    }

    fn fields() -> &'static str {
        "id, name, description, version, author, hash_type"
    }

    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(DatRecord {
            id: row.get("id")?,
            name: row.get("name")?,
            description: row.get("description")?,
            version: row.get("version")?,
            author: row.get("author")?,
            hash_type: row.get("hash_type")?,
        })
    }
}

impl Deletable for DatRecord {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NewDat {
    pub name: String,
    pub description: String,
    pub version: String,
    pub author: String,
    pub hash_type: String,
}

impl Bindable for NewDat {
    fn bind_params(&self) -> Vec<(&'static str, &dyn rusqlite::ToSql)> {
        named_params! {
            ":name": self.name,
            ":description": self.description,
            ":version": self.version,
            ":author": self.author,
            ":hash_type": self.hash_type
        }
        .to_vec()
    }
}

impl Insertable for DatRecord {
    type NewType = NewDat;
}

pub type SetId = Id<SetRecord>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SetRecord {
    pub id: SetId,

    pub dat_id: DatId,
    pub name: String,
}

impl Queryable for SetRecord {
    type IdType = SetId;

    fn table_name() -> &'static str {
        "sets"
    }

    fn fields() -> &'static str {
        "id, dat_id, name"
    }

    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(SetRecord {
            id: row.get("id")?,
            dat_id: row.get("dat_id")?,
            name: row.get("name")?,
        })
    }
}

impl QueryableByDat for SetRecord {}
impl DeletableByDat for SetRecord {}
impl FindableByName for SetRecord {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NewSet {
    pub dat_id: DatId,
    pub name: String,
}

impl Bindable for NewSet {
    fn bind_params(&self) -> Vec<(&'static str, &dyn rusqlite::ToSql)> {
        named_params! {
            ":dat_id": self.dat_id,
            ":name": self.name
        }
        .to_vec()
    }
}

impl Insertable for SetRecord {
    type NewType = NewSet;
}

pub type RomId = Id<RomRecord>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RomRecord {
    pub id: RomId,

    pub dat_id: DatId, //denormalized to avoid N+1 queries

    pub set_id: SetId,
    pub name: String,
    pub size: u64,
    pub hash: String,
}

impl Queryable for RomRecord {
    type IdType = RomId;

    fn table_name() -> &'static str {
        "roms"
    }

    fn fields() -> &'static str {
        "id, dat_id, set_id, name, size, hash"
    }

    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(RomRecord {
            id: row.get("id")?,
            dat_id: row.get("dat_id")?,
            set_id: row.get("set_id")?,
            name: row.get("name")?,
            size: row.get::<_, StoredU64>("size")?.0,
            hash: row.get("hash")?,
        })
    }
}

impl QueryableByDat for RomRecord {}
impl DeletableByDat for RomRecord {}
impl FindableByName for RomRecord {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NewRom {
    pub dat_id: DatId, //denormalized to avoid N+1 queries

    pub set_id: SetId,
    pub name: String,
    pub size: StoredU64,
    pub hash: String,
}

impl Bindable for NewRom {
    fn bind_params(&self) -> Vec<(&'static str, &dyn rusqlite::ToSql)> {
        named_params! {
            ":dat_id": self.dat_id,
            ":set_id": self.set_id,
            ":name": self.name,
            ":size": self.size,
            ":hash": self.hash,
        }
        .to_vec()
    }
}

impl Insertable for RomRecord {
    type NewType = NewRom;
}

pub type DirId = Id<DirRecord>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DirRecord {
    pub id: DirId,

    pub dat_id: DatId,
    pub path: String,
}

impl Queryable for DirRecord {
    type IdType = DirId;

    fn table_name() -> &'static str {
        "dirs"
    }

    fn fields() -> &'static str {
        "id, dat_id, path"
    }

    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(DirRecord {
            id: row.get("id")?,
            dat_id: row.get("dat_id")?,
            path: row.get("path")?,
        })
    }
}

impl Deletable for DirRecord {}
impl QueryableByDat for DirRecord {}
impl DeletableByDat for DirRecord {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NewDir {
    pub dat_id: DatId,
    pub path: String,
}

impl Bindable for NewDir {
    fn bind_params(&self) -> Vec<(&'static str, &dyn rusqlite::ToSql)> {
        named_params! {
            ":dat_id": self.dat_id,
            ":path": self.path,
        }
        .to_vec()
    }
}

impl Insertable for DirRecord {
    type NewType = NewDir;
}

pub type FileId = Id<FileRecord>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FileRecord {
    pub id: FileId,

    pub dat_id: DatId, //denormalized to avoid N+1 queries

    pub dir_id: DirId,
    pub name: String,
    pub size: u64,
    pub hash: String,
}

impl Queryable for FileRecord {
    type IdType = FileId;

    fn table_name() -> &'static str {
        "files"
    }

    fn fields() -> &'static str {
        "id, dat_id, dir_id, name, size, hash"
    }

    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(FileRecord {
            id: row.get("id")?,
            dat_id: row.get("dat_id")?,
            dir_id: row.get("dir_id")?,
            name: row.get("name")?,
            size: row.get::<_, StoredU64>("size")?.0,
            hash: row.get("hash")?,
        })
    }
}

impl Deletable for FileRecord {}
impl QueryableByDat for FileRecord {}
impl DeletableByDat for FileRecord {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NewFile {
    pub dat_id: DatId, //denormalized to avoid N+1 queries

    pub dir_id: DirId,
    pub name: String,
    pub size: StoredU64,
    pub hash: String,
}

impl Bindable for NewFile {
    fn bind_params(&self) -> Vec<(&'static str, &dyn rusqlite::ToSql)> {
        named_params! {
            ":dat_id": self.dat_id,
            ":dir_id": self.dir_id,
            ":name": self.name,
            ":size": self.size,
            ":hash": self.hash,
        }
        .to_vec()
    }
}

impl Insertable for FileRecord {
    type NewType = NewFile;
}

pub type MatchId = Id<MatchRecord>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MatchRecord {
    pub id: MatchId,
    pub dat_id: DatId, //denormalized to avoid N+1 queries

    pub file_id: FileId,
    pub status: MatchStatus,
    pub set_id: SetId,
    pub rom_id: RomId,
}

impl Queryable for MatchRecord {
    type IdType = MatchId;

    fn table_name() -> &'static str {
        "matches"
    }

    fn fields() -> &'static str {
        "id, dat_id, file_id, status, set_id, rom_id"
    }

    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(MatchRecord {
            id: row.get("id")?,
            dat_id: row.get("dat_id")?,
            file_id: row.get("file_id")?,

            status: row.get("status")?,
            set_id: row.get("set_id")?,
            rom_id: row.get("rom_id")?,
        })
    }
}

impl Deletable for MatchRecord {}
impl QueryableByDat for MatchRecord {}
impl DeletableByDat for MatchRecord {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NewMatch {
    pub dat_id: DatId, //denormalized to avoid N+1 queries

    pub file_id: FileId,
    pub status: MatchStatus,
    pub set_id: SetId,
    pub rom_id: RomId,
}

impl Bindable for NewMatch {
    fn bind_params(&self) -> Vec<(&'static str, &dyn rusqlite::ToSql)> {
        named_params! {
            ":dat_id": self.dat_id,
            ":file_id": self.file_id,

            ":status": self.status,
            ":set_id": self.set_id,
            ":rom_id": self.rom_id,
        }
        .to_vec()
    }
}

impl Insertable for MatchRecord {
    type NewType = NewMatch;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum MatchStatus {
    Hash,
    Name,
    Match,
}

impl rusqlite::types::FromSql for MatchStatus {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        value.as_str().and_then(|s| match s {
            "hash" => Ok(MatchStatus::Hash),
            "name" => Ok(MatchStatus::Name),
            "match" => Ok(MatchStatus::Match),
            _ => Err(rusqlite::types::FromSqlError::InvalidType),
        })
    }
}

impl rusqlite::ToSql for MatchStatus {
    #[inline]
    fn to_sql(&self) -> Result<rusqlite::types::ToSqlOutput<'_>, rusqlite::Error> {
        let str_value = match self {
            MatchStatus::Hash => "hash",
            MatchStatus::Name => "name",
            MatchStatus::Match => "match",
        };
        Ok(rusqlite::types::ToSqlOutput::from(str_value))
    }
}

#[allow(dead_code)]
impl DatRecord {
    pub fn get_sets(&self, conn: &Connection) -> Result<Vec<SetRecord>> {
        SetRecord::get_by_dat(conn, &self.id)
    }

    pub fn get_roms(&self, conn: &Connection) -> Result<Vec<RomRecord>> {
        RomRecord::get_by_dat(conn, &self.id)
    }

    pub fn get_directories(&self, conn: &Connection) -> Result<Vec<DirRecord>> {
        DirRecord::get_by_dat(conn, &self.id)
    }
}

impl SetRecord {
    pub fn get_roms(&self, conn: &Connection) -> Result<Vec<RomRecord>> {
        RomRecord::get_by_set(conn, &self.id)
    }
}

impl RomRecord {
    fn get_by_set(conn: &Connection, set_id: &SetId) -> Result<Vec<Self>> {
        let matches = sql_query!(conn, Self::table_name(), Self::fields(), where {set_id}, Self::from_row)?;
        Ok(matches)
    }

    pub fn find_by_hash_in_dat(conn: &Connection, dat_id: &DatId, hash: &str) -> Result<Vec<RomRecord>> {
        let matches = sql_query!(conn, Self::table_name(), Self::fields(), where {dat_id, hash}, Self::from_row)?;
        Ok(matches)
    }

    pub fn get_by_sets<I>(conn: &Connection, set_ids: &I) -> Result<HashMap<SetId, Vec<Self>>>
    where
        for<'a> &'a I: IntoIterator<Item = &'a SetId>,
    {
        let set_ids: Vec<_> = set_ids.into_iter().collect();
        let mut map: HashMap<SetId, Vec<Self>> = HashMap::new();
        if set_ids.is_empty() {
            return Ok(map);
        }
        let placeholders = set_ids.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let sql = format!(
            "SELECT {} FROM {} WHERE set_id IN ({}) ORDER BY id",
            Self::fields(),
            Self::table_name(),
            placeholders
        );
        let mut stmt = conn.prepare(&sql)?;
        let matches = stmt
            .query_map(params_from_iter(set_ids), Self::from_row)?
            .collect::<Result<Vec<_>, _>>()?;
        for m in matches {
            map.entry(m.set_id).or_default().push(m);
        }
        Ok(map)
    }
}

impl DirRecord {
    pub fn get_by_path(conn: &Connection, path: &str) -> Result<Vec<DirRecord>> {
        let matches =
            sql_query!(conn, Self::table_name(), DirRecord::fields(), where {path}, order by "path", Self::from_row)?;
        Ok(matches)
    }

    pub fn find_by_path_in_dat(conn: &Connection, dat_id: &DatId, path: &str) -> Result<Option<DirRecord>> {
        match sql_query_one!(conn, Self::table_name(), Self::fields(), where {path, dat_id}, Self::from_row
        ) {
            Ok(dir) => Ok(Some(dir)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => bail!(e),
        }
    }

    pub fn get_children(&self, conn: &Connection) -> Result<Vec<DirRecord>> {
        let sql = format!(
            "SELECT {} FROM {} WHERE dat_id = :dat_id AND path LIKE :prefix ORDER BY path",
            Self::fields(),
            Self::table_name(),
        );
        let prefix = format!("{}/%", self.path);
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt
            .query_map(named_params! { ":dat_id": self.dat_id, ":prefix": prefix }, Self::from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn get_files(&self, conn: &Connection) -> Result<Vec<FileRecord>> {
        FileRecord::get_by_dir(conn, &self.id)
    }

    pub fn find_files(&self, conn: &Connection, name: &str, exact: bool) -> Result<Vec<FileRecord>> {
        FileRecord::find_by_name_in_dir(conn, &self.id, name, exact)
    }

    pub fn delete_matches(&self, conn: &Connection) -> Result<usize> {
        MatchRecord::delete_by_dir_id(conn, &self.id)
    }

    pub fn delete_files(&self, conn: &Connection) -> Result<usize> {
        FileRecord::delete_files(conn, &self.id)
    }

    pub fn relink_dirs(conn: &Connection, old_dat_id: &DatId, new_dat_id: &DatId) -> Result<usize> {
        let sql = format!("UPDATE {} SET dat_id = :new_dat_id WHERE dat_id = :old_dat_id", Self::table_name());
        let num_updated = conn.execute(
            &sql,
            named_params! {
                ":new_dat_id": new_dat_id.id(),
                ":old_dat_id": old_dat_id.id(),
            },
        )?;
        Ok(num_updated)
    }
}

impl FileRecord {
    fn get_by_dir(conn: &Connection, dir_id: &DirId) -> Result<Vec<Self>> {
        let matches =
            sql_query!(conn, Self::table_name(), Self::fields(), where {dir_id}, order by "name", Self::from_row)?;
        Ok(matches)
    }

    pub fn find_by_name_in_dir(conn: &Connection, dir_id: &DirId, name: &str, exact: bool) -> Result<Vec<FileRecord>> {
        let matches = if exact {
            sql_query!(conn, Self::table_name(), FileRecord::fields(), where {dir_id, name}, order by "name", Self::from_row)
        } else {
            let mut stmt = conn.prepare(
                format!(
                    "SELECT {} FROM {} WHERE dir_id = (?1) AND name LIKE (?2) ORDER BY name",
                    Self::fields(),
                    Self::table_name()
                )
                .as_str(),
            )?;

            stmt.query_map(params![dir_id, format!("%{}%", name)], FileRecord::from_row)?
                .collect::<Result<Vec<_>, _>>()
        }?;
        Ok(matches)
    }

    pub fn delete_files(conn: &Connection, dir_id: &DirId) -> Result<usize> {
        let sql = format!("DELETE FROM {} WHERE dir_id = :dir_id", Self::table_name());
        let num_deleted = conn.execute(&sql, named_params! {":dir_id": dir_id})?;
        Ok(num_deleted)
    }

    pub fn relink_files(conn: &Connection, old_dat_id: &DatId, new_dat_id: &DatId) -> Result<usize> {
        let sql = format!("UPDATE {} SET dat_id = :new_dat_id WHERE dat_id = :old_dat_id", Self::table_name());
        let num_updated = conn.execute(
            &sql,
            named_params! {
                ":new_dat_id": new_dat_id.id(),
                ":old_dat_id": old_dat_id.id(),
            },
        )?;
        Ok(num_updated)
    }

    pub fn update_name(&self, conn: &Connection, name: &str) -> Result<Self> {
        let sql = format!("UPDATE {} SET name = :name WHERE id = :id", Self::table_name());
        conn.execute(
            &sql,
            named_params! {
                ":id": self.id,
                ":name": name,
            },
        )?;
        Ok(Self {
            id: self.id,
            dat_id: self.dat_id,
            dir_id: self.dir_id,
            name: name.to_string(),
            size: self.size,
            hash: self.hash.clone(),
        })
    }

    pub fn delete_matches(&self, conn: &Connection) -> Result<usize> {
        MatchRecord::delete_by_file_id(conn, &self.id)
    }
}

impl MatchRecord {
    pub fn delete_by_file_id(conn: &Connection, file_id: &FileId) -> Result<usize> {
        let sql = format!("DELETE FROM {} WHERE file_id = :file_id", Self::table_name());
        let num_deleted = conn.execute(&sql, named_params! {":file_id": file_id})?;
        Ok(num_deleted)
    }

    pub fn delete_by_dir_id(conn: &Connection, dir_id: &DirId) -> Result<usize> {
        let sql = format!(
            "DELETE FROM {matches} WHERE file_id IN (SELECT id FROM {files} WHERE dir_id = :dir_id)",
            matches = Self::table_name(),
            files = FileRecord::table_name(),
        );
        let num_deleted = conn.execute(&sql, named_params! {":dir_id": dir_id})?;
        Ok(num_deleted)
    }

    pub fn find_by_status_for_file(conn: &Connection, file_id: &FileId, status: &MatchStatus) -> Result<Vec<Self>> {
        let matches = sql_query!(conn, Self::table_name(), Self::fields(), where {file_id, status}, order by "id", Self::from_row)?;
        Ok(matches)
    }

    pub fn update(&self, conn: &Connection, status: &MatchStatus) -> Result<Self> {
        let sql = format!("UPDATE {} SET status = :status WHERE id = :id", Self::table_name());
        conn.execute(
            &sql,
            named_params! {
                ":id": self.id,
                ":status": status,
            },
        )?;
        Ok(Self {
            id: self.id,
            dat_id: self.dat_id,
            file_id: self.file_id,
            status: status.clone(),
            set_id: self.set_id,
            rom_id: self.rom_id,
        })
    }
}

const SCHEMA_VERSION: i64 = 2;

pub fn open_or_create<P: AsRef<Utf8Path>>(db_path: P) -> Result<Connection> {
    let mut conn = Connection::open(db_path.as_ref())?;
    conn.execute_batch("PRAGMA foreign_keys = OFF;")?;

    // Create the schema_version table first so we can detect a fresh database.
    conn.execute("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY);", ())?;

    let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
    let version: Option<i64> =
        tx.query_row("SELECT MAX(version) FROM schema_version", [], |row| row.get(0))?;

    if version.is_none() {
        create_schema(&tx)?;
    } else {
        run_migrations(&tx, db_path.as_ref(), version.unwrap())?;
    }

    tx.commit()?;
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;
    Ok(conn)
}

fn create_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE dats (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            description VARCHAR NOT NULL,
            version VARCHAR NOT NULL,
            author VARCHAR NOT NULL,
            hash_type VARCHAR NOT NULL
        );
        CREATE TABLE sets (
            id INTEGER PRIMARY KEY,
            dat_id INTEGER NOT NULL,
            name VARCHAR NOT NULL,
            FOREIGN KEY (dat_id) REFERENCES dats(id)
        );
        CREATE TABLE roms (
            id INTEGER PRIMARY KEY,
            dat_id INTEGER NOT NULL,
            set_id INTEGER NOT NULL,
            name VARCHAR NOT NULL,
            size VARCHAR NOT NULL,
            hash VARCHAR NOT NULL,
            FOREIGN KEY (dat_id) REFERENCES dats(id),
            FOREIGN KEY (set_id) REFERENCES sets(id)
        );
        CREATE TABLE dirs (
            id INTEGER PRIMARY KEY,
            dat_id INTEGER NOT NULL,
            path VARCHAR NOT NULL,
            FOREIGN KEY (dat_id) REFERENCES dats(id),
            UNIQUE(path, dat_id)
        );
        CREATE TABLE files (
            id INTEGER PRIMARY KEY,
            dat_id INTEGER NOT NULL,
            dir_id INTEGER NOT NULL,
            name VARCHAR NOT NULL,
            size VARCHAR NOT NULL,
            hash VARCHAR NOT NULL,
            FOREIGN KEY (dat_id) REFERENCES dats(id),
            FOREIGN KEY (dir_id) REFERENCES dirs(id),
            UNIQUE(dir_id, name)
        );
        CREATE TABLE matches (
            id INTEGER PRIMARY KEY,
            dat_id INTEGER NOT NULL,
            file_id INTEGER NOT NULL,
            status VARCHAR NOT NULL,
            set_id INTEGER NOT NULL,
            rom_id INTEGER NOT NULL,
            FOREIGN KEY (dat_id) REFERENCES dats(id),
            FOREIGN KEY (file_id) REFERENCES files(id),
            FOREIGN KEY (set_id) REFERENCES sets(id),
            FOREIGN KEY (rom_id) REFERENCES roms(id)
        );
        CREATE INDEX idx_dat_sets ON sets(dat_id);
        CREATE INDEX idx_dat_sets_name ON sets(dat_id, name);
        CREATE INDEX idx_set_roms ON roms(set_id);
        CREATE INDEX idx_dat_roms_name ON roms(dat_id, name);
        CREATE INDEX idx_dat_roms_hash ON roms(dat_id, hash);
        CREATE INDEX idx_dat_dirs ON dirs(dat_id);
        CREATE INDEX idx_dat_dirs_path ON dirs(dat_id, path);
        CREATE INDEX idx_dir_files ON files(dir_id);
        CREATE INDEX idx_dir_files_name ON files(dir_id, name);
        CREATE INDEX idx_matches_file_id ON matches(file_id);
        CREATE INDEX idx_matches_set_id ON matches(set_id);
        CREATE INDEX idx_matches_rom_id ON matches(rom_id);
        CREATE INDEX idx_matches_dat_id ON matches(dat_id);
        "#,
    )?;
    conn.execute("INSERT INTO schema_version (version) VALUES (?1)", [SCHEMA_VERSION])?;
    Ok(())
}

fn run_migrations(conn: &Connection, db_path: &Utf8Path, version: i64) -> Result<()> {
    if version < 1 {
        // Before running any migrations, back up the database just in case.
        let backup_file = db_path.with_extension("v0.bak");
        std::fs::copy(db_path, &backup_file)?;

        // Migration 1: Move matches from duplicating files to a new table referenced by the file record.
        // This stops having the need for multiple file entries for the same file when it matches multiple roms
        // as well as allowing us to ditch the none status.
        // NOTE: SQLite does not support altering FK references in ALTER statements, which makes copying the entire
        // table necessary, this is actually useful here as we need to deduplicate the files table
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS matches (
                id INTEGER PRIMARY KEY,
                dat_id INTEGER NOT NULL,
                file_id INTEGER NOT NULL,
                status VARCHAR NOT NULL,
                set_id INTEGER NOT NULL,
                rom_id INTEGER NOT NULL,
                FOREIGN KEY (dat_id) REFERENCES dats(id),
                FOREIGN KEY (file_id) REFERENCES files(id),
                FOREIGN KEY (rom_id) REFERENCES roms(id),
                FOREIGN KEY (set_id) REFERENCES sets(id)
            );
            CREATE INDEX IF NOT EXISTS idx_matches_file_id ON matches(file_id);
            CREATE INDEX IF NOT EXISTS idx_matches_set_id ON matches(set_id);
            CREATE INDEX IF NOT EXISTS idx_matches_rom_id ON matches(rom_id);
            CREATE INDEX IF NOT EXISTS idx_matches_dat_id ON matches(dat_id);

            CREATE TEMP TABLE id_map AS
                SELECT f.id AS old_id, MIN(f.id) OVER (PARTITION BY f.dir_id, f.name) AS new_id
                FROM files f;

            INSERT INTO matches (file_id, set_id, rom_id, status, dat_id)
                SELECT i.new_id, f.set_id, f.rom_id, f.status, s.dat_id FROM files f
                JOIN sets s ON f.set_id = s.id
                JOIN id_map i ON f.id = i.old_id
                WHERE f.status != 'none';

            CREATE TABLE IF NOT EXISTS files_new (
                id INTEGER PRIMARY KEY,
                dat_id INTEGER NOT NULL,
                dir_id INTEGER NOT NULL,
                name VARCHAR NOT NULL,
                size VARCHAR NOT NULL,
                hash VARCHAR NOT NULL,
                FOREIGN KEY (dat_id) REFERENCES dats(id),
                FOREIGN KEY (dir_id) REFERENCES dirs(id),
                UNIQUE(dir_id, name)
            );

            INSERT INTO files_new (id, dat_id, dir_id, name, size, hash)
                SELECT MIN(f.id), d.dat_id, f.dir_id, f.name, f.size, f.hash FROM files f
                JOIN dirs d ON f.dir_id = d.id
                GROUP BY f.dir_id, f.name;

            DROP TABLE files;

            ALTER TABLE files_new RENAME TO files;
            CREATE INDEX IF NOT EXISTS idx_dir_files ON files(dir_id);
            CREATE INDEX IF NOT EXISTS idx_dir_files_name ON files(dir_id, name);
            "#,
        )?;
        conn.execute("INSERT INTO schema_version (version) VALUES (1)", [])?;
    }

    if version < 2 {
        // Migration 2: Drop parent_id from dirs. The self-referential FK caused constraint
        // violations when stale parent dirs were deleted without first deleting child dirs.
        // parent_id was only used to find children, which is now done by path-prefix query.
        conn.execute_batch(
            r#"
            CREATE TABLE dirs_new (
                id INTEGER PRIMARY KEY,
                dat_id INTEGER NOT NULL,
                path VARCHAR NOT NULL,
                FOREIGN KEY (dat_id) REFERENCES dats(id),
                UNIQUE(path, dat_id)
            );

            INSERT INTO dirs_new (id, dat_id, path)
                SELECT id, dat_id, path FROM dirs;

            DROP TABLE dirs;

            ALTER TABLE dirs_new RENAME TO dirs;
            CREATE INDEX IF NOT EXISTS idx_dirs_dat_id ON dirs(dat_id);
            "#,
        )?;
        conn.execute("INSERT INTO schema_version (version) VALUES (2)", [])?;
    }

    Ok(())
}

pub fn with_transaction<T, F: FnOnce(&Transaction) -> Result<T>>(conn: &mut Connection, op: F) -> Result<T> {
    let tx = conn.transaction()?;
    let result = op(&tx)?;
    tx.commit()?;
    Ok(result)
}

pub fn with_transaction_mut<T, F: FnOnce(&mut Transaction) -> Result<T>>(conn: &mut Connection, op: F) -> Result<T> {
    let mut tx = conn.transaction()?;
    let result = op(&mut tx)?;
    tx.commit()?;
    Ok(result)
}

pub fn with_savepoint<T, F: FnOnce(&Savepoint) -> Result<T>>(conn: &mut Transaction, op: F) -> Result<T> {
    let mut sp = conn.savepoint()?;
    let result = op(&mut sp)?;
    sp.commit()?;
    Ok(result)
}
