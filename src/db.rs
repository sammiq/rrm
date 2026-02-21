#![allow(dead_code)]

use camino::Utf8Path;

use anyhow::{Result, bail};
use rusqlite::{Connection, named_params, params};

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
// to be used in rusqlite transparently.

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Id<T>(i64, std::marker::PhantomData<T>);

impl<T> Id<T> {
    fn new(id: i64) -> Self {
        Self(id, std::marker::PhantomData)
    }
}

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
pub struct SizeWrapper(pub u64);

impl rusqlite::ToSql for SizeWrapper {
    #[inline]
    fn to_sql(&self) -> Result<rusqlite::types::ToSqlOutput<'_>, rusqlite::Error> {
        let str_value = self.0.to_string();
        Ok(rusqlite::types::ToSqlOutput::from(str_value))
    }
}

impl rusqlite::types::FromSql for SizeWrapper {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        value.as_str().and_then(|s| {
            s.parse::<u64>()
                .map(SizeWrapper)
                .map_err(|_| rusqlite::types::FromSqlError::InvalidType)
        })
    }
}

pub trait Queryable: Sized {
    type IdType: HasId;

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
}

pub trait Deletable: Queryable {
    // move the id as we are removing the record so the id will no longer
    // be valid and we want to avoid accidentally using it after deletion
    fn delete(conn: &Connection, id: Self::IdType) -> Result<bool> {
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
    pub dat_id: DatId,
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
            size: row.get::<_, SizeWrapper>("size")?.0,
            hash: row.get("hash")?,
        })
    }
}

impl QueryableByDat for RomRecord {}
impl DeletableByDat for RomRecord {}
impl FindableByName for RomRecord {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NewRom {
    pub dat_id: DatId,
    pub set_id: SetId,
    pub name: String,
    pub size: SizeWrapper,
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
    pub parent_id: Option<DirId>,
}

impl Queryable for DirRecord {
    type IdType = DirId;

    fn table_name() -> &'static str {
        "dirs"
    }

    fn fields() -> &'static str {
        "id, dat_id, path, parent_id"
    }

    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(DirRecord {
            id: row.get("id")?,
            dat_id: row.get("dat_id")?,
            path: row.get("path")?,
            parent_id: row.get("parent_id")?,
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
    pub parent_id: Option<DirId>,
}

impl Bindable for NewDir {
    fn bind_params(&self) -> Vec<(&'static str, &dyn rusqlite::ToSql)> {
        named_params! {
            ":dat_id": self.dat_id,
            ":path": self.path,
            ":parent_id": self.parent_id,
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
    pub dir_id: DirId,
    pub name: String,
    pub size: u64,
    pub hash: String,
    pub status: MatchStatus,
}

impl Queryable for FileRecord {
    type IdType = FileId;

    fn table_name() -> &'static str {
        "files"
    }

    fn fields() -> &'static str {
        "id, dir_id, name, size, hash, status, set_id, rom_id"
    }

    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        let status = match row.get::<_, String>("status")?.as_ref() {
            "hash" => MatchStatus::Hash {
                set_id: row.get("set_id")?,
                rom_id: row.get("rom_id")?,
            },
            "name" => MatchStatus::Name {
                set_id: row.get("set_id")?,
                rom_id: row.get("rom_id")?,
            },
            "match" => MatchStatus::Match {
                set_id: row.get("set_id")?,
                rom_id: row.get("rom_id")?,
            },
            _ => MatchStatus::None,
        };
        Ok(FileRecord {
            id: row.get("id")?,
            dir_id: row.get("dir_id")?,
            name: row.get("name")?,
            size: row.get::<_, SizeWrapper>("size")?.0,
            hash: row.get("hash")?,
            status,
        })
    }
}

impl Deletable for FileRecord {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NewFile {
    pub dir_id: DirId,
    pub name: String,
    pub size: SizeWrapper,
    pub hash: String,
    // cannot make this bindable as it maps to multiple columns and the values depend on the match status
    pub status: MatchStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum MatchStatus {
    None,
    Hash { set_id: SetId, rom_id: RomId },
    Name { set_id: SetId, rom_id: RomId },
    Match { set_id: SetId, rom_id: RomId },
}

impl MatchStatus {
    fn as_str(&self) -> &str {
        match self {
            Self::None => "none",
            Self::Hash { .. } => "hash",
            Self::Name { .. } => "name",
            Self::Match { .. } => "match",
        }
    }

    pub fn ids(&self) -> Option<(SetId, RomId)> {
        match self {
            Self::None => None,
            Self::Hash { set_id, rom_id } | Self::Name { set_id, rom_id } | Self::Match { set_id, rom_id } => {
                Some((set_id.clone(), rom_id.clone()))
            }
        }
    }
}

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
        let matches =
            sql_query!(conn, Self::table_name(), Self::fields(), where {set_id}, Self::from_row)?;
        Ok(matches)
    }

    pub fn get_by_hash(conn: &Connection, dat_id: &DatId, hash: &str) -> Result<Vec<RomRecord>> {
        let matches = sql_query!(conn, Self::table_name(), Self::fields(), where {dat_id, hash}, Self::from_row)?;
        Ok(matches)
    }
}

impl DirRecord {
    pub fn get_by_path(conn: &Connection, path: &str) -> Result<Vec<DirRecord>> {
        let matches =
            sql_query!(conn, Self::table_name(), DirRecord::fields(), where {path}, order by "path", Self::from_row)?;
        Ok(matches)
    }

    pub fn get_by_dat_path(conn: &Connection, dat_id: &DatId, path: &str) -> Result<Option<DirRecord>> {
        match sql_query_one!(conn, Self::table_name(), Self::fields(), where {path, dat_id}, Self::from_row
        ) {
            Ok(dir) => Ok(Some(dir)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => bail!(e),
        }
    }

    pub fn get_children(&self, conn: &Connection) -> Result<Vec<DirRecord>> {
        let matches = sql_query!(conn, Self::table_name(), Self::fields(), where {parent_id = self.id}, order by "path", Self::from_row)?;
        Ok(matches)
    }

    pub fn get_files(&self, conn: &Connection) -> Result<Vec<FileRecord>> {
        FileRecord::get_by_dir(conn, &self.id)
    }

    pub fn find_files(&self, conn: &Connection, name: &str, exact: bool) -> Result<Vec<FileRecord>> {
        FileRecord::find_by_name(conn, &self.id, name, exact)
    }

    pub fn delete_files(&self, conn: &Connection) -> Result<usize> {
        FileRecord::delete_files(conn, &self.id)
    }

    pub fn relink_dirs(conn: &Connection, old_dat_id: DatId, new_dat_id: &DatId) -> Result<usize> {
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
    //manual implentation due to complexity in mapping MatchStatus
    pub fn insert(conn: &Connection, new: &NewFile) -> Result<Self> {
        let (set_id, rom_id) = match new.status.ids() {
            Some((set_id, rom_id)) => (Some(set_id), Some(rom_id)),
            None => (None, None),
        };

        let sql = format!(
            "INSERT INTO {} (dir_id, name, size, hash, status, set_id, rom_id) VALUES (:dir_id, :name, :size, :hash, :status, :set_id, :rom_id)",
            Self::table_name()
        );
        let params = named_params! {
            ":dir_id": new.dir_id,
            ":name": new.name.as_str(),
            ":size": new.size,
            ":hash": new.hash.as_str(),
            ":status": new.status.as_str(),
            ":set_id": set_id,
            ":rom_id": rom_id,
        };
        conn.execute(&sql, params)?;
        let id = conn.last_insert_rowid();
        Self::get_by_id(conn, &FileId::from(id))
    }

    fn get_by_dir(conn: &Connection, dir_id: &DirId) -> Result<Vec<Self>> {
        let matches =
            sql_query!(conn, Self::table_name(), Self::fields(), where {dir_id}, order by "name", Self::from_row)?;
        Ok(matches)
    }

    pub fn find_by_name(conn: &Connection, dir_id: &DirId, name: &str, exact: bool) -> Result<Vec<FileRecord>> {
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

    pub fn update(&self, conn: &Connection, name: &str, status: &MatchStatus) -> Result<Self> {
        let (set_id, rom_id) = match status.ids() {
            Some((set_id, rom_id)) => (Some(set_id), Some(rom_id)),
            None => (None, None),
        };

        let sql = format!(
            "UPDATE {} SET name = :name, status = :status, set_id = :set_id, rom_id = :rom_id WHERE id = :id",
            Self::table_name()
        );
        let num_updated = conn.execute(
            &sql,
            named_params! {
                ":id": self.id,
                ":name": name,
                ":status": status.as_str(),
                ":set_id": set_id,
                ":rom_id": rom_id,
            },
        )?;
        if num_updated != 0 {
            Ok(Self {
                id: self.id.clone(),
                dir_id: self.dir_id.clone(),
                name: name.to_string(),
                size: self.size,
                hash: self.hash.clone(),
                status: status.clone(),
            })
        } else {
            Err(anyhow::anyhow!("Failed to update file record with id {}", self.id.id()))
        }
    }
}

pub fn open_or_create<P: AsRef<Utf8Path>>(db_path: P) -> Result<Connection> {
    const CREATE_STATEMENTS: [&str; 14] = [
        /* dat file */
        "CREATE TABLE IF NOT EXISTS dats ( id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, description VARCHAR NOT NULL, \
        version VARCHAR NOT NULL, author VARCHAR NOT NULL, hash_type VARCHAR NOT NULL);",
        "CREATE TABLE IF NOT EXISTS sets ( id INTEGER PRIMARY KEY, dat_id INTEGER NOT NULL, name VARCHAR NOT NULL, \
        FOREIGN KEY (dat_id) REFERENCES dats(id) );",
        "CREATE TABLE IF NOT EXISTS roms ( id INTEGER PRIMARY KEY, dat_id INTEGER NOT NULL, set_id INTEGER NOT NULL, \
        name VARCHAR NOT NULL, size VARCHAR NOT NULL, hash VARCHAR NOT NULL, \
        FOREIGN KEY (dat_id) REFERENCES dats(id), FOREIGN KEY (set_id) REFERENCES sets(id) );",
        /* file system */
        "CREATE TABLE IF NOT EXISTS dirs ( id INTEGER PRIMARY KEY, dat_id INTEGER NOT NULL, path VARCHAR NOT NULL, \
        parent_id INTEGER, FOREIGN KEY (dat_id) REFERENCES dats(id), FOREIGN KEY (parent_id) REFERENCES dirs(id), UNIQUE(path, dat_id) );",
        "CREATE TABLE IF NOT EXISTS files ( id INTEGER PRIMARY KEY, dir_id INTEGER NOT NULL, name VARCHAR NOT NULL, \
        size VARCHAR NOT NULL, hash VARCHAR NOT NULL, status VARCHAR NOT NULL, set_id INTEGER, rom_id INTEGER, \
        FOREIGN KEY (dir_id) REFERENCES dirs(id),  FOREIGN KEY (set_id) REFERENCES sets(id), \
        FOREIGN KEY (rom_id) REFERENCES roms(id) );",
        /* indices */
        "CREATE INDEX IF NOT EXISTS idx_dat_sets ON sets(dat_id);",
        "CREATE INDEX IF NOT EXISTS idx_dat_sets_name ON sets(dat_id, name);",
        "CREATE INDEX IF NOT EXISTS idx_set_roms ON roms(set_id);",
        "CREATE INDEX IF NOT EXISTS idx_dat_roms_name ON roms(dat_id, name);",
        "CREATE INDEX IF NOT EXISTS idx_dat_roms_hash ON roms(dat_id, hash);",
        "CREATE INDEX IF NOT EXISTS idx_dat_dirs ON dirs(dat_id);",
        "CREATE INDEX IF NOT EXISTS idx_dat_dirs_path ON dirs(dat_id, path);",
        "CREATE INDEX IF NOT EXISTS idx_dir_files ON files(dir_id);",
        "CREATE INDEX IF NOT EXISTS idx_dir_files_name ON files(dir_id, name);",
    ];

    let conn = Connection::open(db_path.as_ref())?;
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;

    for stmt in CREATE_STATEMENTS {
        conn.execute(stmt, ())?;
    }

    Ok(conn)
}
