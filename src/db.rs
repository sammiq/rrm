#![allow(dead_code)]

use camino::Utf8Path;

use anyhow::{Result, bail};
use rusqlite::{Connection, params};

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

// macro that allows an insert statement to be built from a table name and the passed data to avoid errors
macro_rules! sql_insert {
    ($conn:expr, $table:expr, set { $($field:ident $(= $value:expr)?),+ } $(,)?) => {{
        let fields_str = vec![$(stringify!($field)),+].join(", ");
        let params_str = vec![$(concat!(":", stringify!($field))),+].join(", ");
        let sql = format!("INSERT INTO {} ({}) VALUES ({})", $table, fields_str, params_str);

        $conn.execute(
            &sql,
            &[$(
                (concat!(":", stringify!($field)), &sql_insert!(@value $field $(= $value)?) as &dyn rusqlite::ToSql)
            ),+] as &[(&str, &dyn rusqlite::ToSql)]
        )
    }};

    (@value $field:ident = $value:expr) => { $value };
    (@value $field:ident) => { $field };
}

// macro that allows an update statement to be built from a table name and the passed data to avoid errors
macro_rules! sql_update {
    ($conn:expr, $table:expr, where { $($where_field:ident $(= $where_value:expr)?),+ }, set { $($field:ident $(= $value:expr)?),+ } $(,)?) => {{
        let sets = vec![$(concat!(stringify!($field), " = :s_", stringify!($field))),+].join(", ");
        let wheres = vec![$(concat!(stringify!($where_field), " = :w_", stringify!($where_field))),+].join(" AND ");
        let sql = format!("UPDATE {} SET {} WHERE {}", $table, sets, wheres);

        $conn.execute(
            &sql,
            &[
                $(
                    (concat!(":s_", stringify!($field)), &sql_update!(@value $field $(= $value)?) as &dyn rusqlite::ToSql)
                ),+,
                $(
                    (concat!(":w_", stringify!($where_field)), &sql_update!(@value $where_field $(= $where_value)?) as &dyn rusqlite::ToSql)
                ),+
            ] as &[(&str, &dyn rusqlite::ToSql)]
        )
    }};

    (@value $field:ident = $value:expr) => { $value };
    (@value $field:ident) => { $field };
}

// macro that allows an delete statement to be built from a table name and the passed data to avoid errors
macro_rules! sql_delete {
    ($conn:expr, $table:expr, where { $($where_field:ident $(= $where_value:expr)?),+ } $(,)?) => {{
        let wheres = vec![$(concat!(stringify!($where_field), " = :", stringify!($where_field))),+].join(" AND ");
        let sql = format!("DELETE FROM {} WHERE {}", $table, wheres);

        $conn.execute(
            &sql,
            &[$(
                (concat!(":", stringify!($where_field)), &sql_delete!(@value $where_field $(= $where_value)?) as &dyn rusqlite::ToSql)
            ),+] as &[(&str, &dyn rusqlite::ToSql)]
        )
    }};

    (@value $field:ident = $value:expr) => { $value };
    (@value $field:ident) => { $field };
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

    for stmt in CREATE_STATEMENTS {
        conn.execute(stmt, ())?;
    }

    Ok(conn)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DatId(i64);

#[derive(Debug)]
pub struct DatRecord {
    pub id: DatId,
    pub name: String,
    pub description: String,
    pub version: String,
    pub author: String,
    pub hash_type: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SetId(i64);

#[derive(Debug)]
pub struct SetRecord {
    pub id: SetId,
    pub dat_id: DatId,
    pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RomId(i64);

#[derive(Debug)]
pub struct RomRecord {
    pub id: RomId,
    pub dat_id: DatId,
    pub set_id: SetId,
    pub name: String,
    pub size: u64,
    pub hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DirId(i64);

#[derive(Debug)]
pub struct DirRecord {
    pub id: DirId,
    pub dat_id: DatId,
    pub path: String,
    pub parent_id: Option<DirId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FileId(i64);

#[derive(Debug)]
pub struct FileRecord {
    pub id: FileId,
    pub dir_id: DirId,
    pub name: String,
    pub size: u64,
    pub hash: String,
    pub status: MatchStatus,
}

#[derive(Debug, PartialEq, Eq)]
pub enum MatchStatus {
    None,
    Hash { set_id: SetId, rom_id: RomId },
    Name { set_id: SetId, rom_id: RomId },
    Match { set_id: SetId, rom_id: RomId },
}

impl MatchStatus {
    fn to_str(&self) -> &str {
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
                Some((*set_id, *rom_id))
            }
        }
    }
}

const DAT_FIELDS: &str = "id, name, description, version, author, hash_type";

fn dat_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<DatRecord> {
    Ok(DatRecord {
        id: DatId(row.get("id")?),
        name: row.get("name")?,
        description: row.get("description")?,
        version: row.get("version")?,
        author: row.get("author")?,
        hash_type: row.get("hash_type")?,
    })
}

pub fn get_dat(conn: &Connection, dat_id: DatId) -> Result<DatRecord> {
    let record = sql_query_one!(conn, "dats", DAT_FIELDS, where {id = dat_id.0}, dat_from_row)?;
    Ok(record)
}

pub fn get_dats(conn: &Connection) -> Result<Vec<DatRecord>> {
    let mut stmt = conn.prepare(format!("SELECT {DAT_FIELDS} FROM dats").as_str())?;
    let matches = stmt
        .query_map(params![], dat_from_row)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(matches)
}

pub fn insert_dat(
    conn: &Connection,
    name: &str,
    description: &str,
    version: &str,
    author: &str,
    hash_type: &str,
) -> Result<DatRecord> {
    sql_insert!(
        conn,
        "dats",
        set {
            name,
            description,
            version,
            author,
            hash_type
        }
    )?;
    let id = conn.last_insert_rowid();
    Ok(DatRecord {
        id: DatId(id),
        name: name.to_string(),
        description: description.to_string(),
        version: version.to_string(),
        author: author.to_string(),
        hash_type: hash_type.to_string(),
    })
}

const SET_FIELDS: &str = "id, dat_id, name";

fn set_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SetRecord> {
    Ok(SetRecord {
        id: SetId(row.get("id")?),
        dat_id: DatId(row.get("dat_id")?),
        name: row.get("name")?,
    })
}

pub fn get_set(conn: &Connection, set_id: SetId) -> Result<SetRecord> {
    let record = sql_query_one!(conn, "sets", SET_FIELDS, where {id = set_id.0}, set_from_row)?;
    Ok(record)
}

pub fn get_sets(conn: &Connection, dat_id: DatId) -> Result<Vec<SetRecord>> {
    let matches = sql_query!(conn, "sets", SET_FIELDS, where {dat_id = dat_id.0}, set_from_row)?;
    Ok(matches)
}

pub fn get_sets_by_name(conn: &Connection, dat_id: DatId, name: &str, exact: bool) -> Result<Vec<SetRecord>> {
    let matches = if exact {
        sql_query!(conn, "sets", SET_FIELDS, where {dat_id = dat_id.0, name}, set_from_row)
    } else {
        let mut stmt =
            conn.prepare(format!("SELECT {SET_FIELDS} FROM sets WHERE dat_id = (?1) AND name LIKE (?2)").as_str())?;
        stmt.query_map(params![dat_id.0, format!("%{name}%")], set_from_row)?
            .collect::<Result<Vec<_>, _>>()
    }?;

    Ok(matches)
}

pub fn insert_set(conn: &Connection, dat_id: DatId, name: &str) -> Result<SetRecord> {
    sql_insert!(conn, "sets", set {dat_id = dat_id.0, name})?;
    let id = conn.last_insert_rowid();
    Ok(SetRecord {
        id: SetId(id),
        dat_id,
        name: name.to_string(),
    })
}

const ROM_FIELDS: &str = "id, dat_id, set_id, name, size, hash";

fn rom_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RomRecord> {
    Ok(RomRecord {
        id: RomId(row.get("id")?),
        dat_id: DatId(row.get("dat_id")?),
        set_id: SetId(row.get("set_id")?),
        name: row.get("name")?,
        size: row
            .get::<_, String>("size")?
            .parse()
            .expect("field is non null and should always be convertible"),
        hash: row.get("hash")?,
    })
}

pub fn get_rom(conn: &Connection, rom_id: RomId) -> Result<RomRecord> {
    let record = sql_query_one!(conn, "roms", ROM_FIELDS, where {id = rom_id.0}, rom_from_row)?;
    Ok(record)
}

pub fn get_roms(conn: &Connection, dat_id: DatId) -> Result<Vec<RomRecord>> {
    let matches = sql_query!(conn, "roms", ROM_FIELDS, where {dat_id = dat_id.0}, rom_from_row)?;
    Ok(matches)
}

pub fn get_roms_by_set(conn: &Connection, set_id: SetId) -> Result<Vec<RomRecord>> {
    let matches = sql_query!(conn, "roms", ROM_FIELDS, where {set_id = set_id.0}, rom_from_row)?;
    Ok(matches)
}

pub fn get_roms_by_name(conn: &Connection, dat_id: DatId, name: &str, exact: bool) -> Result<Vec<RomRecord>> {
    let matches = if exact {
        sql_query!(conn, "roms", ROM_FIELDS, where {dat_id = dat_id.0, name}, rom_from_row)
    } else {
        let mut stmt =
            conn.prepare(format!("SELECT {ROM_FIELDS} FROM roms WHERE dat_id = (?1) AND name LIKE (?2)").as_str())?;
        stmt.query_map(params![dat_id.0, format!("%{name}%")], rom_from_row)?
            .collect::<Result<Vec<_>, _>>()
    }?;
    Ok(matches)
}

pub fn get_roms_by_hash(conn: &Connection, dat_id: DatId, hash: &str) -> Result<Vec<RomRecord>> {
    let matches = sql_query!(conn, "roms", ROM_FIELDS, where {dat_id = dat_id.0, hash}, rom_from_row)?;
    Ok(matches)
}

pub fn insert_rom(
    conn: &Connection,
    dat_id: DatId,
    set_id: SetId,
    name: &str,
    size: u64,
    hash: &str,
) -> Result<RomRecord> {
    sql_insert!(conn, "roms", set {dat_id = dat_id.0, set_id = set_id.0, name, size = size.to_string(), hash})?;
    let id = conn.last_insert_rowid();
    Ok(RomRecord {
        id: RomId(id),
        dat_id,
        set_id,
        name: name.to_string(),
        size,
        hash: hash.to_string(),
    })
}

const DIR_FIELDS: &str = "id, dat_id, path, parent_id";

fn dir_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<DirRecord> {
    Ok(DirRecord {
        id: DirId(row.get("id")?),
        dat_id: DatId(row.get("dat_id")?),
        path: row.get("path")?,
        parent_id: row.get::<_, Option<i64>>("parent_id")?.map(DirId),
    })
}

pub fn get_directory_by_path(conn: &Connection, dat_id: DatId, path: &str) -> Result<Option<DirRecord>> {
    match sql_query_one!(conn, "dirs", DIR_FIELDS, where {path, dat_id = dat_id.0}, dir_from_row
    ) {
        Ok(dir) => Ok(Some(dir)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => bail!(e),
    }
}

pub fn get_directories(conn: &Connection, dat_id: DatId, parent_id: Option<DirId>) -> Result<Vec<DirRecord>> {
    let matches = if let Some(parent_id) = parent_id {
        sql_query!(conn, "dirs", DIR_FIELDS, where {dat_id = dat_id.0, parent_id = parent_id.0}, order by "path", dir_from_row)
    } else {
        sql_query!(conn, "dirs", DIR_FIELDS, where {dat_id = dat_id.0}, order by "path", dir_from_row)
    }?;
    Ok(matches)
}

pub fn get_directories_by_path(conn: &Connection, path: &str) -> Result<Vec<DirRecord>> {
    let matches = sql_query!(conn, "dirs", DIR_FIELDS, where {path}, order by "path", dir_from_row)?;
    Ok(matches)
}

pub fn insert_directory(conn: &Connection, dat_id: DatId, path: &str, parent_id: Option<DirId>) -> Result<DirRecord> {
    sql_insert!(conn, "dirs", set {path, dat_id = dat_id.0, parent_id = parent_id.map(|id| id.0)})?;
    let id = conn.last_insert_rowid();
    Ok(DirRecord {
        id: DirId(id),
        path: path.to_string(),
        dat_id,
        parent_id,
    })
}

pub fn update_directories(conn: &Connection, old_dat_id: DatId, new_dat_id: DatId) -> Result<usize> {
    let num_updated = sql_update!(conn, "dirs", where {
        dat_id = old_dat_id.0
    },
    set {
        dat_id = new_dat_id.0
    })?;
    Ok(num_updated)
}

pub fn delete_directory(conn: &Connection, dir_id: DirId) -> Result<bool> {
    let num_deleted = sql_delete!(conn, "dirs", where {id = dir_id.0})?;
    Ok(num_deleted != 0)
}

const FILE_FIELDS: &str = "id, dir_id, name, size, hash, status, set_id, rom_id";

fn file_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<FileRecord> {
    let status = match row.get::<_, String>("status")?.as_ref() {
        "hash" => MatchStatus::Hash {
            set_id: SetId(row.get("set_id")?),
            rom_id: RomId(row.get("rom_id")?),
        },
        "name" => MatchStatus::Name {
            set_id: SetId(row.get("set_id")?),
            rom_id: RomId(row.get("rom_id")?),
        },
        "match" => MatchStatus::Match {
            set_id: SetId(row.get("set_id")?),
            rom_id: RomId(row.get("rom_id")?),
        },
        _ => MatchStatus::None,
    };
    Ok(FileRecord {
        id: FileId(row.get("id")?),
        dir_id: DirId(row.get("dir_id")?),
        name: row.get("name")?,
        size: row.get::<_, String>("size")?.parse().unwrap(),
        hash: row.get("hash")?,
        status,
    })
}

pub fn get_files(conn: &Connection, dir_id: DirId, filter_name: Option<&str>) -> Result<Vec<FileRecord>> {
    let matches = if let Some(filter_name) = filter_name {
        let mut stmt = conn.prepare(
            format!("SELECT {FILE_FIELDS} FROM files WHERE dir_id = (?1) AND name LIKE (?2) ORDER BY name").as_str(),
        )?;

        stmt.query_map(params![dir_id.0, format!("%{}%", filter_name)], file_from_row)?
            .collect::<Result<Vec<_>, _>>()
    } else {
        sql_query!(conn, "files", FILE_FIELDS, where {dir_id = dir_id.0}, order by "name", file_from_row)
    }?;
    Ok(matches)
}

pub fn insert_file(
    conn: &Connection,
    dir_id: DirId,
    name: &str,
    size: u64,
    hash: &str,
    status: MatchStatus,
) -> Result<FileRecord> {
    let (set_id, rom_id) = match status.ids() {
        Some((set_id, rom_id)) => (Some(set_id.0), Some(rom_id.0)),
        None => (None, None),
    };
    sql_insert!(
        conn,
        "files",
        set {dir_id = dir_id.0,
        name,
        size = size.to_string(),
        hash,
        status = status.to_str(),
        set_id,
        rom_id}
    )?;
    let id = conn.last_insert_rowid();
    Ok(FileRecord {
        id: FileId(id),
        dir_id,
        name: name.to_string(),
        size,
        hash: hash.to_string(),
        status,
    })
}

pub fn update_file(conn: &Connection, file_id: FileId, name: &str, status: MatchStatus) -> Result<bool> {
    let (set_id, rom_id) = match status.ids() {
        Some((set_id, rom_id)) => (Some(set_id.0), Some(rom_id.0)),
        None => (None, None),
    };

    let num_updated =
        sql_update!(conn, "files", where {id = file_id.0}, set {name, status = status.to_str(), set_id, rom_id})?;
    Ok(num_updated != 0)
}

pub fn delete_file(conn: &Connection, dir_id: DirId, name: &str) -> Result<bool> {
    let num_deleted = sql_delete!(conn, "files", where {dir_id = dir_id.0, name})?;
    Ok(num_deleted != 0)
}

pub fn delete_files(conn: &Connection, dir_id: DirId) -> Result<usize> {
    let num_deleted = sql_delete!(conn, "files", where {dir_id = dir_id.0})?;
    Ok(num_deleted)
}
