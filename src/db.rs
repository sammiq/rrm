#![allow(dead_code)]

use camino::Utf8Path;

use anyhow::{Result, bail};
use rusqlite::{Connection, params};

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
        /* alter */
        //"ALTER TABLE dirs ADD COLUMN parent_id INTEGER;"
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

#[derive(Debug)]
pub struct FileRecord {
    pub id: i64,
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
}

pub fn get_dat(conn: &Connection, dat_id: DatId) -> Result<Option<DatRecord>> {
    let record = match conn.query_one(
        "SELECT name, description, version, author, hash_type FROM dats WHERE id = (?1)",
        params![dat_id.0],
        |row| {
            Ok(DatRecord {
                id: dat_id,
                name: row.get(0)?,
                description: row.get(1)?,
                version: row.get(2)?,
                author: row.get(3)?,
                hash_type: row.get(4)?,
            })
        },
    ) {
        Ok(record) => Some(record),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(e) => bail!(e),
    };
    Ok(record)
}

pub fn get_dats(conn: &Connection) -> Result<Vec<DatRecord>> {
    let mut stmt = conn.prepare("SELECT id, name, description, version, author, hash_type FROM dats")?;
    let matches = stmt
        .query_map(params![], |row| {
            Ok(DatRecord {
                id: DatId(row.get(0)?),
                name: row.get(2)?,
                description: row.get(2)?,
                version: row.get(3)?,
                author: row.get(4)?,
                hash_type: row.get(5)?,
            })
        })?
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
    conn.execute(
        "INSERT INTO dats (name, description, version, author, hash_type) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![name, description, version, author, hash_type,],
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

pub fn get_set(conn: &Connection, set_id: SetId) -> Result<SetRecord> {
    let record = conn.query_one("SELECT dat_id, name FROM sets WHERE id = (?1)", params![set_id.0], |row| {
        Ok(SetRecord {
            id: set_id,
            dat_id: DatId(row.get(0)?),
            name: row.get(1)?,
        })
    })?;
    Ok(record)
}

pub fn get_set_with_roms(conn: &Connection, set_id: SetId) -> Result<(SetRecord, Vec<RomRecord>)> {
    let set_record = conn.query_one("SELECT dat_id, name FROM sets WHERE id = (?1)", params![set_id.0], |row| {
        Ok(SetRecord {
            id: set_id,
            dat_id: DatId(row.get(0)?),
            name: row.get(1)?,
        })
    })?;
    let roms = get_roms_by_set(conn, set_id)?;
    Ok((set_record, roms))
}

pub fn get_sets(conn: &Connection, dat_id: DatId) -> Result<Vec<SetRecord>> {
    let mut stmt = conn.prepare("SELECT id, name FROM sets WHERE dat_id = (?1)")?;
    let matches = stmt
        .query_map(params![dat_id.0], |row| {
            Ok(SetRecord {
                id: SetId(row.get(0)?),
                dat_id,
                name: row.get(1)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(matches)
}

pub fn get_sets_by_name(conn: &Connection, dat_id: DatId, name: &str, exact: bool) -> Result<Vec<SetRecord>> {
    let (mut stmt, search_name) = if exact {
        (
            conn.prepare("SELECT id, name FROM sets WHERE dat_id = (?1) AND name = (?2)")?,
            name.to_string(),
        )
    } else {
        (
            conn.prepare("SELECT id, name FROM sets WHERE dat_id = (?1) AND name LIKE (?2)")?,
            format!("%{name}%"),
        )
    };
    let matches = stmt
        .query_map(params![dat_id.0, search_name], |row| {
            Ok(SetRecord {
                id: SetId(row.get(0)?),
                dat_id,
                name: row.get(1)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(matches)
}

pub fn insert_set(conn: &Connection, dat_id: DatId, name: &str) -> Result<SetRecord> {
    conn.execute("INSERT INTO sets (dat_id, name) VALUES (?1, ?2)", params![dat_id.0, name])?;
    let id = conn.last_insert_rowid();
    Ok(SetRecord {
        id: SetId(id),
        dat_id,
        name: name.to_string(),
    })
}

pub fn insert_rom(
    conn: &Connection,
    dat_id: DatId,
    set_id: SetId,
    name: &str,
    size: u64,
    hash: &str,
) -> Result<RomRecord> {
    conn.execute(
        "INSERT INTO roms (dat_id, set_id, name, size, hash) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![dat_id.0, set_id.0, name, size.to_string(), hash],
    )?;
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

pub fn get_roms_by_set(conn: &Connection, set_id: SetId) -> Result<Vec<RomRecord>> {
    let mut stmt = conn.prepare("SELECT id, dat_id, name, size, hash FROM roms WHERE set_id = (?1)")?;
    let matches = stmt
        .query_map(params![set_id.0], |row| {
            Ok(RomRecord {
                id: RomId(row.get(0)?),
                dat_id: DatId(row.get(1)?),
                set_id,
                name: row.get(2)?,
                size: row.get::<_, String>(3)?.parse().unwrap(),
                hash: row.get(4)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(matches)
}

pub fn get_roms_by_name(conn: &Connection, dat_id: DatId, name: &str, exact: bool) -> Result<Vec<RomRecord>> {
    let (mut stmt, search_name) = if exact {
        (
            conn.prepare("SELECT id, set_id, name, size, hash FROM roms WHERE dat_id = (?1) AND name = (?2)")?,
            name.to_string(),
        )
    } else {
        (
            conn.prepare("SELECT id, set_id, name, size, hash FROM roms WHERE dat_id = (?1) AND name LIKE (?2)")?,
            format!("%{name}%"),
        )
    };
    let matches = stmt
        .query_map(params![dat_id.0, search_name], |row| {
            Ok(RomRecord {
                id: RomId(row.get(0)?),
                dat_id,
                set_id: SetId(row.get(1)?),
                name: row.get(2)?,
                size: row.get::<_, String>(3)?.parse().unwrap(),
                hash: row.get(4)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(matches)
}

pub fn get_roms_by_hash(conn: &Connection, dat_id: DatId, hash: &str) -> Result<Vec<RomRecord>> {
    let mut stmt = conn.prepare("SELECT id, set_id, name, size FROM roms WHERE dat_id = (?1) AND hash = (?2)")?;
    let matches = stmt
        .query_map(params![dat_id.0, hash], |row| {
            Ok(RomRecord {
                id: RomId(row.get(0)?),
                dat_id,
                set_id: SetId(row.get(1)?),
                name: row.get(2)?,
                size: row.get::<_, String>(3)?.parse().unwrap(),
                hash: hash.to_string(),
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(matches)
}

pub fn get_directory(conn: &Connection, dat_id: DatId, path: &str) -> Result<Option<DirRecord>> {
    match conn.query_one(
        "SELECT id, parent_id FROM dirs WHERE path = (?1) AND dat_id = (?2)",
        params![path, dat_id.0],
        |row| {
            Ok(DirRecord {
                id: DirId(row.get(0)?),
                path: path.to_string(),
                dat_id,
                parent_id: row.get::<_, Option<i64>>(1)?.map(DirId),
            })
        },
    ) {
        Ok(id) => Ok(Some(id)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => bail!(e),
    }
}

pub fn get_directories(conn: &Connection, dat_id: DatId, parent_id: Option<DirId>) -> Result<Vec<DirRecord>> {
    let matches = if let Some(parent_id) = parent_id {
        let mut stmt = conn.prepare("SELECT id, path, parent_id FROM dirs WHERE dat_id = (?1) AND parent_id = (?2)")?;
        stmt.query_map(params![dat_id.0, parent_id.0], |row| dir_from_row(dat_id, row))?
            .collect::<Result<Vec<_>, _>>()?
    } else {
        let mut stmt = conn.prepare("SELECT id, path, parent_id FROM dirs WHERE dat_id = (?1)")?;
        stmt.query_map(params![dat_id.0], |row| dir_from_row(dat_id, row))?
            .collect::<Result<Vec<_>, _>>()?
    };
    Ok(matches)
}

fn dir_from_row(dat_id: DatId, row: &rusqlite::Row<'_>) -> rusqlite::Result<DirRecord> {
    Ok(DirRecord {
        id: DirId(row.get(0)?),
        path: row.get(1)?,
        dat_id,
        parent_id: row.get::<_, Option<i64>>(2)?.map(DirId),
    })
}

pub fn get_directories_by_path(conn: &Connection, path: &str) -> Result<Vec<DirRecord>> {
    let mut stmt = conn.prepare("SELECT id, dat_id, parent_id FROM dirs WHERE path = (?1)")?;
    let matches = stmt
        .query_map(params![path], |row| {
            Ok(DirRecord {
                id: DirId(row.get(0)?),
                path: path.to_string(),
                dat_id: DatId(row.get(1)?),
                parent_id: row.get::<_, Option<i64>>(2)?.map(DirId),
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(matches)
}

pub fn insert_directory(conn: &Connection, dat_id: DatId, path: &str, parent_id: Option<DirId>) -> Result<DirRecord> {
    if let Some(parent_id) = parent_id {
        conn.execute(
            "INSERT INTO dirs (path, dat_id, parent_id) VALUES (?1, ?2, ?3)",
            params![path, dat_id.0, parent_id.0],
        )?;
    } else {
        conn.execute("INSERT INTO dirs (path, dat_id) VALUES (?1, ?2)", params![path, dat_id.0])?;
    }
    let id = conn.last_insert_rowid();
    Ok(DirRecord {
        id: DirId(id),
        path: path.to_string(),
        dat_id,
        parent_id,
    })
}

pub fn update_directories(conn: &Connection, old_dat_id: DatId, new_dat_id: DatId) -> Result<usize> {
    let num_updated = conn.execute("UPDATE dirs SET dat_id = (?1) WHERE dat_id = (?2)", params![new_dat_id.0, old_dat_id.0])?;
    Ok(num_updated)
}


pub fn delete_directory(conn: &Connection, dir_id: DirId) -> Result<bool> {
    let num_deleted = conn.execute("DELETE FROM dirs WHERE id = (?1)", params![dir_id.0])?;
    Ok(num_deleted != 0)
}

pub fn get_file(conn: &Connection, dir_id: DirId, name: &str) -> Result<Option<FileRecord>> {
    let record = match conn.query_one(
        "SELECT id, size, hash, status, set_id, rom_id FROM files WHERE name = (?1) AND dir_id = (?2)",
        params![name, dir_id.0],
        |row| {
            let status = match row.get::<_, String>(3)?.as_ref() {
                "hash" => MatchStatus::Hash {
                    set_id: SetId(row.get(4)?),
                    rom_id: RomId(row.get(5)?),
                },
                "name" => MatchStatus::Name {
                    set_id: SetId(row.get(4)?),
                    rom_id: RomId(row.get(5)?),
                },
                "match" => MatchStatus::Match {
                    set_id: SetId(row.get(4)?),
                    rom_id: RomId(row.get(5)?),
                },
                _ => MatchStatus::None,
            };

            Ok(FileRecord {
                id: row.get(0)?,
                dir_id,
                name: name.to_string(),
                size: row.get::<_, String>(1)?.parse().unwrap(),
                hash: row.get(2)?,
                status,
            })
        },
    ) {
        Ok(record) => Some(record),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(e) => bail!(e),
    };
    Ok(record)
}

pub fn get_files(conn: &Connection, dir_id: DirId, filter_name: Option<&str>) -> Result<Vec<FileRecord>> {
    let matches = if let Some(filter_name) = filter_name {
        let mut stmt = conn.prepare(
            "SELECT id, name, size, hash, status, set_id, rom_id FROM files WHERE dir_id = (?1) AND name LIKE (?2)",
        )?;

        stmt.query_map(params![dir_id.0, format!("%{}%", filter_name)], |row| file_from_row(dir_id, row))?
            .collect::<Result<Vec<_>, _>>()?
    } else {
        let mut stmt =
            conn.prepare("SELECT id, name, size, hash, status, set_id, rom_id FROM files WHERE dir_id = (?1)")?;

        stmt.query_map(params![dir_id.0], |row| file_from_row(dir_id, row))?
            .collect::<Result<Vec<_>, _>>()?
    };
    Ok(matches)
}

fn file_from_row(dir_id: DirId, row: &rusqlite::Row<'_>) -> rusqlite::Result<FileRecord> {
    let status = match row.get::<_, String>(4)?.as_ref() {
        "hash" => MatchStatus::Hash {
            set_id: SetId(row.get(5)?),
            rom_id: RomId(row.get(6)?),
        },
        "name" => MatchStatus::Name {
            set_id: SetId(row.get(5)?),
            rom_id: RomId(row.get(6)?),
        },
        "match" => MatchStatus::Match {
            set_id: SetId(row.get(5)?),
            rom_id: RomId(row.get(6)?),
        },
        _ => MatchStatus::None,
    };
    Ok(FileRecord {
        id: row.get(0)?,
        dir_id,
        name: row.get(1)?,
        size: row.get::<_, String>(2)?.parse().unwrap(),
        hash: row.get(3)?,
        status,
    })
}

pub fn insert_file(
    conn: &Connection,
    dir_id: DirId,
    name: &str,
    size: u64,
    hash: &str,
    status: MatchStatus,
) -> Result<FileRecord> {
    let ids = match status {
        MatchStatus::None => None,
        MatchStatus::Hash { set_id, rom_id } => Some((set_id, rom_id)),
        MatchStatus::Name { set_id, rom_id } => Some((set_id, rom_id)),
        MatchStatus::Match { set_id, rom_id } => Some((set_id, rom_id)),
    };
    if let Some((set_id, rom_id)) = ids {
        conn.execute(
            "INSERT INTO files (dir_id, name, size, hash, status, set_id, rom_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                dir_id.0,
                name,
                size.to_string(),
                hash,
                status.to_str(),
                set_id.0,
                rom_id.0
            ],
        )?;
    } else {
        conn.execute(
            "INSERT INTO files (dir_id, name, size, hash, status) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![dir_id.0, name, size.to_string(), hash, status.to_str()],
        )?;
    }

    let id = conn.last_insert_rowid();
    Ok(FileRecord {
        id,
        dir_id,
        name: name.to_string(),
        size,
        hash: hash.to_string(),
        status,
    })
}

pub fn delete_file(conn: &Connection, dir_id: DirId, name: &str) -> Result<bool> {
    let num_deleted = conn.execute("DELETE FROM files WHERE dir_id = (?1) AND name = (?2)", params![dir_id.0, name])?;
    Ok(num_deleted != 0)
}

pub fn delete_files(conn: &Connection, dir_id: DirId) -> Result<usize> {
    let num_deleted = conn.execute("DELETE FROM files WHERE dir_id = (?1)", params![dir_id.0])?;
    Ok(num_deleted)
}
