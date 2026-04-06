use std::collections::{BTreeSet, HashMap};

use anyhow::{Result, bail};
use rusqlite::{Connection, named_params, params, params_from_iter};

use super::types::*;

// ── DatRecord ──────────────────────────────────────────────────────

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
    name: String,
    description: String,
    version: String,
    author: String,
    hash_type: String,
}

impl NewDat {
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        version: impl Into<String>,
        author: impl Into<String>,
        hash_type: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            version: version.into(),
            author: author.into(),
            hash_type: hash_type.into(),
        }
    }

    #[cfg(test)]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[cfg(test)]
    pub fn description(&self) -> &str {
        &self.description
    }

    #[cfg(test)]
    pub fn version(&self) -> &str {
        &self.version
    }

    #[cfg(test)]
    pub fn author(&self) -> &str {
        &self.author
    }

    #[cfg(test)]
    pub fn hash_type(&self) -> &str {
        &self.hash_type
    }
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

#[allow(dead_code)]
impl DatRecord {
    pub fn get_sets(&self, conn: &Connection) -> Result<Vec<SetRecord>> {
        SetRecord::get_by_dat(conn, self.id)
    }

    pub fn get_roms(&self, conn: &Connection) -> Result<Vec<RomRecord>> {
        RomRecord::get_by_dat(conn, self.id)
    }

    pub fn get_directories(&self, conn: &Connection) -> Result<Vec<DirRecord>> {
        DirRecord::get_by_dat(conn, self.id)
    }
}

// ── SetRecord ──────────────────────────────────────────────────────

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
    dat_id: DatId,
    name: String,
}

impl NewSet {
    pub fn new(dat_id: DatId, name: impl Into<String>) -> Self {
        Self {
            dat_id,
            name: name.into(),
        }
    }
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

impl SetRecord {
    pub fn get_roms(&self, conn: &Connection) -> Result<Vec<RomRecord>> {
        RomRecord::get_by_set(conn, self.id)
    }
}

// ── RomRecord ──────────────────────────────────────────────────────

pub type RomId = Id<RomRecord>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RomRecord {
    pub id: RomId,

    pub dat_id: DatId, //denormalized to avoid N+1 queries

    pub set_id: SetId,
    pub name: String,
    pub size: u64,
    pub hash: String,
    pub crc: Option<String>,
}

impl Queryable for RomRecord {
    type IdType = RomId;

    fn table_name() -> &'static str {
        "roms"
    }

    fn fields() -> &'static str {
        "id, dat_id, set_id, name, size, hash, crc"
    }

    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(RomRecord {
            id: row.get("id")?,
            dat_id: row.get("dat_id")?,
            set_id: row.get("set_id")?,
            name: row.get("name")?,
            size: row.get::<_, StoredU64>("size")?.0,
            hash: row.get("hash")?,
            crc: row.get("crc")?,
        })
    }
}

impl QueryableByDat for RomRecord {}
impl DeletableByDat for RomRecord {}
impl FindableByName for RomRecord {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NewRom {
    dat_id: DatId, //denormalized to avoid N+1 queries
    set_id: SetId,
    name: String,
    size: StoredU64,
    hash: String,
    crc: Option<String>,
}

impl NewRom {
    pub fn new(
        dat_id: DatId,
        set_id: SetId,
        name: impl Into<String>,
        size: u64,
        hash: impl Into<String>,
        crc: Option<String>,
    ) -> Self {
        Self {
            dat_id,
            set_id,
            name: name.into(),
            size: size.into(),
            hash: hash.into(),
            crc,
        }
    }
}

impl Bindable for NewRom {
    fn bind_params(&self) -> Vec<(&'static str, &dyn rusqlite::ToSql)> {
        named_params! {
            ":dat_id": self.dat_id,
            ":set_id": self.set_id,
            ":name": self.name,
            ":size": self.size,
            ":hash": self.hash,
            ":crc": self.crc,
        }
        .to_vec()
    }
}

impl Insertable for RomRecord {
    type NewType = NewRom;
}

impl RomRecord {
    fn get_by_set(conn: &Connection, set_id: SetId) -> Result<Vec<Self>> {
        let matches = sql_query!(conn, Self::table_name(), Self::fields(), where {set_id}, Self::from_row)?;
        Ok(matches)
    }

    pub fn find_by_hash_in_dat(conn: &Connection, dat_id: DatId, hash: &str) -> Result<Vec<RomRecord>> {
        let matches = sql_query!(conn, Self::table_name(), Self::fields(), where {dat_id, hash}, Self::from_row)?;
        Ok(matches)
    }

    pub fn get_crcs_by_dat(conn: &Connection, dat_id: DatId) -> Result<BTreeSet<String>> {
        let mut stmt =
            conn.prepare("SELECT DISTINCT crc FROM roms WHERE dat_id = ?1 AND crc IS NOT NULL ORDER BY crc")?;
        let crcs = stmt
            .query_map([dat_id], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<BTreeSet<_>>>()?;
        Ok(crcs)
    }

    #[allow(dead_code)]
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

// ── DirRecord ──────────────────────────────────────────────────────

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
    dat_id: DatId,
    path: String,
}

impl NewDir {
    pub fn new(dat_id: DatId, path: impl Into<String>) -> Self {
        Self {
            dat_id,
            path: path.into(),
        }
    }
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

impl DirRecord {
    pub fn get_by_path(conn: &Connection, path: &str) -> Result<Vec<DirRecord>> {
        let matches =
            sql_query!(conn, Self::table_name(), DirRecord::fields(), where {path}, order by "path", Self::from_row)?;
        Ok(matches)
    }

    pub fn find_by_path_in_dat(conn: &Connection, dat_id: DatId, path: &str) -> Result<Option<DirRecord>> {
        match sql_query_one!(conn, Self::table_name(), Self::fields(), where {path, dat_id}, Self::from_row
        ) {
            Ok(dir) => Ok(Some(dir)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => bail!(e),
        }
    }

    pub fn get_children(&self, conn: &Connection) -> Result<Vec<DirRecord>> {
        let sql = format!(
            "SELECT {} FROM {} WHERE dat_id = :dat_id AND path LIKE :prefix ESCAPE '\\' ORDER BY path",
            Self::fields(),
            Self::table_name(),
        );
        let prefix = format!("{}/%", escape_like(&self.path));
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt
            .query_map(named_params! { ":dat_id": self.dat_id, ":prefix": prefix }, Self::from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn get_files(&self, conn: &Connection) -> Result<Vec<FileRecord>> {
        FileRecord::get_by_dir(conn, self.id)
    }

    pub fn find_files(&self, conn: &Connection, name: &str, exact: bool) -> Result<Vec<FileRecord>> {
        FileRecord::find_by_name_in_dir(conn, self.id, name, exact)
    }

    pub fn delete_matches(&self, conn: &Connection) -> Result<usize> {
        MatchRecord::delete_by_dir_id(conn, self.id)
    }

    pub fn delete_files(&self, conn: &Connection) -> Result<usize> {
        FileRecord::delete_files(conn, self.id)
    }

    pub fn update_path(&self, conn: &Connection, path: &str) -> Result<Self> {
        let sql = format!("UPDATE {} SET path = :path WHERE id = :id", Self::table_name());
        conn.execute(
            &sql,
            named_params! {
                ":id": self.id,
                ":path": path,
            },
        )?;
        Ok(Self {
            id: self.id,
            dat_id: self.dat_id,
            path: path.to_string(),
        })
    }

    pub fn relink_dirs(conn: &Connection, old_dat_id: DatId, new_dat_id: DatId) -> Result<usize> {
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

// ── FileRecord ─────────────────────────────────────────────────────

pub type FileId = Id<FileRecord>;

// FileRecord may have an empty hash if and only if it
//is contained in a zip file and there are no matches
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
    dat_id: DatId, //denormalized to avoid N+1 queries
    dir_id: DirId,
    name: String,
    size: StoredU64,
    hash: String,
}

impl NewFile {
    pub fn new(dat_id: DatId, dir_id: DirId, name: impl Into<String>, size: u64, hash: impl Into<String>) -> Self {
        Self {
            dat_id,
            dir_id,
            name: name.into(),
            size: size.into(),
            hash: hash.into(),
        }
    }
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

impl FileRecord {
    fn get_by_dir(conn: &Connection, dir_id: DirId) -> Result<Vec<Self>> {
        let matches =
            sql_query!(conn, Self::table_name(), Self::fields(), where {dir_id}, order by "name", Self::from_row)?;
        Ok(matches)
    }

    pub fn find_by_name_in_dir(conn: &Connection, dir_id: DirId, name: &str, exact: bool) -> Result<Vec<FileRecord>> {
        let matches = if exact {
            sql_query!(conn, Self::table_name(), FileRecord::fields(), where {dir_id, name}, order by "name", Self::from_row)
        } else {
            let mut stmt = conn.prepare(
                format!(
                    "SELECT {} FROM {} WHERE dir_id = (?1) AND name LIKE (?2) ESCAPE '\\' ORDER BY name",
                    Self::fields(),
                    Self::table_name()
                )
                .as_str(),
            )?;

            stmt.query_map(params![dir_id, format!("%{}%", escape_like(name))], FileRecord::from_row)?
                .collect::<Result<Vec<_>, _>>()
        }?;
        Ok(matches)
    }

    pub fn delete_files(conn: &Connection, dir_id: DirId) -> Result<usize> {
        let sql = format!("DELETE FROM {} WHERE dir_id = :dir_id", Self::table_name());
        let num_deleted = conn.execute(&sql, named_params! {":dir_id": dir_id})?;
        Ok(num_deleted)
    }

    pub fn relink_files(conn: &Connection, old_dat_id: DatId, new_dat_id: DatId) -> Result<usize> {
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

    pub fn update_dir_id(&self, conn: &Connection, dir_id: DirId) -> Result<Self> {
        let sql = format!("UPDATE {} SET dir_id = :dir_id WHERE id = :id", Self::table_name());
        conn.execute(
            &sql,
            named_params! {
                ":id": self.id,
                ":dir_id": dir_id,
            },
        )?;
        Ok(Self {
            id: self.id,
            dat_id: self.dat_id,
            dir_id,
            name: self.name.clone(),
            size: self.size,
            hash: self.hash.clone(),
        })
    }

    pub fn update_hash(&self, conn: &Connection, hash: &str) -> Result<Self> {
        let sql = format!("UPDATE {} SET hash = :hash WHERE id = :id", Self::table_name());
        conn.execute(
            &sql,
            named_params! {
                ":id": self.id,
                ":hash": hash,
            },
        )?;
        Ok(Self {
            id: self.id,
            dat_id: self.dat_id,
            dir_id: self.dir_id,
            name: self.name.clone(),
            size: self.size,
            hash: hash.to_string(),
        })
    }

    pub fn delete_matches(&self, conn: &Connection) -> Result<usize> {
        MatchRecord::delete_by_file_id(conn, self.id)
    }
}

// ── MatchRecord ────────────────────────────────────────────────────

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

#[derive(Debug, Clone)]
pub struct FoundSetDetailRow {
    pub set: SetRecord,
    pub rom: RomRecord,
    pub matched: Option<MatchRecord>,
    pub file: Option<FileRecord>,
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
    dat_id: DatId, //denormalized to avoid N+1 queries
    file_id: FileId,
    status: MatchStatus,
    set_id: SetId,
    rom_id: RomId,
}

impl NewMatch {
    pub fn new(dat_id: DatId, file_id: FileId, status: MatchStatus, set_id: SetId, rom_id: RomId) -> Self {
        Self {
            dat_id,
            file_id,
            status,
            set_id,
            rom_id,
        }
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

impl MatchRecord {
    pub fn get_found_set_details(conn: &Connection, dat_id: DatId) -> Result<Vec<FoundSetDetailRow>> {
        let sql = r#"
            WITH found_sets AS (
                SELECT DISTINCT set_id
                FROM matches
                WHERE dat_id = ?1
            )
            SELECT
                s.id AS set_id,
                s.dat_id AS set_dat_id,
                s.name AS set_name,
                r.id AS rom_id,
                r.dat_id AS rom_dat_id,
                r.set_id AS rom_set_id,
                r.name AS rom_name,
                r.size AS rom_size,
                r.hash AS rom_hash,
                r.crc AS rom_crc,
                m.id AS match_id,
                m.dat_id AS match_dat_id,
                m.file_id AS match_file_id,
                m.status AS match_status,
                m.set_id AS match_set_id,
                m.rom_id AS match_rom_id,
                f.id AS file_id,
                f.dat_id AS file_dat_id,
                f.dir_id AS file_dir_id,
                f.name AS file_name,
                f.size AS file_size,
                f.hash AS file_hash
            FROM found_sets fs
            JOIN sets s ON s.id = fs.set_id AND s.dat_id = ?1
            JOIN roms r ON r.set_id = fs.set_id AND r.dat_id = ?1
            LEFT JOIN matches m ON m.dat_id = ?1 AND m.set_id = r.set_id AND m.rom_id = r.id
            LEFT JOIN files f ON f.id = m.file_id
            ORDER BY s.id, r.id, m.id
        "#;

        let mut stmt = conn.prepare(sql)?;
        let rows = stmt
            .query_map([dat_id], |row| {
                let set = SetRecord {
                    id: row.get("set_id")?,
                    dat_id: row.get("set_dat_id")?,
                    name: row.get("set_name")?,
                };
                let rom = RomRecord {
                    id: row.get("rom_id")?,
                    dat_id: row.get("rom_dat_id")?,
                    set_id: row.get("rom_set_id")?,
                    name: row.get("rom_name")?,
                    size: row.get::<_, StoredU64>("rom_size")?.0,
                    hash: row.get("rom_hash")?,
                    crc: row.get("rom_crc")?,
                };

                let matched = if let Some(id) = row.get::<_, Option<MatchId>>("match_id")? {
                    Some(MatchRecord {
                        id,
                        dat_id: row.get("match_dat_id")?,
                        file_id: row.get("match_file_id")?,
                        status: row.get("match_status")?,
                        set_id: row.get("match_set_id")?,
                        rom_id: row.get("match_rom_id")?,
                    })
                } else {
                    None
                };

                let file = if let Some(id) = row.get::<_, Option<FileId>>("file_id")? {
                    Some(FileRecord {
                        id,
                        dat_id: row.get("file_dat_id")?,
                        dir_id: row.get("file_dir_id")?,
                        name: row.get("file_name")?,
                        size: row.get::<_, StoredU64>("file_size")?.0,
                        hash: row.get("file_hash")?,
                    })
                } else {
                    None
                };

                Ok(FoundSetDetailRow {
                    set,
                    rom,
                    matched,
                    file,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn delete_by_file_id(conn: &Connection, file_id: FileId) -> Result<usize> {
        let sql = format!("DELETE FROM {} WHERE file_id = :file_id", Self::table_name());
        let num_deleted = conn.execute(&sql, named_params! {":file_id": file_id})?;
        Ok(num_deleted)
    }

    pub fn delete_by_dir_id(conn: &Connection, dir_id: DirId) -> Result<usize> {
        let sql = format!(
            "DELETE FROM {matches} WHERE file_id IN (SELECT id FROM {files} WHERE dir_id = :dir_id)",
            matches = Self::table_name(),
            files = FileRecord::table_name(),
        );
        let num_deleted = conn.execute(&sql, named_params! {":dir_id": dir_id})?;
        Ok(num_deleted)
    }

    pub fn get_by_file_id(conn: &Connection, file_id: FileId) -> Result<Vec<Self>> {
        let matches =
            sql_query!(conn, Self::table_name(), Self::fields(), where {file_id}, order by "id", Self::from_row)?;
        Ok(matches)
    }

    pub fn find_by_status_for_dat(conn: &Connection, dat_id: DatId, status: MatchStatus) -> Result<Vec<Self>> {
        let matches = sql_query!(conn, Self::table_name(), Self::fields(), where {dat_id = dat_id.id(), status}, order by "id", Self::from_row)?;
        Ok(matches)
    }

    pub fn update(&self, conn: &Connection, status: MatchStatus) -> Result<Self> {
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
            status,
            set_id: self.set_id,
            rom_id: self.rom_id,
        })
    }
}
