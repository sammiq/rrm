#[macro_use]
mod macros;
mod records;
mod types;

pub use records::*;
pub use types::*;

use anyhow::Result;
use camino::Utf8Path;
use rusqlite::{Connection, Savepoint, Transaction};

const SCHEMA_VERSION: i64 = 3;

pub fn open_or_create<P: AsRef<Utf8Path>>(db_path: P) -> Result<Connection> {
    let mut conn = Connection::open(db_path.as_ref())?;
    conn.execute_batch("PRAGMA foreign_keys = OFF;")?;

    // Create the schema_version table first so we can detect a fresh database.
    conn.execute("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY);", ())?;

    let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
    let version: Option<i64> = tx.query_row("SELECT MAX(version) FROM schema_version", [], |row| row.get(0))?;

    if let Some(v) = version {
        run_migrations(&tx, db_path.as_ref(), v)?;
    } else {
        create_schema(&tx)?;
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
            crc VARCHAR,
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
        CREATE INDEX idx_dat_roms_crc ON roms(dat_id, crc);
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

    if version < 3 {
        conn.execute_batch(
            r#"
            ALTER TABLE roms ADD COLUMN crc VARCHAR;
            CREATE INDEX IF NOT EXISTS idx_dat_roms_crc ON roms(dat_id, crc);
            "#,
        )?;
        conn.execute("INSERT INTO schema_version (version) VALUES (3)", [])?;
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
pub(crate) mod tests {
    use std::collections::BTreeSet;

    use super::*;

    /// Create an in-memory database with the current schema.
    pub(crate) fn mem_db() -> Connection {
        let conn = Connection::open_in_memory().expect("open in-memory db");
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        conn.execute("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY);", ())
            .unwrap();
        create_schema(&conn).unwrap();
        conn
    }

    pub(crate) fn sample_dat() -> NewDat {
        NewDat::new("Test DAT", "A test dat file", "1.0", "tester", "sha1")
    }

    // --- DatRecord CRUD ---

    #[test]
    fn insert_and_get_dat() {
        let conn = mem_db();
        let dat = DatRecord::insert(&conn, sample_dat()).unwrap();
        assert_eq!(dat.name, "Test DAT");

        let fetched = DatRecord::get_by_id(&conn, dat.id).unwrap();
        assert_eq!(fetched, dat);
    }

    #[test]
    fn get_all_dats() {
        let conn = mem_db();
        DatRecord::insert(&conn, sample_dat()).unwrap();
        DatRecord::insert(&conn, NewDat::new("Second", "A test dat file", "1.0", "tester", "sha1")).unwrap();

        let all = DatRecord::get_all(&conn).unwrap();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn delete_dat() {
        let conn = mem_db();
        let dat = DatRecord::insert(&conn, sample_dat()).unwrap();
        assert!(DatRecord::delete_by_id(&conn, dat.id).unwrap());
        assert!(!DatRecord::delete_by_id(&conn, dat.id).unwrap());
    }

    // --- SetRecord ---

    fn insert_dat_and_set(conn: &Connection) -> (DatRecord, SetRecord) {
        let dat = DatRecord::insert(conn, sample_dat()).unwrap();
        let set = SetRecord::insert(conn, NewSet::new(dat.id, "Game Set")).unwrap();
        (dat, set)
    }

    #[test]
    fn insert_and_query_set_by_dat() {
        let conn = mem_db();
        let (dat, set) = insert_dat_and_set(&conn);

        let sets = SetRecord::get_by_dat(&conn, dat.id).unwrap();
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].name, set.name);
    }

    #[test]
    fn find_set_by_name_exact() {
        let conn = mem_db();
        let (dat, _) = insert_dat_and_set(&conn);

        let found = SetRecord::find_by_name(&conn, dat.id, "Game Set", true).unwrap();
        assert_eq!(found.len(), 1);

        let not_found = SetRecord::find_by_name(&conn, dat.id, "game set", true).unwrap();
        assert_eq!(not_found.len(), 0);
    }

    #[test]
    fn find_set_by_name_fuzzy() {
        let conn = mem_db();
        let (dat, _) = insert_dat_and_set(&conn);

        let found = SetRecord::find_by_name(&conn, dat.id, "Game", false).unwrap();
        assert_eq!(found.len(), 1);
    }

    #[test]
    fn delete_sets_by_dat() {
        let conn = mem_db();
        let (dat, _) = insert_dat_and_set(&conn);
        let deleted = SetRecord::delete_by_dat(&conn, dat.id).unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(SetRecord::get_num_by_dat(&conn, dat.id).unwrap(), 0);
    }

    // --- RomRecord ---

    fn insert_rom(
        conn: &Connection,
        dat: &DatRecord,
        set: &SetRecord,
        name: &str,
        hash: &str,
        crc: Option<&str>,
    ) -> RomRecord {
        RomRecord::insert(conn, NewRom::new(dat.id, set.id, name, 1024, hash, crc.map(str::to_string))).unwrap()
    }

    #[test]
    fn insert_and_query_roms() {
        let conn = mem_db();
        let (dat, set) = insert_dat_and_set(&conn);
        let rom = insert_rom(&conn, &dat, &set, "game.rom", "abc123", None);

        let roms = RomRecord::get_by_dat(&conn, dat.id).unwrap();
        assert_eq!(roms.len(), 1);
        assert_eq!(roms[0].id, rom.id);

        let by_set = set.get_roms(&conn).unwrap();
        assert_eq!(by_set.len(), 1);
    }

    #[test]
    fn find_rom_by_hash() {
        let conn = mem_db();
        let (dat, set) = insert_dat_and_set(&conn);
        insert_rom(&conn, &dat, &set, "game.rom", "deadbeef", None);

        let found = RomRecord::find_by_hash_in_dat(&conn, dat.id, "deadbeef").unwrap();
        assert_eq!(found.len(), 1);

        let not_found = RomRecord::find_by_hash_in_dat(&conn, dat.id, "000000").unwrap();
        assert_eq!(not_found.len(), 0);
    }

    #[test]
    fn get_roms_by_sets() {
        let conn = mem_db();
        let (dat, set1) = insert_dat_and_set(&conn);
        let set2 = SetRecord::insert(&conn, NewSet::new(dat.id, "Set 2")).unwrap();
        insert_rom(&conn, &dat, &set1, "a.rom", "aaa", None);
        insert_rom(&conn, &dat, &set2, "b.rom", "bbb", None);

        let ids = vec![set1.id, set2.id];
        let map = RomRecord::get_by_sets(&conn, &ids).unwrap();
        assert_eq!(map.len(), 2);
        assert_eq!(map[&set1.id].len(), 1);
        assert_eq!(map[&set2.id].len(), 1);
    }

    #[test]
    fn get_crcs_by_dat_returns_only_populated_crcs() {
        let conn = mem_db();
        let (dat, set1) = insert_dat_and_set(&conn);
        let set2 = SetRecord::insert(&conn, NewSet::new(dat.id, "Set 2")).unwrap();
        insert_rom(&conn, &dat, &set1, "a.rom", "aaa", Some("deadbeef"));
        insert_rom(&conn, &dat, &set2, "b.rom", "bbb", None);
        insert_rom(&conn, &dat, &set2, "c.rom", "ccc", Some("cafebabe"));

        let crcs = RomRecord::get_crcs_by_dat(&conn, dat.id).unwrap();
        assert_eq!(crcs, BTreeSet::from(["cafebabe".to_string(), "deadbeef".to_string()]));
    }

    #[test]
    fn get_roms_by_sets_empty() {
        let conn = mem_db();
        let ids: Vec<SetId> = vec![];
        let map = RomRecord::get_by_sets(&conn, &ids).unwrap();
        assert!(map.is_empty());
    }

    #[test]
    fn get_by_ids_empty() {
        let conn = mem_db();
        let ids: Vec<DatId> = vec![];
        let result = DatRecord::get_by_ids(&conn, &ids).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn get_by_ids() {
        let conn = mem_db();
        let d1 = DatRecord::insert(&conn, sample_dat()).unwrap();
        let d2 = DatRecord::insert(&conn, NewDat::new("Second", "A test dat file", "1.0", "tester", "sha1")).unwrap();

        let ids = vec![d1.id, d2.id];
        let result = DatRecord::get_by_ids(&conn, &ids).unwrap();
        assert_eq!(result.len(), 2);
    }

    // --- DirRecord ---

    fn insert_dir(conn: &Connection, dat: &DatRecord, path: &str) -> DirRecord {
        DirRecord::insert(conn, NewDir::new(dat.id, path)).unwrap()
    }

    #[test]
    fn insert_and_query_dirs() {
        let conn = mem_db();
        let (dat, _) = insert_dat_and_set(&conn);
        insert_dir(&conn, &dat, "/roms/snes");

        let dirs = DirRecord::get_by_dat(&conn, dat.id).unwrap();
        assert_eq!(dirs.len(), 1);
    }

    #[test]
    fn find_dir_by_path() {
        let conn = mem_db();
        let (dat, _) = insert_dat_and_set(&conn);
        insert_dir(&conn, &dat, "/roms/snes");

        let found = DirRecord::find_by_path_in_dat(&conn, dat.id, "/roms/snes").unwrap();
        assert!(found.is_some());

        let not_found = DirRecord::find_by_path_in_dat(&conn, dat.id, "/roms/nes").unwrap();
        assert!(not_found.is_none());
    }

    #[test]
    fn dir_get_children() {
        let conn = mem_db();
        let (dat, _) = insert_dat_and_set(&conn);
        let parent = insert_dir(&conn, &dat, "/roms");
        insert_dir(&conn, &dat, "/roms/snes");
        insert_dir(&conn, &dat, "/roms/nes");
        insert_dir(&conn, &dat, "/other");

        let children = parent.get_children(&conn).unwrap();
        assert_eq!(children.len(), 2);
    }

    #[test]
    fn dir_get_children_escapes_like() {
        let conn = mem_db();
        let (dat, _) = insert_dat_and_set(&conn);
        let tricky = insert_dir(&conn, &dat, "/roms/100%");
        insert_dir(&conn, &dat, "/roms/100%/sub");
        insert_dir(&conn, &dat, "/roms/100xyz");

        let children = tricky.get_children(&conn).unwrap();
        // Should only find /roms/100%/sub, not /roms/100xyz
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].path, "/roms/100%/sub");
    }

    #[test]
    fn relink_dirs() {
        let conn = mem_db();
        let dat1 = DatRecord::insert(&conn, sample_dat()).unwrap();
        let dat2 =
            DatRecord::insert(&conn, NewDat::new("New DAT", "A test dat file", "1.0", "tester", "sha1")).unwrap();
        insert_dir(&conn, &dat1, "/roms");

        let updated = DirRecord::relink_dirs(&conn, dat1.id, dat2.id).unwrap();
        assert_eq!(updated, 1);
        assert_eq!(DirRecord::get_by_dat(&conn, dat1.id).unwrap().len(), 0);
        assert_eq!(DirRecord::get_by_dat(&conn, dat2.id).unwrap().len(), 1);
    }

    // --- FileRecord ---

    fn insert_file(conn: &Connection, dat: &DatRecord, dir: &DirRecord, name: &str) -> FileRecord {
        FileRecord::insert(conn, NewFile::new(dat.id, dir.id, name, 2048, "filehash")).unwrap()
    }

    #[test]
    fn insert_and_get_files_by_dir() {
        let conn = mem_db();
        let (dat, _) = insert_dat_and_set(&conn);
        let dir = insert_dir(&conn, &dat, "/roms");
        insert_file(&conn, &dat, &dir, "game.zip");

        let files = dir.get_files(&conn).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].name, "game.zip");
        assert_eq!(files[0].size, 2048);
    }

    #[test]
    fn find_files_exact_and_fuzzy() {
        let conn = mem_db();
        let (dat, _) = insert_dat_and_set(&conn);
        let dir = insert_dir(&conn, &dat, "/roms");
        insert_file(&conn, &dat, &dir, "game.zip");
        insert_file(&conn, &dat, &dir, "another_game.zip");

        let exact = dir.find_files(&conn, "game.zip", true).unwrap();
        assert_eq!(exact.len(), 1);

        let fuzzy = dir.find_files(&conn, "game", false).unwrap();
        assert_eq!(fuzzy.len(), 2);
    }

    #[test]
    fn update_file_name() {
        let conn = mem_db();
        let (dat, _) = insert_dat_and_set(&conn);
        let dir = insert_dir(&conn, &dat, "/roms");
        let file = insert_file(&conn, &dat, &dir, "old.zip");

        let updated = file.update_name(&conn, "new.zip").unwrap();
        assert_eq!(updated.name, "new.zip");
        assert_eq!(updated.id, file.id);

        let fetched = FileRecord::get_by_id(&conn, file.id).unwrap();
        assert_eq!(fetched.name, "new.zip");
    }

    #[test]
    fn delete_files_by_dir() {
        let conn = mem_db();
        let (dat, _) = insert_dat_and_set(&conn);
        let dir = insert_dir(&conn, &dat, "/roms");
        insert_file(&conn, &dat, &dir, "a.zip");
        insert_file(&conn, &dat, &dir, "b.zip");

        let deleted = dir.delete_files(&conn).unwrap();
        assert_eq!(deleted, 2);
        assert_eq!(dir.get_files(&conn).unwrap().len(), 0);
    }

    #[test]
    fn update_file_hash() {
        let conn = mem_db();
        let (dat, _) = insert_dat_and_set(&conn);
        let dir = insert_dir(&conn, &dat, "/roms");
        let file = insert_file(&conn, &dat, &dir, "game.zip");

        let updated = file.update_hash(&conn, "newhash").unwrap();
        assert_eq!(updated.hash, "newhash");
        assert_eq!(updated.id, file.id);

        let fetched = FileRecord::get_by_id(&conn, file.id).unwrap();
        assert_eq!(fetched.hash, "newhash");
    }

    #[test]
    fn relink_files() {
        let conn = mem_db();
        let dat1 = DatRecord::insert(&conn, sample_dat()).unwrap();
        let dat2 =
            DatRecord::insert(&conn, NewDat::new("New DAT", "A test dat file", "1.0", "tester", "sha1")).unwrap();
        let dir = insert_dir(&conn, &dat1, "/roms");
        insert_file(&conn, &dat1, &dir, "game.zip");

        let updated = FileRecord::relink_files(&conn, dat1.id, dat2.id).unwrap();
        assert_eq!(updated, 1);
    }

    // --- MatchRecord ---

    #[test]
    fn insert_and_query_matches() {
        let conn = mem_db();
        let (dat, set) = insert_dat_and_set(&conn);
        let rom = insert_rom(&conn, &dat, &set, "game.rom", "abc", None);
        let dir = insert_dir(&conn, &dat, "/roms");
        let file = insert_file(&conn, &dat, &dir, "game.zip");

        let m = MatchRecord::insert(&conn, NewMatch::new(dat.id, file.id, MatchStatus::Hash, set.id, rom.id)).unwrap();

        assert_eq!(m.status, MatchStatus::Hash);

        let by_dat = MatchRecord::get_by_dat(&conn, dat.id).unwrap();
        assert_eq!(by_dat.len(), 1);
    }

    #[test]
    fn update_match_status() {
        let conn = mem_db();
        let (dat, set) = insert_dat_and_set(&conn);
        let rom = insert_rom(&conn, &dat, &set, "game.rom", "abc", None);
        let dir = insert_dir(&conn, &dat, "/roms");
        let file = insert_file(&conn, &dat, &dir, "game.zip");

        let m = MatchRecord::insert(&conn, NewMatch::new(dat.id, file.id, MatchStatus::Hash, set.id, rom.id)).unwrap();

        let updated = m.update(&conn, MatchStatus::Match).unwrap();
        assert_eq!(updated.status, MatchStatus::Match);

        let fetched = MatchRecord::get_by_id(&conn, m.id).unwrap();
        assert_eq!(fetched.status, MatchStatus::Match);
    }

    #[test]
    fn delete_matches_by_file() {
        let conn = mem_db();
        let (dat, set) = insert_dat_and_set(&conn);
        let rom = insert_rom(&conn, &dat, &set, "game.rom", "abc", None);
        let dir = insert_dir(&conn, &dat, "/roms");
        let file = insert_file(&conn, &dat, &dir, "game.zip");

        MatchRecord::insert(&conn, NewMatch::new(dat.id, file.id, MatchStatus::Hash, set.id, rom.id)).unwrap();

        let deleted = file.delete_matches(&conn).unwrap();
        assert_eq!(deleted, 1);
    }

    #[test]
    fn delete_matches_by_dir() {
        let conn = mem_db();
        let (dat, set) = insert_dat_and_set(&conn);
        let rom = insert_rom(&conn, &dat, &set, "game.rom", "abc", None);
        let dir = insert_dir(&conn, &dat, "/roms");
        let file = insert_file(&conn, &dat, &dir, "game.zip");

        MatchRecord::insert(&conn, NewMatch::new(dat.id, file.id, MatchStatus::Hash, set.id, rom.id)).unwrap();

        let deleted = dir.delete_matches(&conn).unwrap();
        assert_eq!(deleted, 1);
    }

    #[test]
    fn delete_matches_by_dat() {
        let conn = mem_db();
        let (dat, set) = insert_dat_and_set(&conn);
        let rom = insert_rom(&conn, &dat, &set, "game.rom", "abc", None);
        let dir = insert_dir(&conn, &dat, "/roms");
        let file = insert_file(&conn, &dat, &dir, "game.zip");

        MatchRecord::insert(&conn, NewMatch::new(dat.id, file.id, MatchStatus::Hash, set.id, rom.id)).unwrap();

        let deleted = MatchRecord::delete_by_dat(&conn, dat.id).unwrap();
        assert_eq!(deleted, 1);
    }

    #[test]
    fn find_matches_by_status_for_dat() {
        let conn = mem_db();
        let (dat, set) = insert_dat_and_set(&conn);
        let rom = insert_rom(&conn, &dat, &set, "game.rom", "abc", None);
        let dir = insert_dir(&conn, &dat, "/roms");
        let file = insert_file(&conn, &dat, &dir, "game.zip");

        MatchRecord::insert(&conn, NewMatch::new(dat.id, file.id, MatchStatus::Hash, set.id, rom.id)).unwrap();
        MatchRecord::insert(&conn, NewMatch::new(dat.id, file.id, MatchStatus::Name, set.id, rom.id)).unwrap();

        let hash_matches = MatchRecord::find_by_status_for_dat(&conn, dat.id, MatchStatus::Hash).unwrap();
        assert_eq!(hash_matches.len(), 1);

        let name_matches = MatchRecord::find_by_status_for_dat(&conn, dat.id, MatchStatus::Name).unwrap();
        assert_eq!(name_matches.len(), 1);

        let match_matches = MatchRecord::find_by_status_for_dat(&conn, dat.id, MatchStatus::Match).unwrap();
        assert_eq!(match_matches.len(), 0);
    }

    // --- StoredU64 round-trip ---

    #[test]
    fn stored_u64_large_value() {
        let conn = mem_db();
        let (dat, set) = insert_dat_and_set(&conn);
        let big: u64 = u64::MAX;
        let rom = RomRecord::insert(&conn, NewRom::new(dat.id, set.id, "big.rom", big, "h", None)).unwrap();
        assert_eq!(rom.size, big);

        let fetched = RomRecord::get_by_id(&conn, rom.id).unwrap();
        assert_eq!(fetched.size, big);
    }

    // --- MatchStatus round-trip ---

    #[test]
    fn match_status_roundtrip() {
        use rusqlite::types::{FromSql, ToSql, ValueRef};

        for status in [MatchStatus::Hash, MatchStatus::Name, MatchStatus::Match] {
            let sql_val = status.to_sql().unwrap();
            assert!(matches!(&sql_val, rusqlite::types::ToSqlOutput::Borrowed(ValueRef::Text(_))));
            let rusqlite::types::ToSqlOutput::Borrowed(ValueRef::Text(t)) = &sql_val else {
                return;
            };
            let s = std::str::from_utf8(t).unwrap();
            let back = MatchStatus::column_result(ValueRef::Text(s.as_bytes())).unwrap();
            assert_eq!(back, status);
        }
    }

    // --- Transaction helpers ---

    #[test]
    fn with_transaction_commits() {
        let mut conn = mem_db();
        with_transaction(&mut conn, |tx| {
            DatRecord::insert(tx, sample_dat())?;
            Ok(())
        })
        .unwrap();
        assert_eq!(DatRecord::get_all(&conn).unwrap().len(), 1);
    }

    #[test]
    fn with_transaction_rolls_back_on_error() {
        let mut conn = mem_db();
        let result: Result<()> = with_transaction(&mut conn, |tx| {
            DatRecord::insert(tx, sample_dat())?;
            anyhow::bail!("forced error");
        });
        assert!(result.is_err());
        assert_eq!(DatRecord::get_all(&conn).unwrap().len(), 0);
    }
}
