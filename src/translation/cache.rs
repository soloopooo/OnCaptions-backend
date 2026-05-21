use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::PathBuf;
use std::sync::Mutex;

#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub original: String,
    pub translated: String,
    pub provider: String,
    pub target_lang: String,
    pub timestamp: String,
}

pub struct TranslationCache {
    db: Mutex<Option<rusqlite::Connection>>,
    path: PathBuf,
}

impl TranslationCache {
    pub fn new(db_dir: PathBuf) -> Self {
        let path = db_dir.join("translations.db");
        let conn = Self::open_db(&path);
        Self {
            db: Mutex::new(conn),
            path,
        }
    }

    fn open_db(path: &PathBuf) -> Option<rusqlite::Connection> {
        let conn = rusqlite::Connection::open(path).ok()?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS translations (
                text_hash  TEXT NOT NULL,
                original   TEXT NOT NULL,
                provider   TEXT NOT NULL,
                target_lang TEXT NOT NULL,
                translated TEXT NOT NULL,
                timestamp  TEXT NOT NULL,
                PRIMARY KEY (text_hash, provider, target_lang)
            );
            CREATE INDEX IF NOT EXISTS idx_translations_ts ON translations(timestamp);",
        )
        .ok()?;
        Some(conn)
    }

    fn hash(text: &str) -> String {
        let mut hasher = DefaultHasher::new();
        text.hash(&mut hasher);
        format!("{:x}", hasher.finish())
    }

    pub fn get(&self, text: &str, provider: &str, target_lang: &str) -> Option<String> {
        let guard = self.db.lock().ok()?;
        let conn = guard.as_ref()?;
        let hash = Self::hash(text);
        let mut stmt = conn
            .prepare("SELECT translated FROM translations WHERE text_hash = ?1 AND provider = ?2 AND target_lang = ?3")
            .ok()?;
        let mut rows = stmt
            .query_map(rusqlite::params![hash, provider, target_lang], |row| {
                row.get::<_, String>(0)
            })
            .ok()?;
        rows.next().and_then(|r| r.ok())
    }

    pub fn put(&self, text: &str, provider: &str, target_lang: &str, translated: &str) {
        let guard = match self.db.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        let conn = match guard.as_ref() {
            Some(c) => c,
            None => return,
        };
        let hash = Self::hash(text);
        let ts = unix_ts();
        let _ = conn.execute(
            "INSERT OR REPLACE INTO translations (text_hash, original, provider, target_lang, translated, timestamp)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![hash, text, provider, target_lang, translated, ts],
        );
    }

    #[allow(dead_code)]
    pub fn recent(&self, limit: usize) -> Vec<CacheEntry> {
        let guard = match self.db.lock() {
            Ok(g) => g,
            Err(_) => return vec![],
        };
        let conn = match guard.as_ref() {
            Some(c) => c,
            None => return vec![],
        };
        let mut stmt = match conn.prepare(
            "SELECT original, translated, provider, target_lang, timestamp
             FROM translations ORDER BY timestamp DESC LIMIT ?1",
        ) {
            Ok(s) => s,
            Err(_) => return vec![],
        };
        let rows: Vec<CacheEntry> = stmt
            .query_map(rusqlite::params![limit as i64], |row| {
                Ok(CacheEntry {
                    original: row.get(0)?,
                    translated: row.get(1)?,
                    provider: row.get(2)?,
                    target_lang: row.get(3)?,
                    timestamp: row.get(4)?,
                })
            })
            .ok()
            .into_iter()
            .flatten()
            .filter_map(|r| r.ok())
            .collect();
        rows
    }
}

impl Clone for TranslationCache {
    fn clone(&self) -> Self {
        Self {
            db: Mutex::new(Self::open_db(&self.path)),
            path: self.path.clone(),
        }
    }
}

fn unix_ts() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}", d.as_secs())
}
