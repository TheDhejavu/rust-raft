use sled::{Db, IVec};
use std::str;

pub struct MyDatabase {
    db: Db,
}

impl MyDatabase {
    // Open a new database or existing one from the given path
    pub fn new(path: &str) -> Result<Self, sled::Error> {
        let db = sled::open(path)?;
        Ok(MyDatabase { db })
    }

    // Add a new key-value pair to the database
    pub fn add(&self, key: &str, value: &str) -> Result<(), sled::Error> {
        self.db.insert(key.as_bytes(), value.as_bytes())?;
        Ok(())
    }

    // Delete a key and its associated value from the database
    pub fn delete(&self, key: &str) -> Result<(), sled::Error> {
        self.db.remove(key.as_bytes())?;
        Ok(())
    }

    // Inspect a value associated with a given key
    pub fn inspect(&self, key: &str) -> Result<Option<String>, sled::Error> {
        match self.db.get(key.as_bytes())? {
            Some(ivec) => Ok(Some(str::from_utf8(&ivec).unwrap().to_string())),
            None => Ok(None),
        }
    }

    // List all key-value pairs in the database
    pub fn list(&self) -> Result<Vec<(String, String)>, sled::Error> {
        let mut results = Vec::new();
        for entry in self.db.iter() {
            let (key, value) = entry?;
            results.push((
                str::from_utf8(&key).unwrap().to_string(),
                str::from_utf8(&value).unwrap().to_string(),
            ));
        }
        Ok(results)
    }
}
