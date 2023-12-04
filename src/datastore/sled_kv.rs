use sled::{Config, Db};
use serde::{de::DeserializeOwned, Serialize};
use crate::{error::StoreError, stable::StableStore};

pub struct RaftSledKVStore {
    db: Db,
}

impl RaftSledKVStore {
    pub fn new(path: &str) -> Result<Self, sled::Error> {
        let config = Config::new()
        .path(path)
        .cache_capacity(1_000_000)
        .mode(sled::Mode::LowSpace);
        // .temporary(true);

        let db = config.open()?;
        Ok(Self { db })
    }

    fn get<V>(&self, key: &str) -> Result<Option<V>, StoreError>
    where
        V: DeserializeOwned,
    {
        let key_bytes = key.as_bytes();
        let result = self.db.get(&key_bytes).map_err(|e| StoreError::Error(e.to_string()))?;

        match result {
            Some(ivec) => {
                let value: V = bincode::deserialize(&ivec).map_err(|e| StoreError::Error(e.to_string()))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    fn set<V>(&self, key: &str, value: &V) -> Result<(), StoreError>
    where
        V: Serialize,
    {
        let key_bytes = key.as_bytes();
        let value_bytes = bincode::serialize(value).map_err(|e| StoreError::Error(e.to_string()))?;

        self.db
            .insert(&key_bytes, value_bytes)
            .map_err(|e| StoreError::Error(e.to_string()))?;
        
        self.db.flush().map_err(|e| StoreError::Error(e.to_string()))?;
        Ok(())
    }
}

impl StableStore for RaftSledKVStore {
    fn  get(&self, key: &str) -> Result<Option<u64>, StoreError>{
        self.get(key)
    }
    fn set(&self, key: &str, value: u64) -> Result<(), StoreError>{
        self.set(key, &value)
    }
    fn  get_str(&self, key: &str) -> Result<Option<String>, StoreError>{
        self.get(key)
    }
    fn set_str(&self, key: &str, value: String) -> Result<(), StoreError>{
        self.set(key, &value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
   
    #[test]
    fn test_set() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let store = RaftSledKVStore::new(path).unwrap();  

        let key = "commit_index";
        let value = 21;

        store
            .set(key, &value)
            .expect("Failed to set value in stable storage");


        let retrieved_value_bytes: Option<Vec<u8>> = store
            .db.get(key)
            .expect("Failed to get value from stable storage")
            .map(|ivec| ivec.to_vec());

        let retrieved_value: Option<i32> = retrieved_value_bytes
            .map(|bytes| {
                bincode::deserialize(&bytes).expect("Failed to deserialize retrieved value")
            });

        assert_eq!(retrieved_value, Some(value));
    }

    #[test]
    fn test_get() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let store = RaftSledKVStore::new(path).unwrap();  

        let key = "test_key";
        let value = "test_value";

        store
            .set(key, &value)
            .expect("Failed to set value in stable storage");

        let retrieved_value: Option<String> =  store
            .get(key)
            .expect("Failed to get value in stable storage");

        assert_eq!(retrieved_value, Some(value.to_string()));
    }

}
