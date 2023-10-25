use crate::error::StoreError;

pub trait StableStore: Send + Sync {
    fn set(&self, key: &str, value: u64) -> Result<(), StoreError>;
    fn get(&self, key: &str) -> Result<Option<u64>, StoreError>;
    fn set_str(&self, key: &str, value: String) -> Result<(), StoreError>;
    fn get_str(&self, key: &str) ->  Result<Option<String>, StoreError>;
}