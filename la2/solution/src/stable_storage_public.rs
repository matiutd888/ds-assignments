use sha2::{digest::FixedOutput, Digest, Sha256};
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};
use tokio::{
    fs::{self, rename, File},
    io::AsyncWriteExt,
};

#[async_trait::async_trait]
pub trait StableStorage: Send + Sync {
    /// Stores `value` under `key`.
    ///
    /// Detailed requirements are specified in the description of the assignment.
    async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String>;

    /// Retrieves value stored under `key`.
    ///
    /// Detailed requirements are specified in the description of the assignment.
    async fn get(&self, key: &str) -> Option<Vec<u8>>;

    /// Removes `key` and the value stored under it.
    ///
    /// Detailed requirements are specified in the description of the assignment.
    async fn remove(&mut self, key: &str) -> bool;
}

/// Creates a new instance of stable storage.
pub async fn build_stable_storage(root_storage_dir: PathBuf) -> Box<dyn StableStorage> {
    Box::new(StableStorageImpl {
        root_storage_dir: root_storage_dir,
    })
}

struct StableStorageImpl {
    root_storage_dir: PathBuf,
}

impl StableStorageImpl {
    fn append_to_path(&self, append_str: String) -> OsString {
        let mut copy = self.root_storage_dir.clone();

        copy.push(append_str);
        return copy.into_os_string();
    }

    fn check_limit(limit: usize, size: usize, communicate: &str) -> Option<String> {
        if size > limit {
            let mut ret_msg: String = communicate.to_owned();
            ret_msg.push_str(&format!(", size = {}, expected = {}", size, limit));
            return Some(ret_msg);
        }
        return None;
    }

    fn check_if_key_too_big(key_size: usize) -> Option<String> {
        let key_size_limit = 255;
        Self::check_limit(key_size_limit, key_size, "Too big key")
    }

    fn check_if_value_too_big(value_size: usize) -> Option<String> {
        let value_size_limit = 65535;
        Self::check_limit(value_size_limit, value_size, "Too big value")
    }

    fn encode(key: &str) -> String {
        let mut hasher = Sha256::new(); // write input message
        hasher.update(key);
        format!("{:X}", FixedOutput::finalize_fixed(hasher))
    }

    fn create_tmp_code(key: &str) -> String {
        let mut encoded_key = Self::encode(key);
        encoded_key.push_str("tmp");
        encoded_key
    }
}

#[async_trait::async_trait]
impl StableStorage for StableStorageImpl {
    async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String> {
        if let Some(error_msg) = Self::check_if_key_too_big(key.len()) {
            return Err(error_msg);
        }
        if let Some(error_msg) = Self::check_if_value_too_big(value.len()) {
            return Err(error_msg);
        }

        let tmp_key_path = self.append_to_path(Self::create_tmp_code(key));
        let key_path = self.append_to_path(Self::encode(key));

        let directory = File::open(self.root_storage_dir.clone().into_os_string())
            .await
            .unwrap();
        let mut tmp_file = tokio::fs::File::create(tmp_key_path.clone()).await.unwrap();
        tmp_file.write_all(value).await.unwrap();
        tmp_file.sync_data().await.unwrap();

        {
            rename(tmp_key_path, key_path).await.unwrap();
            directory.sync_data().await.unwrap();
        }

        return Ok(());
    }

    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        if let Some(_) = Self::check_if_key_too_big(key.len()) {
            return None;
        }
        let key_path = self.append_to_path(Self::encode(key));
        if !Path::new(&key_path).exists() {
            return None;
        }
        let result = fs::read(key_path).await.unwrap();
        return Some(result);
    }

    async fn remove(&mut self, key: &str) -> bool {
        let key_path = self.append_to_path(Self::encode(key));
        if !Path::new(&key_path).exists() {
            return false;
        }
        let directory = File::open(self.root_storage_dir.clone().into_os_string())
            .await
            .unwrap();
        fs::remove_file(key_path).await.unwrap();
        directory.sync_data().await.unwrap();
        return true;
    }
}
