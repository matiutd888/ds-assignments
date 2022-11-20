use sha2::{digest::FixedOutput, Digest, Sha256};
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};
use tokio::{
    fs::{rename, File, self},
    io::{AsyncWriteExt, AsyncReadExt},
    sync::Mutex,
};
// You can add here other imports from std or crates listed in Cargo.toml.

// You can add any private types, structs, consts, functions, methods, etc., you need.

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
    unimplemented!()
}

struct StableStorageImpl {
    root_storage_dir: PathBuf,
    mutex_guard: Mutex<()>,
}

impl StableStorageImpl {
    fn append_to_path(&self, append_str: String) -> OsString {
        let mut copy = self.root_storage_dir.clone();

        copy.push(append_str);
        return copy.into_os_string();
    }

    // TODO these two functions should be one.
    fn check_if_key_too_big(key_size: usize) -> Option<String> {
        let KEY_SIZE_LIMIT = 255;
        if key_size > KEY_SIZE_LIMIT {
            return Some(format!(
                "Too big key, size = {}, expected = {}",
                key_size, KEY_SIZE_LIMIT
            ));
        }
        None
    }

    fn check_if_value_too_big(value_size: usize) -> Option<String> {
        let VALUE_SIZE_LIMIT = 65535;
        if value_size > VALUE_SIZE_LIMIT {
            return Some(format!(
                "Too big value, size = {}, expected = {}",
                value_size, VALUE_SIZE_LIMIT
            ));
        }
        None
    }

    fn encode(key: &str) -> String {
        let mut hasher = Sha256::new(); // write input message
        hasher.update(key);
        format!("{:X}", FixedOutput::finalize_fixed(hasher))
    }

    fn create_tmp_code(code: &str) -> String {
        let mut tmp_name: String = code.parse().unwrap();
        tmp_name.push_str(code);
        Self::encode(&*tmp_name)
    }
}

// TODO should get() be concurrent?

// Pomysł:
// -W specjalnym pliku trzymamy wszystkie dostępne klucze
// - Mamy globalnego locka na put, remove (i get??)
// - put
//      - skopiuj plik z kluczami, nadpisz go

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

        // TODO should I wait for the result?
        // TODO I should put mutex here.
        // TODO why the tmp files?
        {
            let _ = self.mutex_guard.lock().await;
            rename(tmp_key_path, key_path).await.unwrap();
            directory.sync_data().await.unwrap();
        }

        return Ok(());
    }

    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        const BUFFER_SIZE: usize = 2048;

        if let Some(_) = Self::check_if_key_too_big(key.len()) {
            return None;
        }
        let key_path = self.append_to_path(Self::encode(key));
    

        let result = fs::read(key_path).await;
        if result.is_err() {
            return None;
        }
        return Some(result.unwrap());
    }

    async fn remove(&mut self, key: &str) -> bool {
        todo!()
    }
}
