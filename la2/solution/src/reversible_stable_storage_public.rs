use sha2::{digest::FixedOutput, Digest, Sha256};
use std::str;
use std::{
    collections::HashSet,
    ffi::OsString,
    path::{Path, PathBuf},
};
use tokio::{
    fs::{self, rename, File},
    io::AsyncWriteExt,
};

// You can add here other imports from std or crates listed in Cargo.toml.

// You can add any private types, structs, consts, functions, methods, etc., you need.

#[async_trait::async_trait]
pub trait ReversibleStableStorage: Send + Sync {
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

    fn get_all_keys(&self) -> HashSet<String>;
}

/// Creates a new instance of stable storage.
pub async fn build_reversible_stable_storage(
    root_storage_dir: PathBuf,
) -> Box<dyn ReversibleStableStorage> {
    let paths = std::fs::read_dir(&root_storage_dir).unwrap();

    let mut keys: HashSet<String> = HashSet::new();

    for path_it in paths {
        if let Some(filename) = path_it.unwrap().path().file_name() {
            if let Some(string) = filename.to_str() {
                // If string.contains '-' then the file was a tmp file
                if !string.contains('-') {
                    if let Some(decoded_key) = ReversibleStableStorageImpl::decode(string) {
                        keys.insert(decoded_key);
                    }
                }
            }
        }
    }
    
    Box::new(ReversibleStableStorageImpl {
        root_storage_dir: root_storage_dir,
        keys: keys,
    })
}

struct ReversibleStableStorageImpl {
    root_storage_dir: PathBuf,
    // mutex_guard: Mutex<()>,
    keys: HashSet<String>,
}

impl ReversibleStableStorageImpl {
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
        base64::encode(key)

        // let mut hasher = Sha256::new(); // write input message
        // hasher.update(key);
        // format!("{:X}", FixedOutput::finalize_fixed(hasher))
    }

    fn decode(key: &str) -> Option<String> {
        if let Ok(s_vec) = base64::decode(key) {
            str::from_utf8(&s_vec).ok().map(|x| x.to_owned())
        } else {
            None
        }
    }

    fn create_tmp_code(key: &str) -> String {
        let mut encoded_key = String::from("tmp-");
        encoded_key.push_str(&Self::encode(key));
        encoded_key
    }
}

#[async_trait::async_trait]
impl ReversibleStableStorage for ReversibleStableStorageImpl {
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
            self.keys.insert(key.to_owned());
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
        self.keys.remove(key);
        return true;
    }

    fn get_all_keys(&self) -> HashSet<String> {
        self.keys.clone()
    }
}
