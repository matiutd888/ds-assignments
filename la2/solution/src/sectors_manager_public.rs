use std::collections::HashMap;
use std::ffi::OsString;
use std::io::Error;

use tokio::fs::{self, rename, File};
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;

use crate::{constants, SectorIdx, SectorVec};
use std::path::PathBuf;
use std::sync::Arc;

#[async_trait::async_trait]
pub trait SectorsManager: Send + Sync {
    /// Returns 4096 bytes of sector data by index.
    async fn read_data(&self, idx: SectorIdx) -> SectorVec;

    /// Returns timestamp and write rank of the process which has saved this data.
    /// Timestamps and ranks are relevant for atomic register algorithm, and are described
    /// there.
    async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8);

    // It can store metadata in filename
    /// Writes a new data, along with timestamp and write rank to some sector.
    async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8));
}

/// Path parameter points to a directory to which this method has exclusive access.
/// Path parameter points to a directory to which this method has exclusive access.
pub async fn build_sectors_manager(path: PathBuf) -> Arc<dyn SectorsManager> {
    match SectorsManagerImpl::build_recover(path).await {
        Ok(s) => Arc::new(s),
        Err(e) => {
            panic!("Failed to build sector manager {}", e);
        }
    }
}

pub type Timestamp = u64;
pub type WriteRank = u8;

pub type Metadata = (Timestamp, WriteRank);

struct SectorsManagerImpl {
    sectors_metadata: RwLock<HashMap<SectorIdx, Metadata>>,
    root_storage_dir: PathBuf,
}

impl SectorsManagerImpl {
    const DELIMITER: &str = "-";
    const TMP_PREFIX: &str = "tmp";

    pub async fn build_recover(path: PathBuf) -> Result<SectorsManagerImpl, Error> {
        async fn cleanup_if_tmp_file(v: &Vec<&str>, path: &PathBuf) -> Result<(), Error> {
            if v.len() == 2 {
                let tmp_str = v.get(0).unwrap();
                let idx = v.get(1).unwrap().parse::<u64>();
                if tmp_str.eq(&SectorsManagerImpl::TMP_PREFIX) && idx.is_ok() {
                    fs::remove_file(path).await?;
                }
            }
            Ok(())
        }

        fn read_and_add_metadata_if_sector_file(
            v: &Vec<&str>,
            metadata_map: &mut HashMap<SectorIdx, (Timestamp, WriteRank)>,
        ) {
            if v.len() == 3 {
                let idx = v.get(0).unwrap().parse::<u64>();
                let timestamp = v.get(1).unwrap().parse::<u64>();
                let write_rank = v.get(2).unwrap().parse::<u8>();
                if idx.is_ok() && timestamp.is_ok() && write_rank.is_ok() {
                    assert!(
                        metadata_map
                            .insert(idx.unwrap(), (timestamp.unwrap(), write_rank.unwrap()))
                            == None
                    );
                }
            }
        }

        let mut metadata_map = HashMap::new();

        let paths = std::fs::read_dir(&path).unwrap();

        for path_it in paths {
            let p = path_it?.path();

            if let Some(filename) = p.file_name() {
                if let Some(string) = filename.to_str() {
                    let split = string.split(SectorsManagerImpl::DELIMITER);
                    let v = split.collect::<Vec<&str>>();
                    read_and_add_metadata_if_sector_file(&v, &mut metadata_map);
                    cleanup_if_tmp_file(&v, &p).await?;
                }
            }
        }
        return Ok(SectorsManagerImpl {
            sectors_metadata: RwLock::new(metadata_map),
            root_storage_dir: path,
        });
    }
    const DEFAULT_METADATA: Metadata = (0, 0);

    fn create_filename(idx: &SectorIdx, metadata: &Metadata) -> String {
        let v = vec![
            idx.to_string(),
            metadata.0.to_string(),
            metadata.1.to_string(),
        ];
        v.join(SectorsManagerImpl::DELIMITER)
    }

    fn append_to_path(&self, append_str: String) -> OsString {
        let mut copy = self.root_storage_dir.clone();

        copy.push(append_str);
        copy.into_os_string()
    }

    fn create_tmp_filename(idx: SectorIdx) -> String {
        let idx_str = idx.to_string();
        let v = vec![SectorsManagerImpl::TMP_PREFIX, &idx_str];
        v.join(SectorsManagerImpl::DELIMITER)
    }
}

#[async_trait::async_trait]
impl SectorsManager for SectorsManagerImpl {
    /// Returns 4096 bytes of sector data by index.
    async fn read_data(&self, idx: SectorIdx) -> SectorVec {
        let result_vec = if let Some(metadata) = self.sectors_metadata.read().await.get(&idx) {
            let file_to_read = Self::create_filename(&idx, metadata);
            let file_path = self.append_to_path(file_to_read);
            let result = fs::read(file_path).await.unwrap();
            result
        } else {
            // If a sector was never written, we assume that both the logical timestamp and the write rank are 0, and that it contains 4096 zero bytes.
            vec![0; constants::SECTOR_SIZE_BYTES]
        };
        assert!(result_vec.len() == constants::SECTOR_SIZE_BYTES);

        SectorVec(result_vec)
    }

    /// Returns timestamp and write rank of the process which has saved this data.
    /// Timestamps and ranks are relevant for atomic register algorithm, and are described
    /// there.
    async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8) {
        if let Some(metadata) = self.sectors_metadata.read().await.get(&idx) {
            metadata.clone()
        } else {
            Self::DEFAULT_METADATA.clone()
        }
    }

    // It can store metadata in filename
    /// Writes a new data, along with timestamp and write rank to some sector.
    async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8)) {
        let key_path = self.append_to_path(Self::create_filename(&idx, &(sector.1, sector.2)));
        let tmp_path = self.append_to_path(Self::create_tmp_filename(idx));

        let directory = File::open(self.root_storage_dir.clone().into_os_string())
            .await
            .unwrap();

        let mut tmp_file = tokio::fs::File::create(tmp_path.clone()).await.unwrap();
        tmp_file.write_all(&(sector.0 .0)).await.unwrap();
        tmp_file.sync_data().await.unwrap();

        rename(tmp_path, key_path).await.unwrap();
        directory.sync_data().await.unwrap();

        let should_remove_file = self.sectors_metadata.read().await.get(&idx).cloned();

        {
            let mut w = self.sectors_metadata.write().await;
            (*w).insert(idx, (sector.1, sector.2));
        }

        if let Some(metadata) = should_remove_file {
            let path_to_remove = self.append_to_path(Self::create_filename(&idx, &metadata));
            fs::remove_file(path_to_remove).await.unwrap();
            directory.sync_data().await.unwrap();
        }
    }
}
