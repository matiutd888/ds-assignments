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
    const FAILED_WRITE_PREFIX: &str = "failed";

    pub async fn build_recover(root_path: PathBuf) -> Result<SectorsManagerImpl, Error> {
        async fn cleanup_if_tmp_file(v: &Vec<&str>, file_path: &PathBuf) -> Result<(), Error> {
            if v.len() == 2 {
                let tmp_str = v.get(0).unwrap();
                let idx = v.get(1).unwrap().parse::<u64>();
                if tmp_str.eq(&SectorsManagerImpl::TMP_PREFIX) && idx.is_ok() {
                    fs::remove_file(file_path.as_os_str()).await?;
                }
            }
            Ok(())
        }

        fn add_metadata_if_sector_file(
            v: &Vec<&str>,
            metadata_map: &mut HashMap<SectorIdx, (Timestamp, WriteRank)>,
        ) {
            if v.len() == 3 {
                let idx_res = v.get(0).unwrap().parse::<u64>();
                let timestamp_res = v.get(1).unwrap().parse::<u64>();
                let write_rank_res = v.get(2).unwrap().parse::<u8>();
                if idx_res.is_ok() && timestamp_res.is_ok() && write_rank_res.is_ok() {
                    let idx = idx_res.unwrap();
                    if metadata_map
                        .insert(idx, (timestamp_res.unwrap(), write_rank_res.unwrap()))
                        .is_some()
                    {
                        log::error!("Sector manager has two values for {}", idx);
                    }
                }
            }
        }

        fn check_if_value_from_failed_write(
            v: &Vec<&str>,
            file_path: &PathBuf,
            failed_writes: &mut HashMap<SectorIdx, (Timestamp, WriteRank, PathBuf)>,
        ) {
            if v.len() == 4 {
                let prefix = v.get(0).unwrap();
                let idx_res = v.get(1).unwrap().parse::<u64>();
                let timestamp_res = v.get(2).unwrap().parse::<u64>();
                let write_rank_res = v.get(3).unwrap().parse::<u8>();
                if prefix.eq(&SectorsManagerImpl::FAILED_WRITE_PREFIX)
                    && idx_res.is_ok()
                    && timestamp_res.is_ok()
                    && write_rank_res.is_ok()
                {
                    failed_writes.insert(
                        idx_res.unwrap(),
                        (
                            timestamp_res.unwrap(),
                            write_rank_res.unwrap(),
                            file_path.clone(),
                        ),
                    );
                }
            }
        }

        async fn handle_value_from_failed_write(
            sector_idx: SectorIdx,
            root_dir: &PathBuf,
            metadata_map: &mut HashMap<SectorIdx, (Timestamp, WriteRank)>,
            failed_writes: &HashMap<SectorIdx, (Timestamp, WriteRank, PathBuf)>,
        ) -> Result<(), Error> {
            let val = failed_writes.get(&sector_idx).unwrap();
            if !metadata_map.contains_key(&sector_idx) {
                // Create normal filename from failed write filename and rename the file
                tokio::fs::rename(
                    val.2.as_os_str(),
                    SectorsManagerImpl::append_to_pathbuf(
                        root_dir,
                        &SectorsManagerImpl::create_filename(&sector_idx, &(val.0, val.1)),
                    ),
                )
                .await?;

                metadata_map.insert(sector_idx, (val.0, val.1));
            } else {
                tokio::fs::remove_file(val.2.as_os_str()).await?;
            }
            Ok(())
        }

        let mut metadata_map = HashMap::new();
        let mut failed_writes = HashMap::new();
        let mut paths = tokio::fs::read_dir(&root_path).await?;

        let mut correct_filenames: Vec<String> = Vec::new();
        let mut file_paths: Vec<PathBuf> = Vec::new();
        while let Some(path_it) = paths.next_entry().await? {
            if let Some(filename) = path_it.path().file_name() {
                if let Some(string) = filename.to_str() {
                    correct_filenames.push(String::from(string));
                    file_paths.push(path_it.path());
                }
            }
        }

        for (filename, file_path) in correct_filenames.into_iter().zip(file_paths.into_iter()) {
            let split = filename.split(SectorsManagerImpl::DELIMITER);
            let v = split.collect::<Vec<&str>>();
            add_metadata_if_sector_file(&v, &mut metadata_map);
            cleanup_if_tmp_file(&v, &file_path).await?;
            check_if_value_from_failed_write(&v, &file_path, &mut failed_writes);
        }

        for key in failed_writes.keys() {
            handle_value_from_failed_write(
                key.clone(),
                &root_path,
                &mut metadata_map,
                &failed_writes,
            )
            .await?;
        }

        return Ok(SectorsManagerImpl {
            sectors_metadata: RwLock::new(metadata_map),
            root_storage_dir: root_path,
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

    fn create_failed_write_name(idx: &SectorIdx, metadata: &Metadata) -> String {
        let idx_str = idx.to_string();
        let timestamp_str = metadata.0.to_string();
        let write_rank_str = metadata.1.to_string();
        let v = vec![
            Self::FAILED_WRITE_PREFIX,
            &idx_str,
            &timestamp_str,
            &write_rank_str,
        ];
        v.join(SectorsManagerImpl::DELIMITER)
    }

    fn append_to_home_pathbuf(&self, append_str: &str) -> OsString {
        Self::append_to_pathbuf(&self.root_storage_dir, append_str)
    }

    fn append_to_pathbuf(path: &PathBuf, append_str: &str) -> OsString {
        let mut copy = path.clone();

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
            let file_path = self.append_to_home_pathbuf(&file_to_read);
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
        // No mutex - You can assume that the unit tests will not perform
        // concurrent operations on the same sector, even though the trait is marked as Sync.

        let key_path =
            self.append_to_home_pathbuf(&Self::create_filename(&idx, &(sector.1, sector.2)));
        let tmp_path = self.append_to_home_pathbuf(&Self::create_tmp_filename(idx));

        let directory = File::open(self.root_storage_dir.clone().into_os_string())
            .await
            .unwrap();

        let mut tmp_file = tokio::fs::File::create(tmp_path.clone()).await.unwrap();
        tmp_file.write_all(&(sector.0 .0)).await.unwrap();
        tmp_file.sync_data().await.unwrap();

        let should_remove_file = self.sectors_metadata.read().await.get(&idx).cloned();

        if let Some(metadata) = should_remove_file {
            let safety_name =
                self.append_to_home_pathbuf(&Self::create_failed_write_name(&idx, &metadata));
            let current_sector_name =
                self.append_to_home_pathbuf(&Self::create_filename(&idx, &metadata));

            rename(current_sector_name, safety_name.clone())
                .await
                .unwrap();
            directory.sync_data().await.unwrap();
            rename(tmp_path, key_path).await.unwrap();
            directory.sync_data().await.unwrap();

            fs::remove_file(safety_name).await.unwrap();
            directory.sync_data().await.unwrap();
        } else {
            rename(tmp_path, key_path).await.unwrap();
            directory.sync_data().await.unwrap();
        }

        let mut w = self.sectors_metadata.write().await;
        (*w).insert(idx, (sector.1, sector.2));
    }
}
