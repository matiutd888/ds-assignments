mod domain;

mod register_client_public;
mod stable_storage_public;
mod transfer_public;
mod transport;
mod atomic_register_public;

pub use crate::domain::*;
pub use atomic_register_public::*;
pub use register_client_public::*;
pub use sectors_manager_public::*;
pub use stable_storage_public::*;
pub use transfer_public::*;

pub async fn run_register_process(config: Configuration) {
    unimplemented!()
}



pub mod constants {
    pub const SECTOR_SIZE_BYTES: usize = 4096;
    pub type MsgType = u8;

    pub const TYPE_READ: u8 = 0x01;
    pub const TYPE_WRITE: u8 = 0x02;
    pub const TYPE_READ_PROC: u8 = 0x03;
    pub const TYPE_VALUE: u8 = 0x04;
    pub const TYPE_WRITE_PROC: u8 = 0x05;
    pub const TYPE_ACK: u8 = 0x06;
}

pub mod sectors_manager_public {
    use crate::{SectorIdx, SectorVec};
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
    pub fn build_sectors_manager(path: PathBuf) -> Arc<dyn SectorsManager> {
        unimplemented!()
    }
}
