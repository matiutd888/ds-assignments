mod domain;

mod register_client_public;
mod stable_storage_public;
mod transfer_public;

pub use crate::domain::*;
pub use atomic_register_public::*;
pub use register_client_public::*;
pub use sectors_manager_public::*;
pub use stable_storage_public::*;
pub use transfer_public::*;

pub async fn run_register_process(config: Configuration) {
    unimplemented!()
}

pub mod atomic_register_public {
    use crate::{
        ClientRegisterCommand, OperationSuccess, RegisterClient, SectorsManager, StableStorage,
        SystemRegisterCommand,
    };
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    #[async_trait::async_trait]
    pub trait AtomicRegister: Send + Sync {
        /// Handle a client command. After the command is completed, we expect
        /// callback to be called. Note that completion of client command happens after
        /// delivery of multiple system commands to the register, as the algorithm specifies.
        ///
        /// This function corresponds to the handlers of Read and Write events in the
        /// (N,N)-AtomicRegister algorithm.
        async fn client_command(
            &mut self,
            cmd: ClientRegisterCommand,
            success_callback: Box<
                dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
                    + Sync,
            >,
        );

        /// Handle a system command.
        ///
        /// This function corresponds to the handlers of READ_PROC, VALUE, WRITE_PROC
        /// and ACK messages in the (N,N)-AtomicRegister algorithm.
        async fn system_command(&mut self, cmd: SystemRegisterCommand);
    }

    /// Idents are numbered starting at 1 (up to the number of processes in the system).
    /// Storage for atomic register algorithm data is separated into StableStorage.
    /// Communication with other processes of the system is to be done by register_client.
    /// And sectors must be stored in the sectors_manager instance.
    ///
    /// This function corresponds to the handlers of Init and Recovery events in the
    /// (N,N)-AtomicRegister algorithm.
    pub async fn build_atomic_register(
        self_ident: u8,
        metadata: Box<dyn StableStorage>,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        processes_count: u8,
    ) -> Box<dyn AtomicRegister> {
        // It should build stable storage
        unimplemented!()
    }
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

        /// Writes a new data, along with timestamp and write rank to some sector.
        async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8));
    }

    /// Path parameter points to a directory to which this method has exclusive access.
    pub fn build_sectors_manager(path: PathBuf) -> Arc<dyn SectorsManager> {
        unimplemented!()
    }
}
