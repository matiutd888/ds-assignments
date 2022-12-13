pub mod atomic_register_public {
    use crate::constants::SECTOR_SIZE_BYTES;
    use crate::{
        ClientRegisterCommand, OperationSuccess, RegisterClient, SectorVec, SectorsManager,
        StableStorage, SystemRegisterCommand, SectorIdx,
    };
    use std::collections::{HashMap, HashSet};
    use std::fmt::Write;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::vec;

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

            // Success callback should be equal to
            // sending the response to client
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

    type Timestamp = u64;
    type WriteRank = u8;

    const NO_VAL: Option<(Timestamp, WriteRank, SectorVec)> = None;

    struct Algorithm {
        rid: u32,
        sectors: Vec<SectorIdx>, 
        readlist: HashSet<SectorIdx>,
        acklist: HashSet<SectorIdx>,
        highest: Option<(Timestamp, WriteRank, SectorVec)>,
        reading: bool,
        writing: bool,
        readval: SectorVec,
        writeval: SectorVec,
        write_phase: bool
    }

    struct AtomicRegisterImpl {
        a: Algorithm,
        metadata: Box<dyn StableStorage>,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        processes_count: u8,
    }

    impl AtomicRegister for AtomicRegisterImpl {
        fn client_command<'life0, 'async_trait>(
            &'life0 mut self,
            cmd: ClientRegisterCommand,
            success_callback: Box<
                dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
                    + Sync,
            >,
        ) -> core::pin::Pin<
            Box<dyn core::future::Future<Output = ()> + core::marker::Send + 'async_trait>,
        >
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            todo!()
        }

        fn system_command<'life0, 'async_trait>(
            &'life0 mut self,
            cmd: SystemRegisterCommand,
        ) -> core::pin::Pin<
            Box<dyn core::future::Future<Output = ()> + core::marker::Send + 'async_trait>,
        >
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            todo!()
        }
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
        // It should initialize stable storage
        
        // Here it should recover metadata from stable storage

        let a: Algorithm = Algorithm {
            rid: 0,
            readlist: HashMap<Sector>,
            acklist: vec![false; processes_count as usize],
            reading: false,
            writing: false,
            readval_key: "readval_key",
            writeval_key: "writeval_key",
            write_phase: false,
        };

        metadata.put(key, value)
        Box::new(AtomicRegisterImpl {
            a,
            metadata,
            register_client,
            sectors_manager,
            processes_count,
        })
    }
}
