use hmac::digest::typenum::True;
use serde::de::IntoDeserializer;
use uuid::Uuid;

use crate::constants::{self, SECTOR_SIZE_BYTES};
use crate::{
    ClientCommandHeader, ClientRegisterCommand, ClientRegisterCommandContent, OperationSuccess,
    RegisterClient, SectorIdx, SectorVec, SectorsManager, StableStorage, SystemCommandHeader,
    SystemRegisterCommand, SystemRegisterCommandContent,
};
use core::time;
use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::{mem, vec};

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
        success_callback: Box<SuccessCallback>,
    );

    /// Handle a system command.
    ///
    /// This function corresponds to the handlers of READ_PROC, VALUE, WRITE_PROC
    /// and ACK messages in the (N,N)-AtomicRegister algorithm.
    async fn system_command(&mut self, cmd: SystemRegisterCommand);
}

type Timestamp = u64;
type WriteRank = u8;

type Metadata = (Timestamp, WriteRank);

type SuccessCallback =
    dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync;

#[derive(Clone)]
struct Algorithm {
    readlist: HashSet<u8>,
    acklist: HashSet<u8>,
    highest: Option<(Timestamp, WriteRank, SectorVec)>,
    writing: bool,
    reading: bool,
    write_phase: bool,
}

impl Algorithm {
    fn clear_lists(&mut self) {
        self.acklist = HashSet::new();

        self.readlist = HashSet::new();
        self.highest = None;
    }

    async fn default(
        sectors_idx: SectorIdx,
        sectors_manager: Arc<dyn SectorsManager>,
    ) -> Algorithm {
        let val = sectors_manager.read_data(sectors_idx).await;
        let metadata = sectors_manager.read_metadata(sectors_idx).await;
        Algorithm {
            readlist: HashSet::new(),
            acklist: HashSet::new(),
            highest: None,
            writing: false,
            reading: false,
            write_phase: false,
        }
    }

    fn update_highest(&mut self, timestamp: u64, write_rank: u8, sector_data: SectorVec) {
        let change: bool = if let Some((curr_timestamp, curr_wr, curr_data)) = self.highest {
            match timestamp.cmp(&curr_timestamp) {
                std::cmp::Ordering::Less => false,
                std::cmp::Ordering::Equal => curr_wr < write_rank,
                std::cmp::Ordering::Greater => true,
            }
        } else {
            true
        };

        if change {
            self.highest = Some((timestamp, write_rank, sector_data));
        }
    }
}

struct AtomicRegisterImpl {
    rid: u64,
    a: HashMap<SectorIdx, Algorithm>,
    metadata: Box<dyn StableStorage>,
    register_client: Arc<dyn RegisterClient>,
    sectors_manager: Arc<dyn SectorsManager>,
    processes_count: u8,
    process_identifier: u8,
    success_callbacks: HashMap<SectorIdx, Box<SuccessCallback>>,
    request_identifiers: HashMap<SectorIdx, u64>,
}

impl AtomicRegisterImpl {
    // These methods could also be changed if we will be keeping the values in memory
    fn get_writeval_key(sector_idx: SectorIdx) -> String {
        format!("{}-writeval", sector_idx)
    }

    fn get_readval_key(sector_idx: SectorIdx) -> String {
        format!("{}-readval", sector_idx)
    }

    fn get_val_key(sector_idx: SectorIdx) -> String {
        format!("{}-val", sector_idx)
    }

    async fn get_sector_metadata(&self, key: String) -> SectorVec {
        let data = if let Some(vec) = self.metadata.get(&key).await {
            assert!(vec.len() == constants::SECTOR_SIZE_BYTES);
            vec
        } else {
            vec![0; constants::SECTOR_SIZE_BYTES]
        };
        SectorVec(data)
    }

    async fn get_val(&self, sector_idx: SectorIdx) -> SectorVec {
        self.sectors_manager.read_data(sector_idx).await
    }

    async fn get_metadata(&self, sector_idx: SectorIdx) -> Metadata {
        self.sectors_manager.read_metadata(sector_idx).await
    }

    async fn get_readval(&self, sector_idx: SectorIdx) -> SectorVec {
        self.get_sector_metadata(Self::get_readval_key(sector_idx))
            .await
    }

    async fn get_writeval(&self, sector_idx: SectorIdx) -> SectorVec {
        self.get_sector_metadata(Self::get_writeval_key(sector_idx))
            .await
    }

    async fn store_rid(&mut self) {
        self.metadata
            .put("rid", &self.rid.to_be_bytes())
            .await
            .unwrap();
    }

    async fn save_writeval(&mut self, sector_idx: SectorIdx, data: SectorVec) {
        self.metadata
            .put(&Self::get_writeval_key(sector_idx), &data.0)
            .await
            .unwrap();
    }

    async fn save_readval(&mut self, sector_idx: SectorIdx, data: SectorVec) {
        self.metadata
            .put(&&Self::get_readval_key(sector_idx), &data.0)
            .await
            .unwrap();
    }

    async fn get_algorithm_or_default(&mut self, sector_idx: SectorIdx) -> Algorithm {
        if let Some(algorithm) = self.a.remove(&sector_idx) {
            algorithm // We need to use the old value so we don't have to read from sectors manager each time :)
        } else {
            Algorithm::default(sector_idx, self.sectors_manager.clone()).await
        }
    }

    async fn broadcast_readproc(&self, sector_idx: SectorIdx) {
        let new_system_header = SystemCommandHeader {
            process_identifier: self.process_identifier,
            msg_ident: Uuid::new_v4(),
            read_ident: self.rid,
            sector_idx: sector_idx,
        };
        let broadcast_system_message = SystemRegisterCommand {
            header: new_system_header,
            content: SystemRegisterCommandContent::ReadProc,
        };

        self.register_client
            .broadcast(crate::Broadcast {
                cmd: Arc::new(broadcast_system_message),
            })
            .await;
    }

    async fn broadcast_writeproc(
        &self,
        sector_idx: SectorIdx,
        timestamp: Timestamp,
        write_rank: u8,
        sector_data: SectorVec,
    ) {
        let new_system_header = SystemCommandHeader {
            process_identifier: self.process_identifier,
            msg_ident: Uuid::new_v4(),
            read_ident: self.rid,
            sector_idx: sector_idx,
        };
        let broadcast_system_message = SystemRegisterCommand {
            header: new_system_header,
            content: SystemRegisterCommandContent::WriteProc {
                timestamp: timestamp,
                write_rank: write_rank,
                data_to_write: sector_data,
            },
        };

        self.register_client
            .broadcast(crate::Broadcast {
                cmd: Arc::new(broadcast_system_message),
            })
            .await;
    }

    async fn create_entry_for_read(&mut self, sector_idx: SectorIdx) -> Algorithm {
        let mut new_algorithm_entry = self.get_algorithm_or_default(sector_idx).await;

        new_algorithm_entry.clear_lists();

        new_algorithm_entry.writing = false;
        new_algorithm_entry.reading = true;

        new_algorithm_entry
    }

    async fn handle_read_client_command(
        &mut self,
        header: ClientCommandHeader,
        success_callback: Box<SuccessCallback>,
    ) {
        let new_entry = self.create_entry_for_read(header.sector_idx).await;
        self.a.insert(header.sector_idx, new_entry);

        self.request_identifiers
            .insert(header.sector_idx, header.request_identifier);
        self.success_callbacks
            .insert(header.sector_idx, success_callback);

        // Create system message and send to other processes
        self.broadcast_readproc(header.sector_idx).await;
    }

    async fn create_entry_for_write(&mut self, sector_idx: SectorIdx) -> Algorithm {
        let mut new_algorithm_entry = self.get_algorithm_or_default(sector_idx).await;

        new_algorithm_entry.clear_lists();

        new_algorithm_entry.reading = false;
        new_algorithm_entry.writing = true;

        new_algorithm_entry
    }

    async fn handle_write_client_command(
        &mut self,
        header: ClientCommandHeader,
        success_callback: Box<SuccessCallback>,
        data: SectorVec,
    ) {
        let new_entry = self.create_entry_for_write(header.sector_idx).await;
        self.request_identifiers
            .insert(header.sector_idx, header.request_identifier);
        self.success_callbacks
            .insert(header.sector_idx, success_callback);

        self.a.insert(header.sector_idx, new_entry);

        self.save_writeval(header.sector_idx, data).await;
        // Create system message and send to other processes
        self.broadcast_readproc(header.sector_idx).await;
    }

    async fn handle_readproc(&self, header: SystemCommandHeader) {
        let sv = self.get_val(header.sector_idx).await;
        let metadata = self.get_metadata(header.sector_idx).await;

        let new_system_header = SystemCommandHeader {
            process_identifier: self.process_identifier,
            msg_ident: Uuid::new_v4(),
            read_ident: header.read_ident,
            sector_idx: header.sector_idx,
        };

        let new_cmd = SystemRegisterCommand {
            header: new_system_header,
            content: SystemRegisterCommandContent::Value {
                timestamp: metadata.0,
                write_rank: metadata.1,
                sector_data: self.get_val(header.sector_idx).await,
            },
        };

        self.register_client
            .send(crate::Send {
                cmd: Arc::new(new_cmd),
                target: header.process_identifier,
            })
            .await;
    }

    async fn handle_value(
        &mut self,
        header: SystemCommandHeader,
        timestamp: u64,
        write_rank: u8,
        sector_data: SectorVec,
    ) {
        if header.read_ident == self.rid {
            let algorithm = self.a.get_mut(&header.sector_idx).unwrap();
            if algorithm.write_phase {
                return ();
            }

            algorithm.readlist.insert(header.process_identifier);
            algorithm.update_highest(timestamp, write_rank, sector_data);

            if algorithm.readlist.len() as u8 > (self.processes_count / 2)
                && (algorithm.reading || algorithm.writing)
            {
                // get_metadata
                let (m_timestamp, m_wr) =
                    self.sectors_manager.read_metadata(header.sector_idx).await;
                // get_data
                let m_val = self.sectors_manager.read_data(header.sector_idx).await;

                algorithm.update_highest(m_timestamp, m_wr, m_val);

                // TODO dokończyć to - zapisać readlist, wysłać broadcast
                let mut highest: Option<(Timestamp, WriteRank, SectorVec)> = None;
                mem::swap(&mut highest, &mut algorithm.highest);

                algorithm.clear_lists();
                algorithm.write_phase = true;

                (w_ts, w_wr, w_val) = if algorithm.writing {

                }
                self.broadcast_writeproc(sector_idx, timestamp, write_rank, sector_data)
            }
        }
    }
}

#[async_trait::async_trait]
impl AtomicRegister for AtomicRegisterImpl {
    async fn client_command(
        &mut self,
        cmd: ClientRegisterCommand,

        // Success callback should be equal to
        // sending the response to client
        success_callback: Box<SuccessCallback>,
    ) {
        self.rid = self.rid + 1;
        self.store_rid().await;
        match cmd.content {
            ClientRegisterCommandContent::Read => {
                self.handle_read_client_command(cmd.header, success_callback)
                    .await
            }
            ClientRegisterCommandContent::Write { data } => {
                self.handle_write_client_command(cmd.header, success_callback, data)
                    .await
            }
        }
    }

    /// Handle a system command.
    ///
    /// This function corresponds to the handlers of READ_PROC, VALUE, WRITE_PROC
    /// and ACK messages in the (N,N)-AtomicRegister algorithm.
    async fn system_command(&mut self, cmd: SystemRegisterCommand) {
        match cmd.content {
            SystemRegisterCommandContent::ReadProc => todo!(),
            SystemRegisterCommandContent::Value {
                timestamp,
                write_rank,
                sector_data,
            } => todo!(),
            SystemRegisterCommandContent::WriteProc {
                timestamp,
                write_rank,
                data_to_write,
            } => todo!(),
            SystemRegisterCommandContent::Ack => todo!(),
        }
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
    todo!()
    // let a: Algorithm = Algorithm {
    //     rid: 0,
    //     readlist: HashMap<Sector>,
    //     acklist: vec![false; processes_count as usize],
    //     reading: false,
    //     writing: false,
    //     readval_key: "readval_key",
    //     writeval_key: "writeval_key",
    //     write_phase: false,
    // };

    // metadata.put(key, value)
    // Box::new(AtomicRegisterImpl {
    //     a,
    //     metadata,
    //     register_client,
    //     sectors_manager,
    //     processes_count,
    // })
}
