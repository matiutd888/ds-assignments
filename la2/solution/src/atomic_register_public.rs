use uuid::Uuid;

use crate::{
    register_client_public, ClientCommandHeader, ClientRegisterCommand,
    ClientRegisterCommandContent, OperationSuccess, RegisterClient, SectorIdx, SectorVec,
    SectorsManager, StableStorage, SystemCommandHeader, SystemRegisterCommand,
    SystemRegisterCommandContent,
};

use std::collections::{HashMap, HashSet};

use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;

// TODO Store highest VALUE in metadata (disk) and highest metadata in memory

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

struct Algorithm {
    readlist: HashSet<u8>,
    acklist: HashSet<u8>,
    highest: Option<(Timestamp, WriteRank, SectorVec)>,
    writing: bool,
    reading: bool,
    algorithm_val: Option<SectorVec>,
    write_phase: bool,
    request_identifier: u64,
    success_callback: Box<SuccessCallback>,
}

impl Algorithm {
    fn clear_lists(&mut self) {
        self.acklist = HashSet::new();

        self.readlist = HashSet::new();
        self.highest = None;
    }

    async fn new(request_identifier: u64, success_callback: Box<SuccessCallback>) -> Algorithm {
        Algorithm {
            algorithm_val: None,
            readlist: HashSet::new(),
            acklist: HashSet::new(),
            highest: None,
            writing: false,
            reading: false,
            write_phase: false,
            request_identifier: request_identifier,
            success_callback: success_callback,
        }
    }

    // [DISC] This function needs to be changed if I decide to store highest on disk.
    fn update_highest(&mut self, timestamp: u64, write_rank: u8, sector_data: SectorVec) {
        let change: bool = if let Some((curr_timestamp, curr_wr, _)) = self.highest {
            (timestamp, write_rank) > (curr_timestamp, curr_wr)
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
    // This really should always have only one element!
    a: HashMap<SectorIdx, Algorithm>,
    metadata: Box<dyn StableStorage>,
    register_client: Arc<dyn RegisterClient>,
    sectors_manager: Arc<dyn SectorsManager>,
    processes_count: u8,
    process_identifier: u8,
}

impl AtomicRegisterImpl {
    // These methods could also be changed if we will be keeping the values in memory
    // fn get_writeval_key(sector_idx: SectorIdx) -> String {
    //     format!("{}-writeval", sector_idx)
    // }

    // fn get_readval_key(sector_idx: SectorIdx) -> String {
    //     format!("{}-readval", sector_idx)
    // }

    // fn get_val_key(sector_idx: SectorIdx) -> String {
    //     format!("{}-val", sector_idx)
    // }

    // async fn get_sector_metadata(&self, key: String) -> SectorVec {
    //     let data = if let Some(vec) = self.metadata.get(&key).await {
    //         assert!(vec.len() == constants::SECTOR_SIZE_BYTES);
    //         vec
    //     } else {
    //         vec![0; constants::SECTOR_SIZE_BYTES]
    //     };
    //     SectorVec(data)
    // }

    // async fn save_writeval(&mut self, sector_idx: SectorIdx, data: SectorVec) {
    //     self.metadata
    //         .put(&Self::get_writeval_key(sector_idx), &data.0)
    //         .await
    //         .unwrap();
    // }

    // async fn save_readval(&mut self, sector_idx: SectorIdx, data: SectorVec) {
    //     self.metadata
    //         .put(&&Self::get_readval_key(sector_idx), &data.0)
    //         .await
    //         .unwrap();
    // }

    // async fn get_readval(&self, sector_idx: SectorIdx) -> SectorVec {
    //     self.get_sector_metadata(Self::get_readval_key(sector_idx))
    //         .await
    // }

    // async fn get_writeval(&self, sector_idx: SectorIdx) -> SectorVec {
    //     self.get_sector_metadata(Self::get_writeval_key(sector_idx))
    //         .await
    // }

    async fn get_val(&self, sector_idx: SectorIdx) -> SectorVec {
        self.sectors_manager.read_data(sector_idx).await
    }

    async fn get_metadata(&self, sector_idx: SectorIdx) -> Metadata {
        self.sectors_manager.read_metadata(sector_idx).await
    }

    async fn store_val_and_metadata(
        &self,
        sector_idx: SectorIdx,
        timestamp: Timestamp,
        write_rank: u8,
        data: SectorVec,
    ) {
        self.sectors_manager
            .write(sector_idx, &(data, timestamp, write_rank))
            .await;
    }

    async fn store_rid(&mut self) {
        self.metadata
            .put("rid", &self.rid.to_be_bytes())
            .await
            .unwrap();
    }

    async fn get_stored_rid(&self) -> u64 {
        if let Some(rid_bytes) = self.metadata.get("rid").await {
            u64::from_be_bytes(rid_bytes.try_into().unwrap())
        } else {
            0
        }
    }

    async fn remove_algorithm(&mut self, sector_idx: SectorIdx) {
        self.a.remove(&sector_idx);
    }

    fn get_default_ret_header(&self, sector_idx: SectorIdx, rid: u64) -> SystemCommandHeader {
        SystemCommandHeader {
            process_identifier: self.process_identifier,
            msg_ident: Uuid::new_v4(),
            read_ident: rid,
            sector_idx,
        }
    }

    async fn broadcast_readproc(&self, sector_idx: SectorIdx) {
        let new_system_header = self.get_default_ret_header(sector_idx, self.rid);
        let broadcast_system_message = SystemRegisterCommand {
            header: new_system_header,
            content: SystemRegisterCommandContent::ReadProc,
        };

        self.register_client
            .broadcast(register_client_public::Broadcast {
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
        let new_system_header = self.get_default_ret_header(sector_idx, self.rid);
        let broadcast_system_message = SystemRegisterCommand {
            header: new_system_header,
            content: SystemRegisterCommandContent::WriteProc {
                timestamp: timestamp,
                write_rank: write_rank,
                data_to_write: sector_data,
            },
        };

        self.register_client
            .broadcast(register_client_public::Broadcast {
                cmd: Arc::new(broadcast_system_message),
            })
            .await;
    }

    async fn handle_read_client_command(
        &mut self,
        header: ClientCommandHeader,
        success_callback: Box<SuccessCallback>,
    ) {
        self.remove_algorithm(header.sector_idx).await;
        let mut new_entry = Algorithm::new(header.request_identifier, success_callback).await;
        new_entry.reading = true;

        self.a.insert(header.sector_idx, new_entry);

        // Create system message and send to other processes
        self.broadcast_readproc(header.sector_idx).await;
    }

    async fn handle_write_client_command(
        &mut self,
        header: ClientCommandHeader,
        success_callback: Box<SuccessCallback>,
        data: SectorVec,
    ) {
        self.remove_algorithm(header.sector_idx).await;
        let mut new_entry = Algorithm::new(header.request_identifier, success_callback).await;
        new_entry.writing = true;

        new_entry.algorithm_val = Some(data);

        self.a.insert(header.sector_idx, new_entry);

        // Create system message and send to other processes
        self.broadcast_readproc(header.sector_idx).await;
    }

    async fn handle_readproc(&self, header: SystemCommandHeader) {
        log::debug!("RID: {}, READPROC", header.read_ident);

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
            .send(register_client_public::Send {
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
        log::debug!("RID: {}, VALUE", header.read_ident);
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
                let (m_timestamp, m_wr) = self.get_metadata(header.sector_idx).await;
                let m_val = self.get_val(header.sector_idx).await;

                let mut algorithm_val = self.a.get_mut(&header.sector_idx).unwrap();

                algorithm_val.update_highest(m_timestamp, m_wr, m_val);

                // TODO dokończyć to - zapisać readlist, wysłać broadcast
                let mut highest: Option<(Timestamp, WriteRank, SectorVec)> = None;
                mem::swap(&mut highest, &mut algorithm_val.highest);

                algorithm_val.clear_lists();
                algorithm_val.write_phase = true;

                let highest_val = highest.unwrap();

                let (send_ts, send_wr, send_val) = if algorithm_val.writing {
                    let writeval = algorithm_val.algorithm_val.clone().unwrap();
                    let self_rank = self.process_identifier;
                    let new_timestamp = highest_val.0 + 1;

                    self.store_val_and_metadata(
                        header.sector_idx,
                        new_timestamp,
                        self_rank,
                        writeval.clone(),
                    )
                    .await;

                    (new_timestamp, self_rank, writeval)
                } else {
                    algorithm_val.algorithm_val = Some(highest_val.2);
                    (
                        highest_val.0,
                        highest_val.1,
                        algorithm_val.algorithm_val.clone().unwrap(),
                    )
                };
                self.broadcast_writeproc(header.sector_idx, send_ts, send_wr, send_val)
                    .await;
            }
        }
    }

    async fn handle_writeproc(
        &mut self,
        header: SystemCommandHeader,
        timestamp: u64,
        write_rank: u8,
        sector_data: SectorVec,
    ) {
        log::debug!("RID: {}, WRITEPROC", header.read_ident);
        let sector_idx = header.sector_idx;
        let (ts, ws) = self.get_metadata(sector_idx).await;

        if (timestamp, write_rank) > (ts, ws) {
            self.store_val_and_metadata(sector_idx, timestamp, write_rank, sector_data)
                .await;
        }

        let new_cmd = SystemRegisterCommand {
            header: self.get_default_ret_header(sector_idx, header.read_ident),
            content: SystemRegisterCommandContent::Ack,
        };

        self.register_client
            .send(register_client_public::Send {
                cmd: Arc::new(new_cmd),
                target: header.process_identifier,
            })
            .await;
    }

    async fn handle_ack(&mut self, header: SystemCommandHeader) {
        log::debug!("RID: {}, ACK", header.read_ident);
        if header.read_ident == self.rid {
            let sector_idx = header.sector_idx;

            let algorithm = self.a.get_mut(&sector_idx).unwrap();
            if !algorithm.write_phase {
                return ();
            }

            algorithm.acklist.insert(header.process_identifier);
            log::debug!("ACK: {}", algorithm.acklist.len());
            if algorithm.acklist.len() as u8 > self.processes_count / 2
                && (algorithm.reading || algorithm.writing)
            {
                log::debug!("Operation will finish");
                let mut algorithm_removed = self.a.remove(&sector_idx).unwrap();

                let op_return = if algorithm_removed.reading {
                    let mut readval: Option<SectorVec> = None;
                    std::mem::swap(&mut readval, &mut algorithm_removed.algorithm_val);

                    crate::OperationReturn::Read(crate::ReadReturn {
                        read_data: readval.unwrap(),
                    })
                } else {
                    crate::OperationReturn::Write
                };

                self.remove_algorithm(sector_idx).await;

                let op_succ = OperationSuccess {
                    request_identifier: algorithm_removed.request_identifier,
                    op_return: op_return,
                };

                (algorithm_removed.success_callback)(op_succ).await;
                log::debug!("success callback!");
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
        log::debug!(
            "I am register {} and I just received command {}",
            self.process_identifier,
            cmd.header.request_identifier
        );
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
        log::debug!(
            "I am register {} and I just received System command from {} with rid {}",
            self.process_identifier,
            cmd.header.process_identifier,
            cmd.header.read_ident
        );

        match cmd.content {
            SystemRegisterCommandContent::ReadProc => self.handle_readproc(cmd.header).await,
            SystemRegisterCommandContent::Value {
                timestamp,
                write_rank,
                sector_data,
            } => {
                self.handle_value(cmd.header, timestamp, write_rank, sector_data)
                    .await
            }
            SystemRegisterCommandContent::WriteProc {
                timestamp,
                write_rank,
                data_to_write,
            } => {
                self.handle_writeproc(cmd.header, timestamp, write_rank, data_to_write)
                    .await
            }
            SystemRegisterCommandContent::Ack => self.handle_ack(cmd.header).await,
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
    let mut atomic_register = AtomicRegisterImpl {
        rid: 0,
        a: HashMap::new(),
        metadata,
        register_client,
        sectors_manager,
        processes_count,
        process_identifier: self_ident,
    };
    atomic_register.rid = atomic_register.get_stored_rid().await;
    Box::new(atomic_register)
}
