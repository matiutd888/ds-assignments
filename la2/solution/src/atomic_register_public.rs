use uuid::Uuid;

use crate::{
    register_client_public, ClientCommandHeader, ClientRegisterCommand,
    ClientRegisterCommandContent, Metadata, OperationSuccess, RegisterClient, SectorIdx, SectorVec,
    SectorsManager, StableStorage, SystemCommandHeader, SystemRegisterCommand,
    SystemRegisterCommandContent, Timestamp, WriteRank,
};

use std::collections::HashSet;

use std::future::Future;
use std::mem;
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
    name: String,
    rid: u64,
    a: Option<Algorithm>,
    metadata: Box<dyn StableStorage>,
    register_client: Arc<dyn RegisterClient>,
    sectors_manager: Arc<dyn SectorsManager>,
    processes_count: u8,
    process_identifier: u8,
}

impl AtomicRegisterImpl {
    const RID_KEY: &str = "rid";

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
            .put(Self::RID_KEY, &self.rid.to_be_bytes())
            .await
            .unwrap();
    }

    async fn get_stored_rid(&self) -> u64 {
        if let Some(rid_bytes) = self.metadata.get(Self::RID_KEY).await {
            u64::from_be_bytes(rid_bytes.try_into().unwrap())
        } else {
            0
        }
    }

    fn warn_if_algorithm_not_none(&mut self, _sector_idx: SectorIdx) {
        if let Some(_) = self.a {
            log::warn!(
                "{}, {} overriding algorithm value!",
                self.process_identifier,
                self.name,
            );
        }
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
        log::debug!(
            "{}, {}: NEW RID({}) READ CLIENT {}",
            self.process_identifier,
            self.name,
            self.rid,
            header.sector_idx
        );
        self.warn_if_algorithm_not_none(header.sector_idx);
        let mut new_entry = Algorithm::new(header.request_identifier, success_callback).await;
        new_entry.reading = true;

        self.a = Some(new_entry);

        // Create system message and send to other processes
        self.broadcast_readproc(header.sector_idx).await;
    }

    async fn handle_write_client_command(
        &mut self,
        header: ClientCommandHeader,
        success_callback: Box<SuccessCallback>,
        data: SectorVec,
    ) {
        log::debug!(
            "{}, {}: NEW RID({}) WRITE CLIENT {}",
            self.process_identifier,
            self.name,
            self.rid,
            header.sector_idx
        );
        self.warn_if_algorithm_not_none(header.sector_idx);
        let mut new_entry = Algorithm::new(header.request_identifier, success_callback).await;
        new_entry.writing = true;

        new_entry.algorithm_val = Some(data);

        self.a = Some(new_entry);

        // Create system message and send to other processes
        self.broadcast_readproc(header.sector_idx).await;
    }

    async fn handle_readproc(&self, header: SystemCommandHeader) {
        log::debug!(
            "{}, {}, rid: {}, READPROC",
            self.process_identifier,
            self.name,
            header.read_ident
        );

        let metadata = self.sectors_manager.read_metadata(header.sector_idx).await;

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
        log::debug!(
            "{}, {}: rid {}, VALUE",
            self.process_identifier,
            self.name,
            header.read_ident
        );
        if header.read_ident == self.rid {
            if let Some(algorithm) = &mut self.a {
                if algorithm.write_phase {
                    return ();
                }

                algorithm.readlist.insert(header.process_identifier);
                algorithm.update_highest(timestamp, write_rank, sector_data);

                if algorithm.readlist.len() as u8 > (self.processes_count / 2)
                    && (algorithm.reading || algorithm.writing)
                {
                    let (m_timestamp, m_wr) =
                        self.sectors_manager.read_metadata(header.sector_idx).await;
                    let m_val = self.sectors_manager.read_data(header.sector_idx).await;

                    algorithm.update_highest(m_timestamp, m_wr, m_val);

                    let mut highest: Option<(Timestamp, WriteRank, SectorVec)> = None;
                    mem::swap(&mut highest, &mut algorithm.highest);

                    algorithm.clear_lists();
                    algorithm.write_phase = true;

                    let highest_val = highest.unwrap();

                    let (send_ts, send_wr, send_val) = if algorithm.writing {
                        let writeval = algorithm.algorithm_val.clone().unwrap();
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
                        algorithm.algorithm_val = Some(highest_val.2);
                        (
                            highest_val.0,
                            highest_val.1,
                            algorithm.algorithm_val.clone().unwrap(),
                        )
                    };
                    self.broadcast_writeproc(header.sector_idx, send_ts, send_wr, send_val)
                        .await;
                }
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
        log::debug!(
            "{}, {}: rid {}, WRITEPROC",
            self.process_identifier,
            self.name,
            header.read_ident
        );
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
        log::debug!(
            "{}, {}: rid {}, ACK",
            self.process_identifier,
            self.name,
            header.read_ident,
        );
        if header.read_ident == self.rid {
            if let Some(algorithm) = &mut self.a {
                if !algorithm.write_phase {
                    return ();
                }

                algorithm.acklist.insert(header.process_identifier);
                log::debug!(
                    "{}, {}: rid {}, number of acks: {}",
                    self.process_identifier,
                    self.name,
                    self.rid,
                    algorithm.acklist.len()
                );
                if algorithm.acklist.len() as u8 > self.processes_count / 2
                    && (algorithm.reading || algorithm.writing)
                {
                    log::debug!(
                        "{}, {}: rid {}, Operation will finish",
                        self.process_identifier,
                        self.name,
                        self.rid,
                    );

                    let mut algorithm_removed_opt: Option<Algorithm> = None;
                    mem::swap(&mut algorithm_removed_opt, &mut self.a);
                    let mut algorithm_removed = algorithm_removed_opt.unwrap();

                    let op_return = if algorithm_removed.reading {
                        let mut readval: Option<SectorVec> = None;
                        std::mem::swap(&mut readval, &mut algorithm_removed.algorithm_val);

                        crate::OperationReturn::Read(crate::ReadReturn {
                            read_data: readval.unwrap(),
                        })
                    } else {
                        crate::OperationReturn::Write
                    };

                    let op_succ = OperationSuccess {
                        request_identifier: algorithm_removed.request_identifier,
                        op_return: op_return,
                    };
                    (algorithm_removed.success_callback)(op_succ).await;
                    self.a = None;
                }
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

pub async fn build_atomic_register(
    self_ident: u8,
    metadata: Box<dyn StableStorage>,
    register_client: Arc<dyn RegisterClient>,
    sectors_manager: Arc<dyn SectorsManager>,
    processes_count: u8,
) -> Box<dyn AtomicRegister> {
    my_build_atomic_register(
        "".to_string(),
        self_ident,
        metadata,
        register_client,
        sectors_manager,
        processes_count,
    )
    .await
}

/// Idents are numbered starting at 1 (up to the number of processes in the system).
/// Storage for atomic register algorithm data is separated into StableStorage.
/// Communication with other processes of the system is to be done by register_client.
/// And sectors must be stored in the sectors_manager instance.
///
/// This function corresponds to the handlers of Init and Recovery events in the
/// (N,N)-AtomicRegister algorithm.
pub async fn my_build_atomic_register(
    name: String,
    self_ident: u8,
    metadata: Box<dyn StableStorage>,
    register_client: Arc<dyn RegisterClient>,
    sectors_manager: Arc<dyn SectorsManager>,
    processes_count: u8,
) -> Box<dyn AtomicRegister> {
    let mut atomic_register = AtomicRegisterImpl {
        name: name,
        rid: 0,
        a: None,
        metadata,
        register_client,
        sectors_manager,
        processes_count,
        process_identifier: self_ident,
    };
    atomic_register.rid = atomic_register.get_stored_rid().await;
    Box::new(atomic_register)
}
