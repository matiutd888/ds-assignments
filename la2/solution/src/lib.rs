mod domain;

mod atomic_register_public;
mod register_client_public;
mod reversible_stable_storage_public;
mod sectors_manager_public;
mod stable_storage_public;
mod transfer_public;

use std::{
    path::PathBuf,
    sync::Arc, collections::HashSet,
};

pub use crate::domain::*;
pub use atomic_register_public::*;
pub use register_client_public::*;
pub use sectors_manager_public::*;
pub use stable_storage_public::*;
use tokio::{
    net::{tcp::OwnedWriteHalf, TcpListener, TcpStream},
    select,
    sync::mpsc::{channel, Receiver, Sender},
};
pub use transfer_public::*;
use uuid::Uuid;

use core::marker::Send as MarkerSend;

#[async_trait::async_trait]
pub trait MySender<T>: MarkerSend + Sync {
    async fn send(&self, s: T);
}

// Każde połączenie to osobny async task
// TODO Muszę pomyśleć, w jaki sposób powinienem przekazywać
// wiadomość o zakończeniu operacji odpowiedniemu taskowi
struct TcpReader {
    socket: TcpListener,
    hmac_system_key: [u8; 64],
    hmac_client_key: [u8; 32],
}

impl TcpReader {
    pub async fn new(
        tcp_address: &(String, u16),
        hmac_system_key: [u8; 64],
        hmac_client_key: [u8; 32],
    ) -> TcpReader {
        let s = Self::bind(tcp_address).await;
        TcpReader {
            hmac_client_key: hmac_client_key,
            hmac_system_key: hmac_system_key,
            socket: s,
        }
    }

    // Open connection as FAST as possible
    async fn bind(tcp_address: &(String, u16)) -> TcpListener {
        TcpListener::bind(tcp_address).await.unwrap()
    }

    async fn start(
        reader: TcpReader,
        atomic_register_handler: Arc<AtomicRegisterCommandsDisposer>,
    ) {
        // TODO think about not using arcs.
        let hmac_client_key_arc = Arc::new(reader.hmac_client_key);
        let hmac_system_key_arc = Arc::new(reader.hmac_system_key);

        _ = tokio::spawn(async move {
            loop {
                let accept_res = reader.socket.accept().await;
                match accept_res {
                    Ok((stream, _)) => {
                        Self::spawn_connection_task(
                            stream,
                            atomic_register_handler.clone(),
                            hmac_client_key_arc.clone(),
                            hmac_system_key_arc.clone(),
                        )
                        .await
                    }
                    Err(err) => log::info!("{:?}", err),
                }
            }
        })
        .await;
    }

    async fn send_op_success(_stream: &mut OwnedWriteHalf, _op_success: OperationSuccess) {
        todo!();
    }

    async fn spawn_connection_task(
        stream: TcpStream,
        command_disposer: Arc<AtomicRegisterCommandsDisposer>,
        hmac_client_key: Arc<[u8; 32]>,
        hmac_system_key: Arc<[u8; 64]>,
    ) {
        let (s_op_success, mut r_op_success) = channel::<OperationSuccess>(2137);
        let (mut reader, mut writer) = stream.into_split();

        let _ = tokio::spawn(async move {
            loop {
                if let Some(op) = r_op_success.recv().await {
                    Self::send_op_success(&mut writer, op).await;
                }
            }
        })
        .await;
        let _ = tokio::spawn(async move {
            loop {
                let res = deserialize_register_command(
                    &mut reader,
                    hmac_system_key.as_ref(),
                    hmac_client_key.as_ref(),
                )
                .await;

                if let Ok((cmd, b)) = res {
                    if b {
                        let atomic_task_command = match cmd {
                            RegisterCommand::Client(c) => {
                                AtomicRegisterTaskCommand::ClientCommand((c, s_op_success.clone()))
                            },
                            RegisterCommand::System(s) => {
                                AtomicRegisterTaskCommand::SystemCommand(s)
                            }
                        };
                        command_disposer.send_dispose(atomic_task_command).await;
                    } else {
                       todo!()
                    }

                    
                } else {
                    log::debug!("Error while deserialize");
                }
            }
        })
        .await;
    }
}

fn get_sectors_manager_pathbuf(mut original_pathbuf: PathBuf) -> PathBuf {
    original_pathbuf.push("sectors-manager");
    original_pathbuf
}

fn get_atomic_register_metadata_pathbuf(
    mut original_pathbuf: PathBuf,
    register_index: usize,
) -> PathBuf {
    original_pathbuf.push(format!("atomic-register-{}", register_index));
    original_pathbuf
}

// Task dla każdego atomic register czytający z kolejek
// Task czytający z tcp clienta i wysyłający odpowiednie rzeczy
pub async fn run_register_process(config: Configuration) {
    let self_rank = config.public.self_rank;
    let self_rank_index = (self_rank - 1) as usize;
    let reader: TcpReader = TcpReader::new(
        config.public.tcp_locations.get(self_rank_index).unwrap(),
        config.hmac_system_key,
        config.hmac_client_key,
    )
    .await;

    let processes_count = config.public.tcp_locations.len();

    let sectors_manager = build_sectors_manager(get_sectors_manager_pathbuf(
        config.public.storage_dir.clone(),
    ));

    let atomic_handler = AtomicHandler::create_atomic_handler(config.public.n_sectors);

    let command_disposer = atomic_handler.disposer.clone();
    let register_client = Arc::new(
        RegisterClientImpl::new(
            self_rank,
            processes_count,
            config.public.tcp_locations,
            config.hmac_system_key,
            command_disposer.clone(),
        )
        .await,
    );
    AtomicHandler::spawn_tasks(
        atomic_handler,
        sectors_manager,
        self_rank,
        register_client,
        config.public.storage_dir,
        processes_count as u8,
    )
    .await;
    TcpReader::start(reader, command_disposer.clone()).await;
}

#[derive(Clone)]
struct AtomicRegisterCommandsDisposer {
    client_senders: Vec<Sender<ClientAtomicRegisterTaskCommand>>,
    system_senders: Vec<Sender<SystemAtomicRegisterTaskCommand>>,
    n_atomic_registers: usize,
    _n_sectors: u64,
}

struct AtomicHandler {
    disposer: Arc<AtomicRegisterCommandsDisposer>,
    client_receivers: Vec<Receiver<ClientAtomicRegisterTaskCommand>>,
    system_receivers: Vec<Receiver<SystemAtomicRegisterTaskCommand>>,
}

impl AtomicHandler {
    const SYSTEM_CHANNEL_SIZE: usize = 2137;
    
    // TODO think about making it 1.
    // Since one atomic register can execute only one operation at a time (for a given sector), 
    // the operations shall be queued. We suggest using a TCP buffer itself as the queue
    const CLIENT_CHANNEL_SIZE: usize = 4;

    fn create_atomic_handler(n_sectors: u64) -> AtomicHandler {
        let n_atomic_registers = constants::N_ATOMIC_REGISTERS as usize;

        let mut client_senders: Vec<Sender<ClientAtomicRegisterTaskCommand>> =
            Vec::with_capacity(n_atomic_registers);
        let mut system_senders: Vec<Sender<SystemAtomicRegisterTaskCommand>> =
            Vec::with_capacity(n_atomic_registers);

        let mut client_receivers: Vec<Receiver<ClientAtomicRegisterTaskCommand>> =
            Vec::with_capacity(n_atomic_registers);
        let mut system_receivers: Vec<Receiver<SystemAtomicRegisterTaskCommand>> =
            Vec::with_capacity(n_atomic_registers);

        for _ in 0..n_atomic_registers {
            let (s_s, r_s) = channel::<SystemAtomicRegisterTaskCommand>(Self::SYSTEM_CHANNEL_SIZE);
            let (s_c, r_c) = channel::<ClientAtomicRegisterTaskCommand>(Self::CLIENT_CHANNEL_SIZE);
            client_senders.push(s_c);
            system_senders.push(s_s);

            client_receivers.push(r_c);
            system_receivers.push(r_s);
        }

        let command_disposer = AtomicRegisterCommandsDisposer {
            system_senders: system_senders,
            client_senders: client_senders,
            n_atomic_registers: n_atomic_registers,
            _n_sectors: n_sectors,
        };

        AtomicHandler {
            disposer: Arc::new(command_disposer),
            client_receivers: client_receivers,
            system_receivers: system_receivers,
        }
    }

    async fn spawn_atomic_register_task(
        register_index: usize,
        original_pathbuf: PathBuf,
        mut r_s: Receiver<SystemAtomicRegisterTaskCommand>,
        mut r_c: Receiver<ClientAtomicRegisterTaskCommand>,
        register_client: Arc<RegisterClientImpl>,
        processes_count: u8,
        sectors_manager: Arc<dyn SectorsManager>,
        self_rank: u8,
    ) {
        let metadata = build_stable_storage(get_atomic_register_metadata_pathbuf(
            original_pathbuf,
            register_index,
        ))
        .await;
        let mut a = build_atomic_register(
            self_rank,
            metadata,
            register_client.clone(),
            sectors_manager,
            processes_count,
        )
        .await;

        let _ = tokio::spawn(async move {
            let (s, mut r_finished) = channel::<OperationSuccess>(1);
            let s_arc = Arc::new(s);
            let mut current_operation_data: Option<(u64, Sender<OperationSuccess>)> = None;
            let mut messages_during_current_request: HashSet<Uuid> = HashSet::new();
            loop {
                if let Some((current_sector, success_sender)) = &current_operation_data {
                    select! {
                        Some(cmd) = r_s.recv() => {                        
                            if !messages_during_current_request.contains(&cmd.header.msg_ident) {
                                messages_during_current_request.insert(cmd.header.msg_ident.clone());
                                a.system_command(cmd).await;
                            }
                        },
                        Some(op) = r_finished.recv() => {
                            register_client.cancel_broadcast(current_sector.clone()).await;
                            let _ = success_sender.send(op).await;
                            current_operation_data = None;
                        }
                    }
                } else {
                    if let Some((cmd, sender)) = r_c.recv().await {
                        current_operation_data = Some((cmd.header.sector_idx, sender));
                        messages_during_current_request.clear();
                        let s_cloned = s_arc.clone();
                        a.client_command(
                            cmd,
                            Box::new(|op: OperationSuccess| {
                                Box::pin(async move {
                                    _ = s_cloned.send(op).await;
                                })
                            }),
                        )
                        .await
                    }
                }
            }
        })
        .await;
    }

    pub async fn spawn_tasks(
        atomic_handler: AtomicHandler,
        sectors_manager: Arc<dyn SectorsManager>,
        self_rank: u8,
        register_client: Arc<RegisterClientImpl>,
        home_dir: PathBuf,
        processes_count: u8,
    ) {
        for (register_index, (r_c, r_s)) in atomic_handler
            .client_receivers
            .into_iter()
            .zip(atomic_handler.system_receivers.into_iter())
            .enumerate()
        {
            Self::spawn_atomic_register_task(
                register_index,
                home_dir.clone(),
                r_s,
                r_c,
                register_client.clone(),
                processes_count,
                sectors_manager.clone(),
                self_rank,
            )
            .await;
        }
    }
}

enum AtomicRegisterTaskCommand {
    SystemCommand(SystemRegisterCommand),
    ClientCommand(ClientAtomicRegisterTaskCommand),
}

type SystemAtomicRegisterTaskCommand = SystemRegisterCommand;
type ClientAtomicRegisterTaskCommand = (ClientRegisterCommand, Sender<OperationSuccess>);

impl AtomicRegisterCommandsDisposer {
    fn get_index_from_sector(&self, sector_idx: u64) -> usize {
        // TODO check sector_idx
        (sector_idx % self.n_atomic_registers as u64) as usize
    }

    async fn send_dispose(&self, command: AtomicRegisterTaskCommand) {
        match command {
            AtomicRegisterTaskCommand::ClientCommand(c) => {
                let sender_index = self.get_index_from_sector(c.0.header.sector_idx);
                let q = self.client_senders.get(sender_index).unwrap();
                _ = q.send(c).await;
            }
            AtomicRegisterTaskCommand::SystemCommand(s) => {
                let sender_index = self.get_index_from_sector(s.header.sector_idx);
                let q = self.system_senders.get(sender_index).unwrap();
                _ = q.send(s).await;
            }
        };
    }
}
#[async_trait::async_trait]
impl MySender<SystemRegisterCommand> for AtomicRegisterCommandsDisposer {
    async fn send(&self, command: SystemRegisterCommand) {
        self.send_dispose(AtomicRegisterTaskCommand::SystemCommand(command)).await;
    }
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

    pub const N_ATOMIC_REGISTERS: u8 = 108;
}
