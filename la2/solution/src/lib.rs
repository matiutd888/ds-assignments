mod domain;

mod atomic_register_public;
mod register_client_public;
mod reversible_stable_storage_public;
mod sectors_manager_public;
mod stable_storage_public;
mod transfer_public;

use std::{collections::HashSet, path::PathBuf, sync::Arc};

pub use crate::domain::*;
pub use atomic_register_public::*;
use env_logger;
pub use register_client_public::*;
pub use sectors_manager_public::*;
pub use stable_storage_public::*;
use tokio::{
    fs,
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream,
    },
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
        let n_sectors = atomic_register_handler.n_sectors;

        // TODO think about not using arcs.
        let hmac_client_key_arc = Arc::new(reader.hmac_client_key);
        let hmac_system_key_arc = Arc::new(reader.hmac_system_key);

        tokio::spawn(async move {
            loop {
                let accept_res = reader.socket.accept().await;
                match accept_res {
                    Ok((stream, _)) => {
                        Self::spawn_connection_task(
                            stream,
                            atomic_register_handler.clone(),
                            hmac_client_key_arc.clone(),
                            hmac_system_key_arc.clone(),
                            n_sectors,
                        )
                        .await
                    }
                    Err(err) => log::info!("{:?}", err),
                }
            }
        });
    }

    async fn send_client_response(
        stream: &mut OwnedWriteHalf,
        c: &ClientCommandResponseTransfer,
        hmac_client_key: &[u8; 32],
    ) -> Result<(), std::io::Error> {
        serialize_client_response(c, stream, &hmac_client_key).await
    }

    async fn spawn_writer_task(
        mut writer: OwnedWriteHalf,
        mut r_op_success: Receiver<ClientCommandResponseTransfer>,
        hmac_client_key: [u8; 32],
    ) {
        tokio::spawn(async move {
            loop {
                if let Some(op) = r_op_success.recv().await {
                    let res = Self::send_client_response(&mut writer, &op, &hmac_client_key).await;
                    if let Err(err) = res {
                        log::error!("Error {} while writing client response", err);
                    }
                }
            }
        });
    }

    async fn handle_incorrect_sector_id(
        s: &RegisterCommand,
        n_sectors: u64,
        s_op_end: Sender<ClientCommandResponseTransfer>,
    ) -> bool {
        fn check_sector_idx(sector_idx: SectorIdx, n_sectors: u64) -> bool {
            sector_idx < n_sectors
        }

        match s {
            RegisterCommand::Client(c) => {
                if !check_sector_idx(c.header.sector_idx, n_sectors) {
                    log::error!("Invalid sector id {:?}", c.header);

                    s_op_end
                        .send(ClientCommandResponseTransfer::from_invalid_sector_id(c))
                        .await
                        .unwrap();
                    true
                } else {
                    false
                }
            }
            RegisterCommand::System(s) => {
                if !check_sector_idx(s.header.sector_idx, n_sectors) {
                    log::error!("Invalid sector id {:?}", s.header);
                    true
                } else {
                    false
                }
            }
        }
    }

    async fn spawn_reader_task(
        mut reader: OwnedReadHalf,
        hmac_client_key: [u8; 32],
        hmac_system_key: [u8; 64],
        command_disposer: Arc<AtomicRegisterCommandsDisposer>,
        sender_op_end: Sender<ClientCommandResponseTransfer>,
        n_sectors: u64,
    ) {
        tokio::spawn(async move {
            loop {
                let res =
                    deserialize_register_command(&mut reader, &hmac_system_key, &hmac_client_key)
                        .await;

                match res {
                    Ok((cmd, b)) => {
                        if b {
                            if !Self::handle_incorrect_sector_id(
                                &cmd,
                                n_sectors,
                                sender_op_end.clone(),
                            )
                            .await
                            {
                                let atomic_object_message = match cmd {
                                    RegisterCommand::Client(c) => {
                                        AtomicRegisterTaskCommand::ClientCommand((
                                            c,
                                            sender_op_end.clone(),
                                        ))
                                    }
                                    RegisterCommand::System(s) => {
                                        AtomicRegisterTaskCommand::SystemCommand(s)
                                    }
                                };
                                command_disposer.send_dispose(atomic_object_message).await;
                            }
                        } else {
                            match cmd {
                                RegisterCommand::Client(c) => {
                                    log::error!("Invalid hmac in client command {:?}", c);
                                    sender_op_end
                                        .send(ClientCommandResponseTransfer::from_invalid_hmac(&c))
                                        .await
                                        .unwrap();
                                }
                                RegisterCommand::System(_) => {
                                    log::error!("Invalid hmac in system command {:?}", cmd)
                                }
                            }
                        }
                    }
                    Err(err) => log::error!("Error while deserializing {}", err),
                }
            }
        });
    }

    async fn spawn_connection_task(
        stream: TcpStream,
        command_disposer: Arc<AtomicRegisterCommandsDisposer>,
        hmac_client_key_arc: Arc<[u8; 32]>,
        hmac_system_key_arc: Arc<[u8; 64]>,
        n_sectors: u64,
    ) {
        let (s_op_success, r_op_success) = channel::<ClientCommandResponseTransfer>(2137);
        let (reader, writer) = stream.into_split();

        let hmac_client_key = hmac_client_key_arc.as_ref().clone();
        Self::spawn_writer_task(writer, r_op_success, hmac_client_key.clone()).await;
        Self::spawn_reader_task(
            reader,
            hmac_client_key,
            hmac_system_key_arc.as_ref().clone(),
            command_disposer,
            s_op_success,
            n_sectors,
        )
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
    env_logger::try_init();

    let self_rank = config.public.self_rank;
    let self_rank_index = (self_rank - 1) as usize;
    let reader: TcpReader = TcpReader::new(
        config.public.tcp_locations.get(self_rank_index).unwrap(),
        config.hmac_system_key,
        config.hmac_client_key,
    )
    .await;

    let processes_count = config.public.tcp_locations.len();

    let sectors_manager_path = get_sectors_manager_pathbuf(config.public.storage_dir.clone());
    fs::create_dir_all(&sectors_manager_path).await.unwrap();

    let sectors_manager = build_sectors_manager(sectors_manager_path);

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
    n_sectors: u64,
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
            n_sectors,
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
        let path = get_atomic_register_metadata_pathbuf(original_pathbuf, register_index);
        fs::create_dir_all(&path).await.unwrap();
        let metadata = build_stable_storage(path).await;
        let mut a = my_build_atomic_register(
            format!("{}", register_index),
            self_rank,
            metadata,
            register_client.clone(),
            sectors_manager,
            processes_count,
        )
        .await;

        tokio::spawn(async move {
            let (s, mut r_finished) = channel::<ClientCommandResponseTransfer>(1);
            let s_arc = Arc::new(s);
            let mut current_operation_data: Option<(u64, Sender<ClientCommandResponseTransfer>)> =
                None;
            let mut messages_during_current_request: HashSet<Uuid> = HashSet::new();
            loop {
                if let Some((current_sector, success_sender)) = &current_operation_data {
                    // log::debug!(
                    //     "{} Register object {} is processing sector {}",
                    //     self_rank,
                    //     register_index,
                    //     current_sector
                    // );
                    select! {
                        Some(cmd) = r_s.recv() => {
                            if !messages_during_current_request.contains(&cmd.header.msg_ident) {
                                messages_during_current_request.insert(cmd.header.msg_ident.clone());
                                a.system_command(cmd).await;
                            }
                        },
                        Some(op) = r_finished.recv() => {
                            register_client.cancel_broadcast(current_sector.clone()).await;
                            success_sender.send(op).await.unwrap();
                            current_operation_data = None;
                        }
                    }
                } else {
                    // log::debug!("{} register object {} waiting for task", register_index);
                    select! {
                        Some(cmd) = r_s.recv() => {
                            a.system_command(cmd).await;
                        },

                        Some((cmd, sender)) = r_c.recv() => {
                            current_operation_data = Some((cmd.header.sector_idx, sender));
                            messages_during_current_request.clear();
                            let s_cloned = s_arc.clone();
                            a.client_command(
                                cmd,
                                Box::new(|op: OperationSuccess| {
                                    Box::pin(async move {
                                        s_cloned
                                            .send(ClientCommandResponseTransfer::from_success(op))
                                            .await.unwrap();
                                    })
                                }),
                            )
                            .await
                        }
                    }
                }
            }
        });
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
type ClientAtomicRegisterTaskCommand =
    (ClientRegisterCommand, Sender<ClientCommandResponseTransfer>);

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
                q.send(c).await.unwrap();
            }
            AtomicRegisterTaskCommand::SystemCommand(s) => {
                let sender_index = self.get_index_from_sector(s.header.sector_idx);
                let q = self.system_senders.get(sender_index).unwrap();
                q.send(s).await.unwrap();
            }
        };
    }
}
#[async_trait::async_trait]
impl MySender<SystemRegisterCommand> for AtomicRegisterCommandsDisposer {
    async fn send(&self, command: SystemRegisterCommand) {
        self.send_dispose(AtomicRegisterTaskCommand::SystemCommand(command))
            .await;
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

    pub const N_ATOMIC_REGISTERS: u8 = 3;
}
