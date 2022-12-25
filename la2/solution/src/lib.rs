mod domain;

mod atomic_register_public;
mod register_client_public;
mod reversible_stable_storage_public;
mod sectors_manager_public;
mod stable_storage_public;
mod transfer_public;
mod transport;

use std::sync::Arc;

pub use crate::domain::*;
pub use atomic_register_public::*;
use register_client_public::RegisterClient;
pub use sectors_manager_public::*;
pub use stable_storage_public::*;
use tokio::{
    net::{TcpListener, TcpStream},
    select,
    sync::mpsc::{channel, Receiver, Sender},
};
pub use transfer_public::*;

struct System {}

impl System {}

#[async_trait::async_trait]
trait MySender<T>: Send + Sync {
    async fn send(&self, s: T);
}

// Każde połączenie to osobny async task
// TODO Muszę pomyśleć, w jaki sposób powinienem przekazywać
// wiadomość o zakończeniu operacji odpowiedniemu taskowi
struct TcpReader {
    socket: TcpListener,
}

impl TcpReader {
    pub async fn new(tcp_address: &(String, u16)) -> TcpReader {
        TcpReader {
            socket: Self::bind(tcp_address).await,
        }
    }

    // Open connection as FAST as possible
    async fn bind(tcp_address: &(String, u16)) -> TcpListener {
        TcpListener::bind(tcp_address).await.unwrap()
    }

    async fn start(reader: TcpReader, atomic_register_handler: AtomicRegisterCommandsDisposer) {
        _ = tokio::spawn(async move {
            loop {
                let accept_res = reader.socket.accept().await;
                match accept_res {
                    Ok((stream, _)) => Self::spawn_read_task(stream).await,
                    Err(err) => log::info!("{:?}", err),
                }
            }
        })
        .await;
    }

    async fn spawn_read_task(stream: TcpStream) {
        // Stworzyć receivera dla wiadomości atomicRegister
        todo!();
    }
}

// Task dla każdego atomic register czytający z kolejek
// Task czytający z tcp clienta i wysyłający odpowiednie rzeczy
pub async fn run_register_process(config: Configuration) {
    let self_rank = config.public.self_rank;
    let self_rank_index = (self_rank - 1) as usize;
    let reader: TcpReader =
        TcpReader::new(config.public.tcp_locations.get(self_rank_index).unwrap()).await;
    
    let SELF_SENDER_SIZE = 2137;
    
    let (self_sender, self_receiver) = channel(SELF_SENDER_SIZE);
    let register_client: Arc<dyn RegisterClient> = Arc::new(register_client_public::RegisterClientImpl::new(
        self_rank,
        config.public.tcp_locations.len(),
        config.public.tcp_locations,
        config.hmac_system_key.clone(),
        self_sender,
    ).await);

    let atomic_register_handler =
        AtomicRegisterCommandsDisposer::spawn_tasks_and_create(config.public.n_sectors, register_client).await;
    TcpReader::start(reader, atomic_register_handler).await;
}


struct AtomicRegisterCommandsDisposer {
    client_senders: Vec<Sender<ClientRegisterCommand>>,
    system_senders: Vec<Sender<SystemRegisterCommand>>,
    n_atomic_registers: usize,
}

impl AtomicRegisterCommandsDisposer {
    const CHANNEL_SIZE: usize = 2137;

    pub async fn spawn_tasks_and_create(n_sectors: u64, register_client: Arc<dyn RegisterClient>) -> AtomicRegisterCommandsDisposer {
        let n_atomic_registers = constants::N_ATOMIC_REGISTERS as usize;
        
        let mut client_senders: Vec<Sender<ClientRegisterCommand>> =
            Vec::with_capacity(n_atomic_registers);
        let mut system_senders: Vec<Sender<SystemRegisterCommand>> =
            Vec::with_capacity(n_atomic_registers);
        for _ in 0..n_atomic_registers {
            let (s_s, r_s) = channel::<SystemRegisterCommand>(Self::CHANNEL_SIZE);
            let (s_c, r_c) = channel::<ClientRegisterCommand>(Self::CHANNEL_SIZE);
            client_senders.push(s_c);
            system_senders.push(s_s);
            Self::spawn_atomic_register_task(r_s, r_c, register_client.clone()).await;
        }
        AtomicRegisterCommandsDisposer {
            system_senders: system_senders,
            client_senders: client_senders,
            n_atomic_registers: n_atomic_registers
        }
    }

    async fn spawn_atomic_register_task(
        r_s: Receiver<SystemRegisterCommand>,
        r_c: Receiver<ClientRegisterCommand>,
        register_client: Arc<dyn RegisterClient>
    ) {
        todo!()
    }

    async fn send_dispose(&self, command: RegisterCommand) {
        match command {
            RegisterCommand::Client(c) => {
                todo!()
                // let atomic_register_dispose:  
            },
            RegisterCommand::System(s) => {
                todo!()
            },
        }
    }
}
#[async_trait::async_trait]
impl MySender<SystemRegisterCommand> for AtomicRegisterCommandsDisposer {
    async fn send(&self, command: SystemRegisterCommand) {
        self.send_dispose(RegisterCommand::System(command)).await;
    }
}

// This probably won't be neccessary
// struct MessageTracker {
//     client_messages: Box<dyn ReversibleStableStorage>,
// }

// impl MessageTracker {
//     fn persist_client_command() {

//     }
// }

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
