use tokio::{
    net::TcpStream,
    sync::{
        mpsc::{channel, Receiver, Sender},
        RwLock,
    },
    time,
};

use crate::{serialize_register_command, RegisterCommand, SectorIdx, SystemRegisterCommand, MySender};
use std::{collections::{HashMap, HashSet}, sync::Arc, time::Duration};

#[async_trait::async_trait]
/// We do not need any public implementation of this trait. It is there for use
/// in AtomicRegister. In our opinion it is a safe bet to say some structure of
/// this kind must appear in your solution.
pub trait RegisterClient: core::marker::Send + core::marker::Sync {
    /// Sends a system message to a single process.
    async fn send(&self, msg: Send);

    /// Broadcasts a system message to all processes in the system, including self.
    async fn broadcast(&self, msg: Broadcast);
}

pub struct Broadcast {
    pub cmd: Arc<SystemRegisterCommand>,
}

pub struct Send {
    pub cmd: Arc<SystemRegisterCommand>,
    /// Identifier of the target process. Those start at 1.
    pub target: u8,
}

pub struct RegisterClientImpl {
    // TODO Add updating list of processes we need to rebroadcast to
    to_rebroadcast: Arc<RwLock<HashMap<SectorIdx, Broadcast>>>,
    // TODO think about changing those into bounded Senders
    self_sender: Arc<dyn MySender<SystemRegisterCommand>>,
    tcp_sender: Sender<Send>,
    self_rank: u8,
    processes_count: usize,
}

impl RegisterClientImpl {
    const CHANNEL_SIZE: usize = 2137;

    pub async fn new(
        self_rank: u8,
        processes_count: usize,
        tcp_locations: Vec<(String, u16)>,
        hmac_key: [u8; 64],
        self_sender: Arc<dyn MySender<SystemRegisterCommand>>,
    ) -> RegisterClientImpl {
        let (s, r) = channel(Self::CHANNEL_SIZE);
        let mut tcp_sender = TcpSender::new(tcp_locations, r, hmac_key);
        let _ = tokio::spawn(async move {
            tcp_sender.send_in_loop().await;
        })
        .await;
        let register_client = RegisterClientImpl {
            to_rebroadcast: Arc::new(RwLock::new(HashMap::new())),
            self_sender: self_sender,
            tcp_sender: s,
            self_rank: self_rank,
            processes_count: processes_count,
        };
        register_client
            .spawn_timer(Duration::from_millis(3000))
            .await;
        register_client
    }

    async fn spawn_timer(&self, duration: Duration) {
        let to_rebroadcast = self.to_rebroadcast.clone();
        let mut interval = time::interval(duration);

        let tcp_sender = self.tcp_sender.clone();
        let self_sender = self.self_sender.clone();

        let processes_count = self.processes_count;
        let self_rank = self.self_rank;

        _ = tokio::spawn(async move {
            loop {
                interval.tick().await;
                let guard = to_rebroadcast.read().await;
                for (_, b) in guard.iter() {
                    Self::handle_broadcast(
                        b,
                        &self_sender,
                        &tcp_sender,
                        processes_count,
                        self_rank,
                    )
                    .await;
                }
            }
        })
        .await;
    }

    async fn handle_broadcast(
        b: &Broadcast,
        self_sender: &Arc<dyn MySender<SystemRegisterCommand>>,
        tcp_sender: &Sender<Send>,
        processes_count: usize,
        self_rank: u8,
    ) {
        for target in 1..=processes_count {
            let target_u8 = target as u8;
            if target_u8 == self_rank {
                _ = self_sender.send(b.cmd.as_ref().clone());
            } else {
                _ = tcp_sender.send(Send {
                    cmd: b.cmd.clone(),
                    target: target_u8,
                });
            }
        }
    }

    pub async fn cancel_broadcast(&self, sector_idx: SectorIdx) {
        self.to_rebroadcast.write().await.remove(&sector_idx);
    }

    pub async fn insert_broadcast(&self, sector_idx: SectorIdx, broadcast: Broadcast) {
        self.to_rebroadcast
            .write()
            .await
            .insert(sector_idx, broadcast);
    }
}

struct TcpSender {
    tcp_locations: Vec<(String, u16)>,
    streams: Vec<Option<TcpStream>>,
    receiver: Receiver<Send>,
    hmac_key: [u8; 64],
}

impl TcpSender {
    pub fn new(
        tcp_locations: Vec<(String, u16)>,
        receiver: Receiver<Send>,
        hmac_key: [u8; 64],
    ) -> TcpSender {
        let mut streams = Vec::new();
        for _ in 0..tcp_locations.len() {
            streams.push(None);
        }

        TcpSender {
            tcp_locations: tcp_locations,
            streams: streams,
            receiver: receiver,
            hmac_key: hmac_key,
        }
    }

    async fn send_in_loop(&mut self) {
        loop {
            let new_command = self.receiver.recv().await.unwrap();
            let index = new_command.target as usize;
            if self.streams.get(index).unwrap().is_none() {
                self.connect(index).await;
            }
            if let Some(tcp) = self.streams.get_mut(index).unwrap() {
                let _ans = serialize_register_command(
                    &RegisterCommand::System(new_command.cmd.as_ref().clone()),
                    tcp,
                    &self.hmac_key,
                )
                .await;
                // TODO handle ans?
            }
        }
    }

    async fn connect(&mut self, index: usize) {
        self.streams.insert(
            index,
            TcpStream::connect(self.tcp_locations.get(index).unwrap())
                .await
                .ok(),
        )
    }
}

// For sure it implements stubborn link algorithm
#[async_trait::async_trait]
impl RegisterClient for RegisterClientImpl {
    async fn send(&self, msg: Send) {
        if self.self_rank == msg.target {
            _ = self.self_sender.send(msg.cmd.as_ref().clone());
        } else {
            _ = self.tcp_sender.send(Send {
                cmd: msg.cmd.clone(),
                target: msg.target,
            });
        }
    }

    async fn broadcast(&self, msg: Broadcast) {
        Self::handle_broadcast(
            &msg,
            &self.self_sender,
            &self.tcp_sender,
            self.processes_count,
            self.self_rank,
        )
        .await;
        self.insert_broadcast(msg.cmd.header.sector_idx, msg).await;
    }
}
