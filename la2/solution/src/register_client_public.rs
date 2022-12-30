use tokio::{
    net::TcpStream,
    sync::{
        mpsc::{channel, Receiver, Sender},
        RwLock,
    },
    time::{self, Instant},
};

use crate::{
    serialize_register_command, MySender, RegisterCommand, SectorIdx, SystemRegisterCommand,
};
use std::{collections::HashMap, sync::Arc, time::Duration};

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

#[derive(Debug)]
pub struct Send {
    pub cmd: Arc<SystemRegisterCommand>,
    /// Identifier of the target process. Those start at 1.
    pub target: u8,
}

// pub struct RegisterClientImpl {
//     to_rebroadcast: Arc<RwLock<HashMap<SectorIdx, Broadcast>>>,
//     self_sender: Arc<dyn MySender<SystemRegisterCommand>>,
//     tcp_sender: Sender<Send>,
//     self_rank: u8,
//     processes_count: usize,
// }

// impl RegisterClientImpl {
//     const CHANNEL_SIZE: usize = 5000;

//     pub async fn new(
//         self_rank: u8,
//         processes_count: usize,
//         tcp_locations: Vec<(String, u16)>,
//         hmac_key: [u8; 64],
//         self_sender: Arc<dyn MySender<SystemRegisterCommand>>,
//     ) -> RegisterClientImpl {
//         let (s, r) = channel(Self::CHANNEL_SIZE);
//         let mut tcp_sender = TcpSender::new(tcp_locations, r, hmac_key);
//         tokio::spawn(async move {
//             tcp_sender.send_in_loop().await;
//         });
//         let register_client = RegisterClientImpl {
//             to_rebroadcast: Arc::new(RwLock::new(HashMap::new())),
//             self_sender: self_sender,
//             tcp_sender: s,
//             self_rank: self_rank,
//             processes_count: processes_count,
//         };
//         register_client
//             .spawn_timer(Duration::from_millis(2000))
//             .await;
//         register_client
//     }

//     async fn spawn_timer(&self, duration: Duration) {
//         let to_rebroadcast = self.to_rebroadcast.clone();
//         let mut interval = time::interval(duration);

//         let tcp_sender = self.tcp_sender.clone();
//         let self_sender = self.self_sender.clone();

//         let processes_count = self.processes_count;
//         let self_rank = self.self_rank;

//         tokio::spawn(async move {
//             loop {
//                 interval.tick().await;
//                 let guard = to_rebroadcast.read().await;
//                 for (_, b) in guard.iter() {
//                     Self::handle_broadcast(
//                         b,
//                         &self_sender,
//                         &tcp_sender,
//                         processes_count,
//                         self_rank,
//                     )
//                     .await;
//                 }
//             }
//         });
//     }

//     async fn handle_broadcast(
//         b: &Broadcast,
//         self_sender: &Arc<dyn MySender<SystemRegisterCommand>>,
//         tcp_sender: &Sender<Send>,
//         processes_count: usize,
//         self_rank: u8,
//     ) {
//         for target in 1..=processes_count {
//             let target_u8 = target as u8;
//             if target_u8 == self_rank {
//                 self_sender.send(b.cmd.as_ref().clone()).await;
//             } else {
//                 tcp_sender
//                     .send(Send {
//                         cmd: b.cmd.clone(),
//                         target: target_u8,
//                     })
//                     .await
//                     .unwrap();
//             }
//         }
//     }

//     pub async fn cancel_broadcast(&self, sector_idx: SectorIdx) {
//         self.to_rebroadcast.write().await.remove(&sector_idx);
//     }

//     pub async fn insert_broadcast(&self, sector_idx: SectorIdx, broadcast: Broadcast) {
//         self.to_rebroadcast
//             .write()
//             .await
//             .insert(sector_idx, broadcast);
//     }
// }

// struct TcpSender {
//     tcp_locations: Vec<(String, u16)>,
//     streams: Vec<Option<TcpStream>>,
//     receiver: Receiver<Send>,
//     hmac_key: [u8; 64],
// }

// impl TcpSender {
//     pub fn new(
//         tcp_locations: Vec<(String, u16)>,
//         receiver: Receiver<Send>,
//         hmac_key: [u8; 64],
//     ) -> TcpSender {
//         let mut streams = Vec::new();
//         for _ in 0..tcp_locations.len() {
//             streams.push(None);
//         }

//         TcpSender {
//             tcp_locations: tcp_locations,
//             streams: streams,
//             receiver: receiver,
//             hmac_key: hmac_key,
//         }
//     }

//     async fn send_in_loop(&mut self) {
//         loop {
//             let prev: Instant = Instant::now();
//             let new_command = self.receiver.recv().await.unwrap();
//             println!("Waited {:?}", Instant::now().duration_since(prev));
//             let index = new_command.target as usize - 1;

//             log::debug!(
//                 "Trying to send to process {} from address {:?}",
//                 new_command.target,
//                 self.tcp_locations.get(index).unwrap()
//             );
//             if self.streams.get(index).unwrap().is_none() {
//                 println!(
//                     "No stream to process {}, trying to connect",
//                     new_command.target
//                 );
//                 self.connect(index).await;
//             }

//             let s = self.streams.get_mut(index).unwrap();
//             if let Some(tcp) = s {
//                 match serialize_register_command(
//                     &RegisterCommand::System(new_command.cmd.as_ref().clone()),
//                     tcp,
//                     &self.hmac_key,
//                 )
//                 .await
//                 {
//                     Ok(_) => {
//                         log::debug!("serialize to {} successful", new_command.target);
//                     }
//                     Err(_) => {
//                         log::debug!("serialize to {} not successful", new_command.target);
//                         *s = None;
//                     }
//                 }
//             } else {
//                 log::debug!("Couldnt connect to process {}", new_command.target);
//             }
//         }
//     }

//     async fn connect(&mut self, index: usize) {
//         log::debug!("Connecting to {:?}", self.tcp_locations.get(index).unwrap());
//         self.streams.insert(
//             index,
//             TcpStream::connect(self.tcp_locations.get(index).unwrap())
//                 .await
//                 .ok(),
//         )
//     }
// }

// // For sure it implements stubborn link algorithm
// #[async_trait::async_trait]
// impl RegisterClient for RegisterClientImpl {
//     async fn send(&self, msg: Send) {
//         // log::debug!(
//         //     "{} received send message for process {}",
//         //     self.self_rank,
//         //     msg.target
//         // );
//         if self.self_rank == msg.target {
//             self.self_sender.send(msg.cmd.as_ref().clone()).await;
//         } else {
//             self.tcp_sender
//                 .send(Send {
//                     cmd: msg.cmd.clone(),
//                     target: msg.target,
//                 })
//                 .await
//                 .unwrap();
//         }
//     }

//     async fn broadcast(&self, msg: Broadcast) {
//         println!("Broadcast {}", self.processes_count);
//         Self::handle_broadcast(
//             &msg,
//             &self.self_sender,
//             &self.tcp_sender,
//             self.processes_count,
//             self.self_rank,
//         )
//         .await;
//         self.insert_broadcast(msg.cmd.header.sector_idx, msg).await;
//     }
// }

pub struct NewRegisterClientImpl {
    to_rebroadcast: Arc<RwLock<HashMap<SectorIdx, Broadcast>>>,
    self_sender: Arc<dyn MySender<SystemRegisterCommand>>,
    senders: Vec<Option<Sender<Send>>>,
    self_rank: u8,
    processes_count: usize,
}

impl NewRegisterClientImpl {
    const CHANNEL_SIZE: usize = 5000;

    pub async fn new(
        self_rank: u8,
        processes_count: usize,
        tcp_locations: Vec<(String, u16)>,
        hmac_key: [u8; 64],
        self_sender: Arc<dyn MySender<SystemRegisterCommand>>,
    ) -> NewRegisterClientImpl {
        let mut senders: Vec<Option<Sender<Send>>> = Vec::with_capacity(processes_count);
        for process_id in 1..=processes_count {
            let new_sender_op = if process_id == self_rank as usize {
                None
            } else {
                let tcp_location = tcp_locations.get(process_id - 1).unwrap().clone();
                let (s, r) = channel(Self::CHANNEL_SIZE);
                let mut tcp_sender = SingleTcpSender::new(tcp_location, r, hmac_key);
                tokio::spawn(async move {
                    tcp_sender.send_in_loop().await;
                });
                Some(s)
            };
            senders.push(new_sender_op);
        }

        let register_client = NewRegisterClientImpl {
            to_rebroadcast: Arc::new(RwLock::new(HashMap::new())),
            self_sender,
            senders,
            self_rank,
            processes_count,
        };
        register_client
            .spawn_timer(Duration::from_millis(1000))
            .await;
        register_client
    }

    async fn spawn_timer(&self, duration: Duration) {
        let to_rebroadcast = self.to_rebroadcast.clone();
        let mut interval = time::interval(duration);

        let tcp_senders = self.senders.clone();
        let self_sender = self.self_sender.clone();

        let processes_count = self.processes_count;
        let self_rank = self.self_rank;

        tokio::spawn(async move {
            loop {
                interval.tick().await;
                let guard = to_rebroadcast.read().await;
                for (_, b) in guard.iter() {
                    Self::handle_broadcast(
                        b,
                        &self_sender,
                        &tcp_senders,
                        processes_count,
                        self_rank,
                    )
                    .await;
                }
            }
        });
    }

    async fn handle_broadcast(
        b: &Broadcast,
        self_sender: &Arc<dyn MySender<SystemRegisterCommand>>,
        tcp_senders: &Vec<Option<Sender<Send>>>,
        processes_count: usize,
        self_rank: u8,
    ) {
        for target in 1..=processes_count {
            let target_u8 = target as u8;
            if target_u8 == self_rank {
                self_sender.send(b.cmd.as_ref().clone()).await;
            } else {
                if let Some(sender) = tcp_senders.get(target - 1).unwrap() {
                    sender
                        .send(Send {
                            cmd: b.cmd.clone(),
                            target: target_u8,
                        })
                        .await
                        .unwrap();
                }
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

// For sure it implements stubborn link algorithm
#[async_trait::async_trait]
impl RegisterClient for NewRegisterClientImpl {
    async fn send(&self, msg: Send) {
        if self.self_rank == msg.target {
            self.self_sender.send(msg.cmd.as_ref().clone()).await;
        } else {
            if let Some(sender) = self.senders.get((msg.target - 1) as usize).unwrap() {
                sender
                    .send(Send {
                        cmd: msg.cmd.clone(),
                        target: msg.target,
                    })
                    .await
                    .unwrap();
            }
        }
    }

    async fn broadcast(&self, msg: Broadcast) {
        NewRegisterClientImpl::handle_broadcast(
            &msg,
            &self.self_sender,
            &self.senders,
            self.processes_count,
            self.self_rank,
        )
        .await;
        self.insert_broadcast(msg.cmd.header.sector_idx, msg).await;
    }
}

struct SingleTcpSender {
    tcp_location: (String, u16),
    stream: Option<TcpStream>,
    receiver: Receiver<Send>,
    hmac_key: [u8; 64],
}

impl SingleTcpSender {
    pub fn new(
        tcp_location: (String, u16),
        receiver: Receiver<Send>,
        hmac_key: [u8; 64],
    ) -> SingleTcpSender {
        SingleTcpSender {
            tcp_location: tcp_location,
            stream: None,
            receiver: receiver,
            hmac_key: hmac_key,
        }
    }

    async fn send_in_loop(&mut self) {
        loop {
            let prev: Instant = Instant::now();
            let new_command = self.receiver.recv().await.unwrap();
            println!("Waited {:?}", Instant::now().duration_since(prev));
            if self.stream.is_none() {
                println!(
                    "No stream to process {}, trying to connect",
                    new_command.target
                );
                self.connect().await;
            }

            if let Some(tcp) = &mut self.stream {
                match serialize_register_command(
                    &RegisterCommand::System(new_command.cmd.as_ref().clone()),
                    tcp,
                    &self.hmac_key,
                )
                .await
                {
                    Ok(_) => {
                        log::debug!("serialize to {} successful", new_command.target);
                    }
                    Err(_) => {
                        log::debug!("serialize to {} not successful", new_command.target);
                        self.stream = None;
                    }
                }
            } else {
                log::debug!("Couldnt connect to process {}", new_command.target);
            }
        }
    }

    async fn connect(&mut self) {
        log::debug!("Connecting to {:?}", self.tcp_location);
        self.stream = TcpStream::connect(&self.tcp_location).await.ok()
    }
}
