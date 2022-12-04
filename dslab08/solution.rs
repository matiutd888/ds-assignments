use std::collections::{HashMap, HashSet};
use std::mem::swap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use log::debug;
use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;
use uuid::Uuid;

use executor::{Handler, ModuleRef, System};

/// A message which disables a process. Used for testing
pub struct Disable;

#[derive(Clone)]
struct Tick;

pub struct FailureDetectorModule {
    enabled: bool,
    uuid: Uuid,
    socket: Arc<UdpSocket>,
    addresses: HashMap<Uuid, SocketAddr>,
    all_idents: Vec<Uuid>,
    alive_curr_set: HashSet<Uuid>,
    alive_curr_ret: Vec<Uuid>,
    alive_prev: Vec<Uuid>,
}

impl FailureDetectorModule {
    pub async fn new(
        system: &mut System,
        delay: Duration,
        addresses: &HashMap<Uuid, SocketAddr>,
        ident: Uuid,
        all_idents: HashSet<Uuid>,
    ) -> ModuleRef<Self> {
        let addr = addresses.get(&ident).unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());

        let mut all_idents_vec: Vec<Uuid> = vec![];
        let mut addresses_new: HashMap<Uuid, SocketAddr> = HashMap::new();
        for ident in all_idents {
            all_idents_vec.push(ident);
            addresses_new.insert(ident, addresses.get(&ident).unwrap().clone());
        }
        let module_ref = system
            .register_module(Self {
                enabled: true,
                uuid: ident,
                socket: socket.clone(),
                addresses: addresses_new,
                alive_curr_set: HashSet::new(),
                alive_curr_ret: vec![],
                alive_prev: all_idents_vec.clone(),
                all_idents: all_idents_vec,
            })
            .await;

        tokio::spawn(deserialize_and_forward(socket, module_ref.clone()));

        module_ref.request_tick(Tick, delay).await;

        module_ref
    }
}

/// New operation arrived on socket.
#[async_trait::async_trait]
impl Handler<DetectorOperationUdp> for FailureDetectorModule {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, item: DetectorOperationUdp) {
        if self.enabled {
            let sock_addr = item.1;
            match item.0 {
                DetectorOperation::AliveRequest => {
                    let alive_info = DetectorOperation::AliveInfo(self.alive_prev.clone());
                    serialize_and_send::<DetectorOperation>(
                        self.socket.clone(),
                        &sock_addr,
                        alive_info,
                    )
                    .await;
                }
                DetectorOperation::HeartbeatRequest => {
                    serialize_and_send(
                        self.socket.clone(),
                        &sock_addr,
                        DetectorOperation::HeartbeatResponse(self.uuid),
                    )
                    .await
                }
                DetectorOperation::HeartbeatResponse(uuid) => {
                    if !self.alive_curr_set.insert(uuid.clone()) {
                        self.alive_curr_ret.push(uuid);
                    }
                }
                _ => {
                    debug!("Unexpected message");
                }
            }
        }
    }
}

/// Called periodically to check send broadcast and update alive processes.
#[async_trait::async_trait]
impl Handler<Tick> for FailureDetectorModule {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, _msg: Tick) {
        if self.enabled {
            self.alive_prev = vec![];
            swap::<Vec<Uuid>>(&mut self.alive_curr_ret, &mut self.alive_prev);
            self.alive_curr_set.clear();

            for uuid in self.all_idents.iter() {
                serialize_and_send(
                    self.socket.clone(),
                    self.addresses.get(&uuid).unwrap(),
                    DetectorOperation::HeartbeatRequest,
                )
                .await;
            }
        }
    }
}

#[async_trait::async_trait]
impl Handler<Disable> for FailureDetectorModule {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, _msg: Disable) {
        self.enabled = false;
    }
}

async fn deserialize_and_forward(
    socket: Arc<UdpSocket>,
    module_ref: ModuleRef<FailureDetectorModule>,
) {
    let mut buffer = vec![0];
    while let Ok((len, sender)) = socket.peek_from(&mut buffer).await {
        if len == buffer.len() {
            buffer.resize(2 * buffer.len(), 0);
        } else {
            socket.recv_from(&mut buffer).await.unwrap();
            match bincode::deserialize(&buffer) {
                Ok(msg) => module_ref.send(DetectorOperationUdp(msg, sender)).await,
                Err(err) => {
                    debug!("Invalid format of detector operation ({})!", err);
                }
            }
        }
    }
}

async fn send_all_data(socket: Arc<UdpSocket>, addr: &SocketAddr, send_buff: Vec<u8>) {
    let mut bytes_send = 0;
    while bytes_send < send_buff.len() {
        match socket.send_to(&send_buff[bytes_send..], addr).await {
            Ok(bytes) => {
                bytes_send += bytes;
            }
            Err(err) => {
                debug!("Error sending udp packet ({})!", err);
            }
        }
    }
}

async fn serialize_and_send<T: Serialize>(socket: Arc<UdpSocket>, addr: &SocketAddr, xd: T) {
    match bincode::serialize(&xd) {
        Ok(send_buff) => {
            send_all_data(socket, addr, send_buff).await;
        }
        Err(err) => {
            debug!("Serialization error during send ({})!", err);
        }
    }
}

struct DetectorOperationUdp(DetectorOperation, SocketAddr);

#[derive(Serialize, Deserialize)]
pub enum DetectorOperation {
    /// Request to receive a heartbeat.
    HeartbeatRequest,
    /// Response to heartbeat, contains uuid of the receiver of HeartbeatRequest.
    HeartbeatResponse(Uuid),
    /// Request to receive information about working processes.
    AliveRequest,
    /// Vector of processes which are alive according to AliveRequest receiver.
    AliveInfo(Vec<Uuid>),
}
