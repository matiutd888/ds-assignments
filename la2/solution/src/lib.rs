mod domain;

mod atomic_register_public;
mod register_client_public;
mod sectors_manager_public;
mod stable_storage_public;
mod transfer_public;
mod transport;
mod reversible_stable_storage_public;

use std::{path::PathBuf};

pub use crate::domain::*;
pub use atomic_register_public::*;
pub use register_client_public::*;
use reversible_stable_storage_public::ReversibleStableStorage;
pub use sectors_manager_public::*;
pub use stable_storage_public::*;
use tokio::net::TcpListener;
pub use transfer_public::*;
pub use register_client_public::*;

struct System {

}

impl System {

}

struct TcpReader {

}

impl TcpReader {

    // Open connection as FAST as possible
    async fn new(tcp_address: (String, u16)) {
        let socket = TcpListener::bind(tcp_address).await.unwrap();
    }
}

// Task dla każdego atomic register czytający z kolejek
// Task czytający z tcp clienta i wysyłający odpowiednie rzeczy
pub async fn run_register_process(config: Configuration) {
    
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
}
