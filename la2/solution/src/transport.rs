// SystemCommandHeader

// TODO maybe all messages should have separate, network interfaces.
// That way we could use deser to deserialize them.

// This is just an idea of providing communication

type MagicNumber = [u8; 4];

// TODO to jest jednak konieczne!!!!!!!!!!!!!!
// Bo potrzebuję znać rozmiary bajtów każdej struktury

pub mod ClientProcessCommunication {
    use serde::Serialize;

    use crate::{
        constants::{self, SECTOR_SIZE_BYTES},
        ClientCommandHeader,
    };

    use super::MagicNumber;

    #[derive(Serialize)]
    pub struct ClientToProcess<T: ClientToProcessContent + Serialize> {
        magic_number: MagicNumber,
        padding: [u8; 3],
        msg_type: u8,
        header: ClientCommandHeader,
        content: T,
    }

    pub trait ClientToProcessContent {}
    impl ClientToProcessContent for ClientToProcessContentRead {}
    impl ClientToProcessContent for ClientToProcessContentWrite {}
    pub struct ClientToProcessContentRead {}
    pub struct ClientToProcessContentWrite {
        data: [u8; SECTOR_SIZE_BYTES],
    }

    #[derive(Serialize)]
    pub struct ProcessToClient<T: ProcessToClientContent + Serialize> {
        magic_number: MagicNumber,
        padding: [u8; 2],
        status_code: u8,
        header: ProcessToClientHeader,
        content: T,
    }

    #[derive(Serialize)]
    pub struct ProcessToClientHeader {
        request_number: u64,
    }

    pub trait ProcessToClientContent {}
    impl ProcessToClientContent for ProcessToClientContentRead {}
    impl ProcessToClientContent for ProcessToClientContentWrite {}

    struct ProcessToClientContentRead {
        bytes: [u8; constants::SECTOR_SIZE_BYTES],
    }

    struct ProcessToClientContentWrite {}
}
