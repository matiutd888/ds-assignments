use crate::{
    constants::{self, MsgType},
    ClientCommandHeader, ClientRegisterCommand, ClientRegisterCommandContent, RegisterCommand,
    SectorVec, SystemRegisterCommand, SystemRegisterCommandContent,
};
use bincode::Options;
use serde::Serialize;
use std::{
    io::{empty, Error, ErrorKind},
    mem,
};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

pub async fn deserialize_register_command(
    data: &mut (dyn AsyncRead + Send + Unpin),
    hmac_system_key: &[u8; 64],
    hmac_client_key: &[u8; 32],
) -> Result<(RegisterCommand, bool), Error> {
    let magic_number: [u8; 4] = [97, 116, 100, 100];

    return Err(Error::new(ErrorKind::Other, "oh no!"));
}

trait CustomSerializable {
    fn custom_serialize(&self, buffer: Vec<u8>) -> Vec<u8>;
}

impl<T: serde::Serialize> CustomSerializable for T {
    fn custom_serialize(&self, mut buffer: Vec<u8>) -> Vec<u8> {
        buffer.extend(serialize_serializable(self));
        buffer
    }
}

impl CustomSerializable for ClientRegisterCommandContent {
    fn custom_serialize(&self, mut buffer: Vec<u8>) -> Vec<u8> {
        match self {
            ClientRegisterCommandContent::Read => buffer,
            ClientRegisterCommandContent::Write { data } => {
                data.custom_serialize(buffer)
            }
    }
}
}

impl CustomSerializable for SectorVec {
    fn custom_serialize(&self) -> Vec<u8> {
        assert!(
            self.0.len() == constants::SECTOR_SIZE_BYTES,
            "Data length should be equal to sector size!"
        );
        self.0.clone()
    }
}

impl CustomSerializable for SystemRegisterCommandContent {
    fn custom_serialize(&self) -> Vec<u8> {
        match self {
            SystemRegisterCommandContent::ReadProc => Vec::new(),
            SystemRegisterCommandContent::Value {
                timestamp,
                write_rank,
                sector_data,
            } => {
                let padding = [0; 7];
                
                let mut  ret = Vec::new();
                ret.extend(timestamp.to_be_bytes());
                ret.extend(padding);
                ret.extend()
                ret
            },
            SystemRegisterCommandContent::WriteProc {
                timestamp,
                write_rank,
                data_to_write,
            } => todo!(),
            SystemRegisterCommandContent::Ack => todo!(),
        }
    }
}

fn get_type(r: &RegisterCommand) -> MsgType {
    fn get_type_client(c: &ClientRegisterCommand) -> MsgType {
        match c.content {
            ClientRegisterCommandContent::Read => constants::TYPE_READ,
            ClientRegisterCommandContent::Write { data: _ } => constants::TYPE_WRITE,
        }
    }

    fn get_type_system(s: &SystemRegisterCommand) -> MsgType {
        match s.content {
            SystemRegisterCommandContent::ReadProc => constants::TYPE_READ_PROC,
            SystemRegisterCommandContent::Value {
                timestamp,
                write_rank,
                sector_data,
            } => constants::TYPE_VALUE,
            SystemRegisterCommandContent::WriteProc {
                timestamp,
                write_rank,
                data_to_write,
            } => constants::TYPE_WRITE,
            SystemRegisterCommandContent::Ack => constants::TYPE_ACK,
        }
    }
    match r {
        RegisterCommand::Client(c) => get_type_client(&c),
        RegisterCommand::System(s) => get_type_system(&s),
    }
}

fn serialize_serializable<T: serde::Serialize>(a: &T) -> Vec<u8> {
    bincode::DefaultOptions::new()
        .with_big_endian()
        .with_fixint_encoding()
        .serialize(a)
        .unwrap()
}

pub async fn serialize_register_command(
    cmd: &RegisterCommand,
    writer: &mut (dyn AsyncWrite + Send + Unpin),
    hmac_key: &[u8],
) -> Result<(), Error> {
    match cmd {
        RegisterCommand::Client(m) => {
            write_client_message(writer, get_type(&m), &m.header, &m.content).await?;
        }
        RegisterCommand::System(m) => {}
    }

    return Ok(());
}

async fn write_client_message<T, U>(
    writer: &mut (dyn AsyncWrite + Send + Unpin),
    msg_type: u8,
    header: &T,
    content: &U,
) -> Result<(), Error>
where
    T: CustomSerializable,
    U: CustomSerializable,
{
    let magic_number: [u8; 4] = [97, 116, 100, 100];
    let padding: [u8; 3] = [0, 0, 0];
    let mut msg: Vec<u8> = vec![];
    msg.extend(magic_number);
    msg.extend(padding);
    msg.extend(msg_type.custom_serialize());
    msg.extend(header.custom_serialize());
    msg.extend(content.custom_serialize());
    writer.write_all(&msg).await
}
