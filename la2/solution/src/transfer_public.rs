use crate::{
    constants::{self, MsgType},
    ClientRegisterCommand, ClientRegisterCommandContent, RegisterCommand, SectorVec,
    SystemCommandHeader, SystemRegisterCommand, SystemRegisterCommandContent, MAGIC_NUMBER,
};
use bincode::Options;

use bytes::Buf;
use hmac::{Hmac, Mac};
use serde::Serialize;
use sha2::Sha256;
use std::io::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub async fn deserialize_register_command(
    data: &mut (dyn AsyncRead + Send + Unpin),
    _hmac_system_key: &[u8; 64],
    _hmac_client_key: &[u8; 32],
) -> Result<(RegisterCommand, bool), Error> {
    
    read_magic_number(data).await?;

    todo!()
    // return Err(Error::new(ErrorKind::Other, "oh no!"));
}


// This function is terrible :)
async fn read_magic_number(
    data: &mut (dyn AsyncRead + Send + Unpin),
) -> Result<(), Error> {
    let expected_bytes: [u8; 4] = MAGIC_NUMBER;
    let find_next_index_value = |buf: &mut [u8; 4], matching: usize| -> usize {
        let start_index_in_buf = if expected_bytes[0] == buf[3] {
            3
        } else if matching < 3 && buf[2] == expected_bytes[0] {
            2
        } else if matching < 2 && buf[1] == expected_bytes[0] {
            1
        } else {
            0
        };

        let sequence_end_in_expected = expected_bytes.len() - start_index_in_buf;
        let index = if start_index_in_buf > 0 && expected_bytes[1..sequence_end_in_expected].eq(&buf[(start_index_in_buf + 1)..]) {
            sequence_end_in_expected           
        } else {
            0
        };
    
        buf[0..index].copy_from_slice(&expected_bytes[0..index]);
        index
    };


    let mut buf: [u8; 4] = [0; 4];
    let index = 0;
    loop {
        data.read_exact(&mut buf[index..]).await?;
        let mut matching = index;
        while matching < 4 && expected_bytes[matching] == buf[index] {
            matching = matching + 1;
        }
        if matching == 4 {
            return Ok(());
        }
        find_next_index_value(&mut buf, matching);
    }
}

// fn read_until_4_bytes<R: Read>(reader: &mut R) -> io::Result<()> {
//     let target = [0x61, 0x74, 0x64, 0x64];
//     let mut buffer = [0; 4];
//     let mut bytes_read = 0;
//     let mut index = 0;
//     while index < 4 {
//         let n = reader.read(&mut buffer[bytes_read..])?;
//         if n == 0 {
//             return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "failed to read 4 bytes"));
//         }
//         bytes_read += n;
//         while index < 4 && buffer[index] == target[index] {
//             index += 1;
//         }
//         if index < 4 {
//             let mut skip = 0;
//             while skip < bytes_read && buffer[skip] != target[0] {
//                 skip += 1;
//             }
//             buffer[0] = buffer[skip];
//             bytes_read -= skip;
//             index = 0;
//         }
//     }
//     Ok(())
// }


///////////////////////

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
            ClientRegisterCommandContent::Write { data } => data.custom_serialize(buffer),
        }
    }
}

impl CustomSerializable for SectorVec {
    fn custom_serialize(&self, mut buffer: Vec<u8>) -> Vec<u8> {
        assert!(
            self.0.len() == constants::SECTOR_SIZE_BYTES,
            "Data length should be equal to sector size!"
        );
        buffer.extend(self.0.clone());
        buffer
    }
}

impl CustomSerializable for SystemRegisterCommandContent {
    fn custom_serialize(&self, mut buffer: Vec<u8>) -> Vec<u8> {
        match self {
            SystemRegisterCommandContent::ReadProc => buffer,
            SystemRegisterCommandContent::Value {
                timestamp,
                write_rank,
                sector_data,
            } => {
                let padding = [0; 7];

                buffer = timestamp.custom_serialize(buffer);
                buffer.extend(padding);
                buffer = write_rank.custom_serialize(buffer);
                buffer = sector_data.custom_serialize(buffer);
                buffer
            }
            SystemRegisterCommandContent::WriteProc {
                timestamp,
                write_rank,
                data_to_write,
            } => {
                buffer = timestamp.custom_serialize(buffer);
                buffer = write_rank.custom_serialize(buffer);
                buffer = data_to_write.custom_serialize(buffer);
                buffer
            }
            SystemRegisterCommandContent::Ack => buffer,
        }
    }
}

impl CustomSerializable for SystemCommandHeader {
    fn custom_serialize(&self, mut buffer: Vec<u8>) -> Vec<u8> {
        buffer = self.msg_ident.custom_serialize(buffer);
        buffer = self.msg_ident.custom_serialize(buffer);
        buffer = self.read_ident.custom_serialize(buffer);
        buffer = self.sector_idx.custom_serialize(buffer);
        buffer
    }
}

fn get_type(r: &RegisterCommand) -> MsgType {
    fn get_type_client(c: &ClientRegisterCommand) -> MsgType {
        match c.content {
            ClientRegisterCommandContent::Read => constants::TYPE_READ,
            ClientRegisterCommandContent::Write { .. } => constants::TYPE_WRITE,
        }
    }

    fn get_type_system(s: &SystemRegisterCommand) -> MsgType {
        match s.content {
            SystemRegisterCommandContent::ReadProc => constants::TYPE_READ_PROC,
            SystemRegisterCommandContent::Value { .. } => constants::TYPE_VALUE,
            SystemRegisterCommandContent::WriteProc { .. } => constants::TYPE_WRITE,
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

// fn get_serializer() -> bincode::config::WithOtherIntEncoding<
//     bincode::config::WithOtherEndian<bincode::DefaultOptions, bincode::config::BigEndian>,
//     bincode::config::FixintEncoding,
// > {
//     bincode::DefaultOptions::new()
//         .with_big_endian()
//         .with_fixint_encoding()
// }

pub async fn serialize_register_command(
    cmd: &RegisterCommand,
    writer: &mut (dyn AsyncWrite + Send + Unpin),
    hmac_key: &[u8],
) -> Result<(), Error> {
    match cmd {
        RegisterCommand::Client(c) => {
            write_client_message(
                writer,
                [0; 3].to_vec(),
                get_type(cmd),
                &c.header,
                &c.content,
                &hmac_key,
            )
            .await
        }

        RegisterCommand::System(s) => {
            let padding: Vec<u8> = [0; 2].to_vec();
            let mut pre_header = Vec::new();
            pre_header.extend(s.header.process_identifier.to_be_bytes());
            pre_header.extend(padding);

            write_client_message(
                writer,
                pre_header,
                get_type(cmd),
                &s.header,
                &s.content,
                &hmac_key,
            )
            .await
        }
    }
}

fn calculate_hmac_tag(data: &[u8], secret_key: &[u8]) -> [u8; 32] {
    // Initialize a new MAC instance from the secret key:
    let mut mac = Hmac::<Sha256>::new_from_slice(secret_key).unwrap();
    mac.update(data);
    let tag = mac.finalize().into_bytes();
    tag.into()
}

async fn write_client_message<T, U>(
    writer: &mut (dyn AsyncWrite + Send + Unpin),
    pre_header: Vec<u8>, // padding plus process rank
    msg_type: u8,
    header: &T,
    content: &U,
    hmac_key: &[u8],
) -> Result<(), Error>
where
    T: CustomSerializable,
    U: CustomSerializable,
{
    let mut msg: Vec<u8> = vec![];
    msg.extend(MAGIC_NUMBER);
    msg.extend(pre_header);
    msg = msg_type.custom_serialize(msg);
    msg = header.custom_serialize(msg);
    msg = content.custom_serialize(msg);
    let tag = calculate_hmac_tag(&msg, &hmac_key);
    msg.extend(tag);
    msg.extend(tag);
    msg.extend(tag);
    msg.extend(tag);

    writer.write_all(&msg).await
}

// async fn write_all_2<T: Serialize>(
//     writer: &mut (dyn AsyncWrite + Send + Unpin),
//     content: &T,
//     hmac_key: &[u8],
// ) -> Result<(), Error> {
//     let serialized: Vec<u8> = get_serializer().serialize(content).unwrap();
//     writer.write_all(&serialized).await
// }
