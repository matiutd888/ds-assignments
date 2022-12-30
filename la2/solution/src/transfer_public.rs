use crate::{
    constants::{self, MsgType, SECTOR_SIZE_BYTES},
    ClientCommandHeader, ClientRegisterCommand, ClientRegisterCommandContent, OperationReturn,
    OperationSuccess, RegisterCommand, SectorVec, StatusCode, SystemCommandHeader,
    SystemRegisterCommand, SystemRegisterCommandContent, Timestamp, WriteRank, MAGIC_NUMBER,
};

use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::{
    io::{Error, ErrorKind},
    vec,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

pub async fn deserialize_register_command(
    data: &mut (dyn AsyncRead + Send + Unpin),
    hmac_system_key: &[u8; 64],
    hmac_client_key: &[u8; 32],
) -> Result<(RegisterCommand, bool), Error> {
    async fn read_hmac_tag(
        async_read: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<[u8; 32], Error> {
        let mut hmac_buffer: [u8; 32] = [0; 32];
        async_read.read_exact(&mut hmac_buffer).await?;
        Ok(hmac_buffer)
    }

    loop {
        read_magic_number(data).await?;
        log::debug!("magic number read");
        let mut pre_type_bytes: [u8; 3] = [0; 3];
        data.read_exact(&mut pre_type_bytes).await?;
        let msg_type = data.read_u8().await?;

        let mut initial_content = Vec::new();
        initial_content.extend(&MAGIC_NUMBER);
        initial_content.extend(&pre_type_bytes);
        initial_content.push(msg_type);

        match msg_type {
            0x1 | 0x2 => {
                let mut c = ClientCommandReader {
                    msg_type,
                    content: initial_content,
                };
                let command = RegisterCommand::Client(c.read_client_command(data).await?);
                let tag = read_hmac_tag(data).await?;
                return Ok((command, verify_hmac_tag(&tag, &c.content, hmac_client_key)));
            }
            0x3..=0x6 => {
                let mut s = SystemCommandReader {
                    msg_type,
                    process_rank: pre_type_bytes[2],
                    content: initial_content,
                };
                let command = RegisterCommand::System(s.read_system_command(data).await?);
                let tag = read_hmac_tag(data).await?;
                return Ok((command, verify_hmac_tag(&tag, &s.content, hmac_system_key)));
            }
            _ => {
                log::debug!("Unexpected message type! {}", msg_type);
            }
        };
    }
}

struct ClientCommandReader {
    msg_type: u8,
    content: Vec<u8>,
}

impl ClientCommandReader {
    pub async fn read_client_command(
        &mut self,
        async_read: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<ClientRegisterCommand, Error> {
        let header: ClientCommandHeader = self.read_header(async_read).await?;
        let content: ClientRegisterCommandContent = self.read_content(async_read).await?;
        Ok(ClientRegisterCommand { header, content })
    }

    async fn read_header(
        &mut self,
        async_read: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<ClientCommandHeader, Error> {
        let mut header_buf: [u8; 16] = [0; 16];
        async_read.read_exact(&mut header_buf).await?;

        let request_number = u64::custom_deserialize(&header_buf[0..8])?;
        let sector_index = u64::custom_deserialize(&header_buf[8..])?;
        self.content.extend(header_buf);
        Ok(ClientCommandHeader {
            request_identifier: request_number,
            sector_idx: sector_index,
        })
    }

    async fn read_nonempty_content(
        &mut self,
        async_read: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<SectorVec, Error> {
        let mut content_buf: [u8; constants::SECTOR_SIZE_BYTES] = [0; constants::SECTOR_SIZE_BYTES];

        async_read.read_exact(&mut content_buf).await?;

        let sector_vec: SectorVec = SectorVec(Vec::from(content_buf.clone()));
        assert!(sector_vec.0.len() == constants::SECTOR_SIZE_BYTES);

        self.content.extend(content_buf);
        Ok(sector_vec)
    }

    async fn read_content(
        &mut self,
        async_read: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<ClientRegisterCommandContent, Error> {
        match self.msg_type {
            constants::TYPE_READ => Ok(ClientRegisterCommandContent::Read),
            constants::TYPE_WRITE => Ok(ClientRegisterCommandContent::Write {
                data: self.read_nonempty_content(async_read).await?,
            }),
            _ => Err(Error::new(ErrorKind::Other, "Invalid message type")),
        }
    }
}

struct SystemCommandReader {
    msg_type: u8,
    process_rank: u8,
    content: Vec<u8>,
}

fn verify_hmac_tag(tag: &[u8], content: &Vec<u8>, secret_key: &[u8]) -> bool {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret_key).unwrap();
    mac.update(content);
    mac.verify_slice(tag).is_ok()
}

impl SystemCommandReader {
    pub async fn read_system_command(
        &mut self,
        async_read: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<SystemRegisterCommand, Error> {
        let header: SystemCommandHeader = self.read_header(async_read).await?;
        let content: SystemRegisterCommandContent = self.read_content(async_read).await?;
        Ok(SystemRegisterCommand { header, content })
    }

    async fn read_header(
        &mut self,
        async_read: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<SystemCommandHeader, Error> {
        let mut header_buf: [u8; 32] = [0; 32];

        async_read.read(&mut header_buf).await?;
        let uuid = Uuid::custom_deserialize(&header_buf[0..16])?;
        let rid = u64::custom_deserialize(&header_buf[16..24])?;
        let sector_idx = u64::custom_deserialize(&header_buf[24..32])?;

        self.content.extend(header_buf);
        Ok(SystemCommandHeader {
            process_identifier: self.process_rank,
            msg_ident: uuid,
            read_ident: rid,
            sector_idx: sector_idx,
        })
    }

    async fn read_nonempty_content(
        &mut self,
        async_read: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<(Timestamp, WriteRank, SectorVec), Error> {
        const CONTENT_BUF_SIZE: usize = 8 + 7 + 1 + constants::SECTOR_SIZE_BYTES;
        let mut content_buf: [u8; CONTENT_BUF_SIZE] = [0; CONTENT_BUF_SIZE];

        async_read.read_exact(&mut content_buf).await?;

        let timestamp = Timestamp::custom_deserialize(&content_buf[0..8])?;
        let write_rank = u8::custom_deserialize(&content_buf[15..16])?;

        let mut sector_data: [u8; SECTOR_SIZE_BYTES] = [0; SECTOR_SIZE_BYTES];
        sector_data.clone_from_slice(&content_buf[16..(16 + SECTOR_SIZE_BYTES)]);

        self.content.extend(content_buf);
        Ok((timestamp, write_rank, SectorVec(Vec::from(sector_data))))
    }

    async fn read_content(
        &mut self,
        async_read: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<SystemRegisterCommandContent, Error> {
        match self.msg_type {
            constants::TYPE_READ_PROC => Ok(SystemRegisterCommandContent::ReadProc),
            constants::TYPE_VALUE => {
                let (ts, wr, v) = self.read_nonempty_content(async_read).await?;
                Ok(SystemRegisterCommandContent::Value {
                    timestamp: ts,
                    write_rank: wr,
                    sector_data: v,
                })
            }
            constants::TYPE_WRITE_PROC => {
                let (ts, wr, v) = self.read_nonempty_content(async_read).await?;
                Ok(SystemRegisterCommandContent::WriteProc {
                    timestamp: ts,
                    write_rank: wr,
                    data_to_write: v,
                })
            }
            constants::TYPE_ACK => Ok(SystemRegisterCommandContent::Ack),
            _ => Err(Error::new(ErrorKind::Other, "Invalid message type")),
        }
    }
}

trait CustomDeserialize<T> {
    fn custom_deserialize(data: &[u8]) -> Result<T, Error>;
}

impl CustomDeserialize<u8> for u8 {
    fn custom_deserialize(data: &[u8]) -> Result<u8, Error> {
        assert!(data.len() == 1);
        Ok(data[0])
    }
}

impl CustomDeserialize<u64> for u64 {
    fn custom_deserialize(data: &[u8]) -> Result<u64, Error> {
        assert!(data.len() == 8);
        Ok(u64::from_be_bytes(data[0..8].try_into().unwrap()))
    }
}

impl CustomDeserialize<Uuid> for Uuid {
    fn custom_deserialize(data: &[u8]) -> Result<Uuid, Error> {
        Ok(Uuid::from_bytes(data.try_into().unwrap()))
    }
}

async fn read_magic_number(data: &mut (dyn AsyncRead + Send + Unpin)) -> Result<(), Error> {
    const N: usize = 4;

    let expected_bytes: [u8; N] = MAGIC_NUMBER;

    let mut buf: [u8; N] = [0; N];
    let mut index = 0;
    loop {
        if index == N {
            return Ok(());
        }
        data.read_exact(&mut buf[index..(index + 1)]).await?;
        index = if buf[index] == expected_bytes[index] {
            index + 1
        } else if buf[index] == expected_bytes[0] {
            1
        } else {
            0
        };
    }
}

trait CustomSerializable {
    fn custom_serialize(&self, buffer: Vec<u8>) -> Vec<u8>;
}

impl CustomSerializable for u8 {
    fn custom_serialize(&self, mut buffer: Vec<u8>) -> Vec<u8> {
        buffer.extend(self.to_be_bytes());
        buffer
    }
}

impl CustomSerializable for u64 {
    fn custom_serialize(&self, mut buffer: Vec<u8>) -> Vec<u8> {
        buffer.extend(self.to_be_bytes());
        buffer
    }
}

impl CustomSerializable for Uuid {
    fn custom_serialize(&self, mut buffer: Vec<u8>) -> Vec<u8> {
        buffer.extend(self.as_bytes());
        buffer
    }
}

impl CustomSerializable for ClientRegisterCommandContent {
    fn custom_serialize(&self, buffer: Vec<u8>) -> Vec<u8> {
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
        buffer.extend(&self.0);
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
                let padding = [0; 7];

                buffer = timestamp.custom_serialize(buffer);
                buffer.extend(padding);
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
        buffer = self.read_ident.custom_serialize(buffer);
        buffer = self.sector_idx.custom_serialize(buffer);
        buffer
    }
}

impl CustomSerializable for ClientCommandHeader {
    fn custom_serialize(&self, mut buffer: Vec<u8>) -> Vec<u8> {
        buffer = self.request_identifier.custom_serialize(buffer);
        buffer = self.sector_idx.custom_serialize(buffer);
        buffer
    }
}

pub fn get_type_client(c: &ClientRegisterCommand) -> MsgType {
    match c.content {
        ClientRegisterCommandContent::Read => constants::TYPE_READ,
        ClientRegisterCommandContent::Write { .. } => constants::TYPE_WRITE,
    }
}

pub fn get_type_system(s: &SystemRegisterCommand) -> MsgType {
    match s.content {
        SystemRegisterCommandContent::ReadProc => constants::TYPE_READ_PROC,
        SystemRegisterCommandContent::Value { .. } => constants::TYPE_VALUE,
        SystemRegisterCommandContent::WriteProc { .. } => constants::TYPE_WRITE_PROC,
        SystemRegisterCommandContent::Ack => constants::TYPE_ACK,
    }
}

pub fn get_type(r: &RegisterCommand) -> MsgType {
    match r {
        RegisterCommand::Client(c) => get_type_client(&c),
        RegisterCommand::System(s) => get_type_system(&s),
    }
}

pub async fn serialize_register_command(
    cmd: &RegisterCommand,
    writer: &mut (dyn AsyncWrite + Send + Unpin),
    hmac_key: &[u8],
) -> Result<(), Error> {
    match cmd {
        RegisterCommand::Client(c) => {
            write_command(
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
            pre_header.extend(padding);
            pre_header.extend(s.header.process_identifier.to_be_bytes());

            write_command(
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

async fn write_command<T, U>(
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

    writer.write_all(&msg).await
}

#[derive(Debug)]
enum OperationContent {
    Success(OperationReturn),
    Failed,
}

#[derive(Debug)]
enum ClientOperationType {
    Write,
    Read,
}

impl ClientOperationType {
    fn from_type(t: u8) -> Self {
        match t {
            constants::TYPE_READ => ClientOperationType::Read,
            constants::TYPE_WRITE => ClientOperationType::Write,
            _ => panic!("Invalid client operation type"),
        }
    }

    fn to_type(&self) -> u8 {
        match &self {
            ClientOperationType::Write => constants::TYPE_WRITE,
            ClientOperationType::Read => constants::TYPE_READ,
        }
    }
}

#[derive(Debug)]
pub struct ClientCommandResponseTransfer {
    status_code: StatusCode,
    msg_type: ClientOperationType,
    request_number: u64,
    content: OperationContent,
}

impl CustomSerializable for StatusCode {
    fn custom_serialize(&self, buffer: Vec<u8>) -> Vec<u8> {
        let val: u8 = match &self {
            StatusCode::Ok => 0x0,
            StatusCode::AuthFailure => 0x1,
            StatusCode::InvalidSectorIndex => 0x2,
        };
        val.custom_serialize(buffer)
    }
}

impl CustomSerializable for ClientOperationType {
    fn custom_serialize(&self, buffer: Vec<u8>) -> Vec<u8> {
        let t = Self::to_type(&self);
        (t + 0x40).custom_serialize(buffer)
    }
}

impl CustomSerializable for OperationContent {
    fn custom_serialize(&self, buffer: Vec<u8>) -> Vec<u8> {
        match &self {
            OperationContent::Success(op) => match op {
                OperationReturn::Read(r) => r.read_data.custom_serialize(buffer),
                OperationReturn::Write => buffer,
            },
            OperationContent::Failed => buffer,
        }
    }
}

pub async fn serialize_client_response(
    c: &ClientCommandResponseTransfer,
    writer: &mut (dyn AsyncWrite + Send + Unpin),
    hmac_client_key: &[u8; 32],
) -> Result<(), Error> {
    let mut buffer: Vec<u8> = Vec::new();
    buffer.extend(&MAGIC_NUMBER);
    buffer.extend([0; 2]);
    buffer = c.status_code.custom_serialize(buffer);
    buffer = c.msg_type.custom_serialize(buffer);
    buffer = c.request_number.custom_serialize(buffer);
    buffer = c.content.custom_serialize(buffer);
    buffer.extend(calculate_hmac_tag(&buffer, hmac_client_key));
    writer.write_all(&buffer).await?;
    Ok(())
}

impl ClientCommandResponseTransfer {
    fn get_type(op: &OperationReturn) -> ClientOperationType {
        match op {
            OperationReturn::Read(_) => ClientOperationType::Read,
            OperationReturn::Write => ClientOperationType::Write,
        }
    }

    pub fn from_success(op: OperationSuccess) -> Self {
        ClientCommandResponseTransfer {
            status_code: StatusCode::Ok,
            msg_type: Self::get_type(&op.op_return),
            request_number: op.request_identifier,
            content: OperationContent::Success(op.op_return),
        }
    }

    pub fn from_invalid_sector_id(c: &ClientRegisterCommand) -> Self {
        ClientCommandResponseTransfer {
            status_code: StatusCode::InvalidSectorIndex,
            msg_type: ClientOperationType::from_type(get_type_client(c)),
            request_number: c.header.request_identifier,
            content: OperationContent::Failed,
        }
    }

    pub fn from_invalid_hmac(c: &ClientRegisterCommand) -> Self {
        ClientCommandResponseTransfer {
            status_code: StatusCode::AuthFailure,
            msg_type: ClientOperationType::from_type(get_type_client(c)),
            request_number: c.header.request_identifier,
            content: OperationContent::Failed,
        }
    }
}
