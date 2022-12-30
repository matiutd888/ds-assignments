use std::{vec, convert::TryInto};

use assignment_2_solution::{
    deserialize_register_command, serialize_register_command, ClientCommandHeader,
    ClientRegisterCommand, ClientRegisterCommandContent, RegisterCommand, SystemCommandHeader,
    SystemRegisterCommand, SystemRegisterCommandContent, SectorVec, MAGIC_NUMBER,
};
use ntest::{timeout, assert_false};
use uuid::Uuid;

#[tokio::test]
#[timeout(200)]
async fn serialize_deserialize_is_identity_for_every_message_type() {
    // given
    let request_identifier = rand::random::<u64>();
    let sector_idx = rand::random::<u64>();
    let process_identifier = rand::random::<u8>();
    let read_ident = rand::random::<u64>();

    // when
    let cr_cmd_read = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier,
            sector_idx,
        },
        content: ClientRegisterCommandContent::Read,
    });
    let cr_cmd_write = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier,
            sector_idx,
        },
        content: ClientRegisterCommandContent::Write {
            data: SectorVec([0x00_u8; 4096].iter().map(|_| rand::random::<u8>()).collect::<Vec<u8>>()),
        },
    });
    let sr_cmd_read_proc = RegisterCommand::System(SystemRegisterCommand {
        header: SystemCommandHeader {
            process_identifier: process_identifier,
            msg_ident: Uuid::from_slice(&[0; 16]).unwrap(),
            sector_idx,
            read_ident: read_ident,
        },
        content: SystemRegisterCommandContent::ReadProc,
    }); 
    let sr_cmd_ack = RegisterCommand::System(SystemRegisterCommand {
        header: SystemCommandHeader {
            process_identifier: process_identifier,
            msg_ident: Uuid::from_slice(&[0; 16]).unwrap(),
            sector_idx,
            read_ident: read_ident,
        },
        content: SystemRegisterCommandContent::Ack,
    });
    let sr_cmd_write_proc = RegisterCommand::System(SystemRegisterCommand {
        header: SystemCommandHeader {
            process_identifier: process_identifier,
            msg_ident: Uuid::from_slice(&[0; 16]).unwrap(),
            sector_idx,
            read_ident: read_ident,
        },
        content: SystemRegisterCommandContent::WriteProc {
            timestamp: rand::random::<u64>(),
            write_rank: rand::random::<u8>(),
            data_to_write: SectorVec([0x00_u8; 4096].iter().map(|_| rand::random::<u8>()).collect::<Vec<u8>>()),
        },
    });
    let sr_cmd_value = RegisterCommand::System(SystemRegisterCommand {
        header: SystemCommandHeader {
            process_identifier: process_identifier,
            msg_ident: Uuid::from_slice(&[0; 16]).unwrap(),
            sector_idx,
            read_ident: read_ident,
        },
        content: SystemRegisterCommandContent::Value {
            timestamp: rand::random::<u64>(),
            write_rank: rand::random::<u8>(),
            sector_data: SectorVec([0x00_u8; 4096].iter().map(|_| rand::random::<u8>()).collect::<Vec<u8>>()),
        },
    });
    
    let cmds = vec![
        cr_cmd_read, 
        cr_cmd_write, 
        sr_cmd_read_proc, 
        sr_cmd_ack, 
        sr_cmd_write_proc, 
        sr_cmd_value,
    ];
    for cmd in cmds {
        let mut sink: Vec<u8> = Vec::new();
        serialize_register_command(&cmd, &mut sink, &[0x00_u8; 32])
            .await
            .expect("Could not serialize?");
        let mut slice: &[u8] = &sink[..];
        let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut slice;
        
        let (deserialized_cmd, hmac_valid) =
            deserialize_register_command(data_read, &[0x00_u8; 64], &[0x00_u8; 32])
                .await
                .expect("Could not deserialize");

        // then
        assert!(hmac_valid);
        match (cmd, deserialized_cmd) {
            (RegisterCommand::Client(cr_cmd), RegisterCommand::Client(deserialized_cr_cmd)) => {
                match (cr_cmd, deserialized_cr_cmd) {
                    (ClientRegisterCommand { header: cr_header, content: ClientRegisterCommandContent::Read }, ClientRegisterCommand { header: deserialized_cr_header, content: ClientRegisterCommandContent::Read }) => {
                        assert_eq!(cr_header.request_identifier, deserialized_cr_header.request_identifier);
                        assert_eq!(cr_header.sector_idx, deserialized_cr_header.sector_idx);
                    }
                    (ClientRegisterCommand { header: cr_header, content: ClientRegisterCommandContent::Write { data } }, ClientRegisterCommand { header: deserialized_cr_header, content: ClientRegisterCommandContent::Write { data: deserialized_data } }) => {
                        assert_eq!(cr_header.request_identifier, deserialized_cr_header.request_identifier);
                        assert_eq!(cr_header.sector_idx, deserialized_cr_header.sector_idx);
                        assert_eq!(data, deserialized_data);
                    }
                    _ => panic!("Expected same type"),
                }
            }
            (RegisterCommand::System(sr_cmd), RegisterCommand::System(deserialized_sr_cmd)) => {
                match (sr_cmd, deserialized_sr_cmd) {
                    (SystemRegisterCommand { header: sr_header, content: SystemRegisterCommandContent::ReadProc }, SystemRegisterCommand { header: deserialized_sr_header, content: SystemRegisterCommandContent::ReadProc }) => {
                        assert_eq!(sr_header.process_identifier, deserialized_sr_header.process_identifier);
                        assert_eq!(sr_header.msg_ident, deserialized_sr_header.msg_ident);
                        assert_eq!(sr_header.sector_idx, deserialized_sr_header.sector_idx);
                        assert_eq!(sr_header.read_ident, deserialized_sr_header.read_ident);
                    }
                    (SystemRegisterCommand { header: sr_header, content: SystemRegisterCommandContent::Ack }, SystemRegisterCommand { header: deserialized_sr_header, content: SystemRegisterCommandContent::Ack }) => {
                        assert_eq!(sr_header.process_identifier, deserialized_sr_header.process_identifier);
                        assert_eq!(sr_header.msg_ident, deserialized_sr_header.msg_ident);
                        assert_eq!(sr_header.sector_idx, deserialized_sr_header.sector_idx);
                        assert_eq!(sr_header.read_ident, deserialized_sr_header.read_ident);
                    }
                    (SystemRegisterCommand { header: sr_header, content: SystemRegisterCommandContent::WriteProc { timestamp, write_rank, data_to_write } }, SystemRegisterCommand { header: deserialized_sr_header, content: SystemRegisterCommandContent::WriteProc { timestamp: deserialized_timestamp, write_rank: deserialized_write_rank, data_to_write: deserialized_data_to_write } }) => {
                        assert_eq!(sr_header.process_identifier, deserialized_sr_header.process_identifier);
                        assert_eq!(sr_header.msg_ident, deserialized_sr_header.msg_ident);
                        assert_eq!(sr_header.sector_idx, deserialized_sr_header.sector_idx);
                        assert_eq!(sr_header.read_ident, deserialized_sr_header.read_ident);
                        assert_eq!(timestamp, deserialized_timestamp);
                        assert_eq!(write_rank, deserialized_write_rank);
                        assert_eq!(data_to_write, deserialized_data_to_write);
                    }
                    (SystemRegisterCommand { header: sr_header, content: SystemRegisterCommandContent::Value { timestamp, write_rank, sector_data } }, SystemRegisterCommand { header: deserialized_sr_header, content: SystemRegisterCommandContent::Value { timestamp: deserialized_timestamp, write_rank: deserialized_write_rank, sector_data: deserialized_sector_data } }) => {
                        assert_eq!(sr_header.process_identifier, deserialized_sr_header.process_identifier);
                        assert_eq!(sr_header.msg_ident, deserialized_sr_header.msg_ident);
                        assert_eq!(sr_header.sector_idx, deserialized_sr_header.sector_idx);
                        assert_eq!(sr_header.read_ident, deserialized_sr_header.read_ident);
                        assert_eq!(timestamp, deserialized_timestamp);
                        assert_eq!(write_rank, deserialized_write_rank);
                        assert_eq!(sector_data, deserialized_sector_data);
                    }
                    _ => panic!("Expected same type"),
                }
            }
            _ => panic!("Expected same type"),
        } 
    }
}

#[tokio::test]
#[timeout(200)]
async fn correct_hmac_is_correct_system_write() {
    // given
    let cmd = RegisterCommand::System(SystemRegisterCommand {
        header: SystemCommandHeader {
            process_identifier: rand::random::<u8>(),
            msg_ident: Uuid::from_slice(&[0; 16].map(|_| rand::random::<u8>())).unwrap(),
            sector_idx: rand::random::<u64>(),
            read_ident: rand::random::<u64>(),
        },
        content: SystemRegisterCommandContent::WriteProc {
            timestamp: rand::random::<u64>(),
            write_rank: rand::random::<u8>(),
            data_to_write: SectorVec([0x00_u8; 4096].iter().map(|_| rand::random::<u8>()).collect::<Vec<u8>>()),
        },
    });
    // random 32bit hmack key
    let hmac_key: [u8; 64] = *b"Super secret keyyek terces repuSSuper secret keyyek terces repuS";
    let mut sink: Vec<u8> = Vec::new();
    serialize_register_command(&cmd, &mut sink, &hmac_key)
        .await
        .expect("Could not serialize?");
    let mut slice: &[u8] = &sink[..];
    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut slice;
    
    let (_, hmac_valid) =
        deserialize_register_command(data_read, &hmac_key, &[0x00_u8; 32])
            .await
            .expect("Could not deserialize");

    // then
    assert!(hmac_valid);
}

#[tokio::test]
#[timeout(200)]
async fn incorrect_hmac_is_incorrect_system_write() {
    // given
    let cmd = RegisterCommand::System(SystemRegisterCommand {
        header: SystemCommandHeader {
            process_identifier: rand::random::<u8>(),
            msg_ident: Uuid::from_slice(&[0; 16].map(|_| rand::random::<u8>())).unwrap(),
            sector_idx: rand::random::<u64>(),
            read_ident: rand::random::<u64>(),
        },
        content: SystemRegisterCommandContent::WriteProc {
            timestamp: rand::random::<u64>(),
            write_rank: rand::random::<u8>(),
            data_to_write: SectorVec([0x00_u8; 4096].iter().map(|_| rand::random::<u8>()).collect::<Vec<u8>>()),
        },
    });
    // random 32bit hmack key
    let mut hmac_key: [u8; 64] = *b"Super secret keyyek terces repuSSuper secret keyyek terces repuS";
    let mut sink: Vec<u8> = Vec::new();
    serialize_register_command(&cmd, &mut sink, &hmac_key)
        .await
        .expect("Could not serialize?");
    let mut slice: &[u8] = &sink[..];
    let data_read: &mut (dyn tokio::io::AsyncRead + Send + Unpin) = &mut slice;

    hmac_key = *b"Slightly different secret key. Super secret keyyek terces repuS.";
    let (_, hmac_valid) =
        deserialize_register_command(data_read, &hmac_key, &[0x00_u8; 32])
            .await
            .expect("Could not deserialize");

    // then
    assert_false!(hmac_valid);
}

#[tokio::test]
#[timeout(200)]
async fn all_serialized_have_correct_format() {
    let request_identifier = rand::random::<u64>();
    let sector_idx = rand::random::<u64>();
    let process_identifier = rand::random::<u8>();
    let read_ident = rand::random::<u64>();
    let msg_ident  = Uuid::from_slice(&[0; 16]).unwrap();

    // when
    let cr_cmd_read = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier,
            sector_idx,
        },
        content: ClientRegisterCommandContent::Read,
    });
    let mut sink = Vec::new();
    serialize_register_command(&cr_cmd_read, &mut sink, &[0x00_u8; 64])
        .await
        .expect("Could not serialize?");
    sink.truncate(sink.len() - 32);
    assert_eq!(sink.len(), 24);
    assert_client_cmd_header(sink.as_slice(), request_identifier, sector_idx, 1);


    let cr_cmd_write = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier,
            sector_idx,
        },
        content: ClientRegisterCommandContent::Write {
            data: SectorVec([0x00_u8; 4096].iter().map(|_| rand::random::<u8>()).collect::<Vec<u8>>()),
        },
    });
    let mut sink = Vec::new();
    serialize_register_command(&cr_cmd_write, &mut sink, &[0x00_u8; 64])
        .await
        .expect("Could not serialize?");
    sink.truncate(sink.len() - 32);
    assert_eq!(sink.len(), 4120);
    assert_client_cmd_header(sink.as_slice(), request_identifier, sector_idx, 2);


    let sr_cmd_read_proc = RegisterCommand::System(SystemRegisterCommand {
        header: SystemCommandHeader {
            process_identifier,
            msg_ident,
            sector_idx,
            read_ident,
        },
        content: SystemRegisterCommandContent::ReadProc,
    }); 
    let mut sink = Vec::new();
    serialize_register_command(&sr_cmd_read_proc, &mut sink, &[0x00_u8; 64])
        .await
        .expect("Could not serialize?");
    sink.truncate(sink.len() - 32);
    assert_eq!(sink.len(), 40);
    assert_system_cmd_header(
        sink.as_slice(), 
        msg_ident.as_bytes(),
        process_identifier,
        3,
        read_ident,
        sector_idx,
    );

    let sr_cmd_ack = RegisterCommand::System(SystemRegisterCommand {
        header: SystemCommandHeader {
            process_identifier: process_identifier,
            msg_ident: Uuid::from_slice(&[0; 16]).unwrap(),
            sector_idx,
            read_ident: read_ident,
        },
        content: SystemRegisterCommandContent::Ack,
    });
    let mut sink = Vec::new();
    serialize_register_command(&sr_cmd_ack, &mut sink, &[0x00_u8; 64])
        .await
        .expect("Could not serialize?");
    sink.truncate(sink.len() - 32);
    assert_eq!(sink.len(), 40);
    assert_system_cmd_header(
        sink.as_slice(), 
        msg_ident.as_bytes(),
        process_identifier,
        6,
        read_ident,
        sector_idx,
    );

    let sr_cmd_write_proc = RegisterCommand::System(SystemRegisterCommand {
        header: SystemCommandHeader {
            process_identifier: process_identifier,
            msg_ident: Uuid::from_slice(&[0; 16]).unwrap(),
            sector_idx,
            read_ident: read_ident,
        },
        content: SystemRegisterCommandContent::WriteProc {
            timestamp: rand::random::<u64>(),
            write_rank: rand::random::<u8>(),
            data_to_write: SectorVec([0x00_u8; 4096].iter().map(|_| rand::random::<u8>()).collect::<Vec<u8>>()),
        },
    });
    let mut sink = Vec::new();
    serialize_register_command(&sr_cmd_write_proc, &mut sink, &[0x00_u8; 64])
        .await
        .expect("Could not serialize?");
    sink.truncate(sink.len() - 32);
    assert_eq!(sink.len(), 4152);
    assert_system_cmd_header(
        sink.as_slice(), 
        msg_ident.as_bytes(),
        process_identifier,
        5,
        read_ident,
        sector_idx,
    );

    let sr_cmd_value = RegisterCommand::System(SystemRegisterCommand {
        header: SystemCommandHeader {
            process_identifier: process_identifier,
            msg_ident: Uuid::from_slice(&[0; 16]).unwrap(),
            sector_idx,
            read_ident: read_ident,
        },
        content: SystemRegisterCommandContent::Value {
            timestamp: rand::random::<u64>(),
            write_rank: rand::random::<u8>(),
            sector_data: SectorVec([0x00_u8; 4096].iter().map(|_| rand::random::<u8>()).collect::<Vec<u8>>()),
        },
    });
    let mut sink = Vec::new();
    serialize_register_command(&sr_cmd_value, &mut sink, &[0x00_u8; 64])
        .await
        .expect("Could not serialize?");
    sink.truncate(sink.len() - 32);
    assert_eq!(sink.len(), 4152);
    assert_system_cmd_header(
        sink.as_slice(), 
        msg_ident.as_bytes(),
        process_identifier,
        4,
        read_ident,
        sector_idx,
    );
}

pub fn assert_system_cmd_header(
    serialized: &[u8],
    msg_ident: &[u8; 16],
    process_identifier: u8,
    msg_type: u8,
    read_ident: u64,
    sector_idx: u64,
) {
    assert_eq!(&serialized[0..4], MAGIC_NUMBER.as_ref());
    assert_eq!(*serialized.get(6).unwrap(), process_identifier);
    assert_eq!(*serialized.get(7).unwrap(), msg_type);
    assert_eq!(&serialized[8..24], msg_ident);
    assert_eq!(
        u64::from_be_bytes(serialized[24..32].try_into().unwrap()),
        read_ident
    );
    assert_eq!(
        u64::from_be_bytes(serialized[32..40].try_into().unwrap()),
        sector_idx
    );
}

pub fn assert_client_cmd_header(
    serialized: &[u8],
    request_identifier: u64,
    sector_idx: u64,
    msg_type: u8,
) {
    assert_eq!(&serialized[0..4], MAGIC_NUMBER.as_ref());
    assert_eq!(*serialized.get(7).unwrap(), msg_type);
    assert_eq!(
        u64::from_be_bytes(serialized[8..16].try_into().unwrap()),
        request_identifier
    );
    assert_eq!(
        u64::from_be_bytes(serialized[16..24].try_into().unwrap()),
        sector_idx
    );
}
