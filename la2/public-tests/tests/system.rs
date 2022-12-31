use assignment_2_solution::{
    run_register_process, serialize_register_command, ClientCommandHeader, ClientRegisterCommand,
    ClientRegisterCommandContent, Configuration, PublicConfiguration, RegisterCommand, SectorVec,
    StatusCode, MAGIC_NUMBER,
};
use assignment_2_test_utils::proxy::ProxyConfig;
use assignment_2_test_utils::system::*;
use hmac::{Mac, NewMac};
use ntest::timeout;

use std::convert::TryInto;
use std::time::Duration;
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const START_PORT: u16 = 25000;

#[tokio::test]
#[timeout(4000)]
async fn single_process_system_completes_operations() {
    let _ = env_logger::builder().is_test(true).try_init();
    // given
    let hmac_client_key = [5; 32];
    let tcp_port = START_PORT;
    let storage_dir = tempdir().unwrap();
    let request_identifier = 1778;

    let config = Configuration {
        public: PublicConfiguration {
            tcp_locations: vec![("127.0.0.1".to_string(), tcp_port)],
            self_rank: 1,
            n_sectors: 20,
            storage_dir: storage_dir.into_path(),
        },
        hmac_system_key: [1; 64],
        hmac_client_key,
    };

    tokio::spawn(run_register_process(config));

    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut stream = TcpStream::connect(("127.0.0.1", tcp_port))
        .await
        .expect("Could not connect to TCP port");
    let write_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier,
            sector_idx: 12,
        },
        content: ClientRegisterCommandContent::Write {
            data: SectorVec(vec![3; 4096]),
        },
    });

    // when
    send_cmd(&write_cmd, &mut stream, &hmac_client_key).await;

    // then
    const EXPECTED_RESPONSES_SIZE: usize = 48;
    let mut buf = [0_u8; EXPECTED_RESPONSES_SIZE];
    stream
        .read_exact(&mut buf)
        .await
        .expect("Less data then expected");

    // asserts for write response
    assert_eq!(&buf[0..4], MAGIC_NUMBER.as_ref());
    assert_eq!(buf[7], 0x42);
    assert_eq!(
        u64::from_be_bytes(buf[8..16].try_into().unwrap()),
        request_identifier
    );
    assert!(hmac_tag_is_ok(&hmac_client_key, &buf));
}

#[tokio::test]
#[serial_test::serial]
#[timeout(30000)]
async fn concurrent_operations_on_the_same_sector() {
    let _ = env_logger::builder().is_test(true).try_init();
    // given
    let port_range_start = START_PORT + 20;
    let total_commands = 16;
    let config = TestProcessesConfig::new(1, port_range_start, None);
    config.start().await;
    let mut stream = config.connect(0).await;
    let mut other_stream = config.connect(0).await;

    // when
    for cmd_num in 0..total_commands {
        config
            .send_cmd(
                &RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: cmd_num,
                        sector_idx: 0,
                    },
                    content: ClientRegisterCommandContent::Write {
                        data: SectorVec(vec![1; 4096]),
                    },
                }),
                &mut stream,
            )
            .await;
        config
            .send_cmd(
                &RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: cmd_num + total_commands,
                        sector_idx: 0,
                    },
                    content: ClientRegisterCommandContent::Write {
                        data: SectorVec(vec![254; 4096]),
                    },
                }),
                &mut other_stream,
            )
            .await;
    }

    for _ in 0..total_commands {
        config.read_response(&mut stream).await.unwrap();
        config.read_response(&mut other_stream).await.unwrap();
    }

    config
        .send_cmd(
            &RegisterCommand::Client(ClientRegisterCommand {
                header: ClientCommandHeader {
                    request_identifier: total_commands * 2,
                    sector_idx: 0,
                },
                content: ClientRegisterCommandContent::Read,
            }),
            &mut stream,
        )
        .await;
    let response = config.read_response(&mut stream).await.unwrap();

    match response.content {
        RegisterResponseContent::Read(SectorVec(sector)) => {
            assert!(sector == vec![1; 4096] || sector == vec![254; 4096]);
        }
        _ => panic!("Expected read response"),
    }
}

#[tokio::test]
#[serial_test::serial]
#[timeout(40000)]
async fn large_number_of_operations_execute_successfully() {
    let _ = env_logger::builder().is_test(true).try_init();
    // given
    let port_range_start = START_PORT + 40;
    let commands_total = 32;
    let config = TestProcessesConfig::new(3, port_range_start, None);
    config.start().await;
    let mut stream = config.connect(2).await;

    test_operations(&config, &mut stream, commands_total).await;
}

#[tokio::test]
#[serial_test::serial]
#[timeout(40000)]
async fn too_large_sector_idx_is_rejected() {
    let _ = env_logger::builder().is_test(true).try_init();
    // given
    let port_range_start = START_PORT + 60;
    let config = TestProcessesConfig::new(1, port_range_start, None);
    config.start().await;
    let mut stream = config.connect(0).await;

    config
        .send_cmd(
            &RegisterCommand::Client(ClientRegisterCommand {
                header: ClientCommandHeader {
                    request_identifier: 0,
                    sector_idx: TestProcessesConfig::MAX_SECTOR - 1,
                },
                content: ClientRegisterCommandContent::Write {
                    data: SectorVec(vec![0 as u8; 4096]),
                },
            }),
            &mut stream,
        )
        .await;

    config
        .send_cmd(
            &RegisterCommand::Client(ClientRegisterCommand {
                header: ClientCommandHeader {
                    request_identifier: 1,
                    sector_idx: TestProcessesConfig::MAX_SECTOR,
                },
                content: ClientRegisterCommandContent::Write {
                    data: SectorVec(vec![0 as u8; 4096]),
                },
            }),
            &mut stream,
        )
        .await;
    config
        .send_cmd(
            &RegisterCommand::Client(ClientRegisterCommand {
                header: ClientCommandHeader {
                    request_identifier: 2,
                    sector_idx: TestProcessesConfig::MAX_SECTOR + 1,
                },
                content: ClientRegisterCommandContent::Write {
                    data: SectorVec(vec![0 as u8; 4096]),
                },
            }),
            &mut stream,
        )
        .await;

    for _ in 0..3 {
        let result = config.read_response(&mut stream).await.unwrap();
        match result.header.request_identifier {
            1 | 2 => assert_eq!(result.header.status_code, StatusCode::InvalidSectorIndex),
            0 => assert_eq!(result.header.status_code, StatusCode::Ok),
            _ => panic!("Unexpected status code"),
        }
    }
}

#[tokio::test]
#[timeout(40000)]
async fn space_taken_within_limit() {
    let _ = env_logger::builder().is_test(true).try_init();
    // given
    let port_range_start = START_PORT + 80;
    let commands_total = 2000;
    assert!(TestProcessesConfig::MAX_SECTOR >= 1000);
    let config = TestProcessesConfig::new(1, port_range_start, None);
    config.start().await;
    let mut stream = config.connect(0).await;

    for cmd_idx in 0..commands_total {
        config
            .send_cmd(
                &RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: cmd_idx,
                        sector_idx: cmd_idx,
                    },
                    content: ClientRegisterCommandContent::Write {
                        data: SectorVec(vec![cmd_idx as u8; 4096]),
                    },
                }),
                &mut stream,
            )
            .await;
    }

    for i in 0..commands_total {
        let response = config.read_response(&mut stream).await.unwrap();
        println!("{}: {:?}", i, response.header);
    }

    
    let size = config.get_dir_size(0).await;
    println!("size is equal to {}", size);
    println!("{}", (1.1 * (commands_total * 4096) as f64) as usize);
    assert!(size > (commands_total * 4096) as usize);
    assert!(size <= (1.1 * (commands_total * 4096) as f64) as usize);
}

#[tokio::test]
#[timeout(40000)]
async fn works_with_proxy() {
    let _ = env_logger::builder().is_test(true).try_init();
    // given
    let port_range_start = START_PORT + 100;
    let commands_total = 3;
    let config = TestProcessesConfig::new(
        5,
        port_range_start,
        Some(ProxyConfig {
            latency: Duration::new(0, 0),
            drop_chance: 0.0,
            corrupt_chance: 0.0,
        }),
    );
    config.start().await;
    let mut stream = config.connect(0).await;

    test_operations(&config, &mut stream, commands_total).await;
}

#[tokio::test]
#[timeout(40000)]
async fn works_with_majority() {
    let _ = env_logger::builder().is_test(true).try_init();
    // given
    let port_range_start = START_PORT + 120;
    let commands_total = 3;
    let config = TestProcessesConfig::new(
        5,
        port_range_start,
        Some(ProxyConfig {
            latency: Duration::new(0, 0),
            drop_chance: 0.0,
            corrupt_chance: 0.0,
        }),
    );
    config.start().await;
    let mut stream = config.connect(0).await;

    config.disable(4);
    config.disable(3);

    test_operations(&config, &mut stream, commands_total).await;
}

#[tokio::test]
#[timeout(40000)]
async fn hangs_with_minority() {
    let _ = env_logger::builder().is_test(true).try_init();
    // given
    let port_range_start = START_PORT + 140;
    let commands_total = 3;
    let config = TestProcessesConfig::new(
        5,
        port_range_start,
        Some(ProxyConfig {
            latency: Duration::new(0, 0),
            drop_chance: 0.0,
            corrupt_chance: 0.0,
        }),
    );
    config.start().await;
    let mut stream = config.connect(0).await;

    config.disable(4);
    config.disable(3);
    config.disable(2);

    match tokio::time::timeout(
        Duration::new(2, 0),
        test_operations(&config, &mut stream, commands_total),
    )
    .await
    {
        Ok(_) => panic!("Operations shouldnt succeed when only minority of registers work"),
        Err(_) => (),
    }
}

#[tokio::test]
#[timeout(40000)]
async fn works_after_recover() {
    let _ = env_logger::builder().is_test(true).try_init();
    // given
    let port_range_start = START_PORT + 160;
    let commands_total = 3;
    let config = TestProcessesConfig::new(
        5,
        port_range_start,
        Some(ProxyConfig {
            latency: Duration::new(0, 0),
            drop_chance: 0.0,
            corrupt_chance: 0.0,
        }),
    );
    config.start().await;
    let mut stream = config.connect(0).await;

    config.disable(4);
    config.disable(3);
    config.disable(2);

    for cmd_idx in 0..commands_total {
        config
            .send_cmd(
                &RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: cmd_idx,
                        sector_idx: cmd_idx,
                    },
                    content: ClientRegisterCommandContent::Write {
                        data: SectorVec(vec![cmd_idx as u8; 4096]),
                    },
                }),
                &mut stream,
            )
            .await;
    }

    tokio::time::sleep(Duration::new(2, 0)).await;
    config.enable(2);

    for _ in 0..commands_total {
        config.read_response(&mut stream).await.unwrap();
    }

    config.disable(2);

    for cmd_idx in 0..commands_total {
        config
            .send_cmd(
                &RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: cmd_idx + commands_total,
                        sector_idx: cmd_idx,
                    },
                    content: ClientRegisterCommandContent::Read,
                }),
                &mut stream,
            )
            .await;
    }

    tokio::time::sleep(Duration::new(2, 0)).await;
    config.enable(2);

    for _ in 0..commands_total {
        let response = config.read_response(&mut stream).await.unwrap();
        match response.content {
            RegisterResponseContent::Read(SectorVec(sector)) => {
                assert_eq!(
                    sector,
                    vec![(response.header.request_identifier - commands_total) as u8; 4096]
                )
            }
            _ => panic!("Expected read response"),
        }
    }
}

#[tokio::test]
#[timeout(40000)]
async fn works_with_big_ping() {
    let _ = env_logger::builder().is_test(true).try_init();
    // given
    let port_range_start = START_PORT + 180;
    let commands_total = 10;
    let config = TestProcessesConfig::new(
        5,
        port_range_start,
        Some(ProxyConfig {
            latency: Duration::new(0, 150000000),
            drop_chance: 0.0,
            corrupt_chance: 0.0,
        }),
    );
    config.start().await;
    let mut stream = config.connect(0).await;

    test_operations(&config, &mut stream, commands_total).await;
}

#[tokio::test]
#[timeout(40000)]
async fn works_with_dropping_packets() {
    let _ = env_logger::builder().is_test(true).try_init();
    // given
    let port_range_start = START_PORT + 200;
    let commands_total = 10;
    let config = TestProcessesConfig::new(
        5,
        port_range_start,
        Some(ProxyConfig {
            latency: Duration::new(0, 0),
            drop_chance: 0.15,
            corrupt_chance: 0.0,
        }),
    );
    config.start().await;
    let mut stream = config.connect(0).await;

    test_operations(&config, &mut stream, commands_total).await;
}

#[tokio::test]
#[timeout(40000)]
async fn works_with_corrupt_packets() {
    let _ = env_logger::builder().is_test(true).try_init();
    // given
    let port_range_start = START_PORT + 220;
    let commands_total = 10;
    let config = TestProcessesConfig::new(
        5,
        port_range_start,
        Some(ProxyConfig {
            latency: Duration::new(0, 0),
            drop_chance: 0.0,
            corrupt_chance: 0.15,
        }),
    );
    config.start().await;
    let mut stream = config.connect(0).await;

    test_operations(&config, &mut stream, commands_total).await;
}

async fn test_operations(
    config: &TestProcessesConfig,
    stream: &mut TcpStream,
    commands_total: u64,
) {
    assert!(commands_total <= TestProcessesConfig::MAX_SECTOR);
    for cmd_idx in 0..commands_total {
        config
            .send_cmd(
                &RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: cmd_idx,
                        sector_idx: cmd_idx,
                    },
                    content: ClientRegisterCommandContent::Write {
                        data: SectorVec(vec![cmd_idx as u8; 4096]),
                    },
                }),
                stream,
            )
            .await;
    }

    for _ in 0..commands_total {
        config.read_response(stream).await.unwrap();
    }

    // when
    for cmd_idx in 0..commands_total {
        config
            .send_cmd(
                &RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: cmd_idx + commands_total,
                        sector_idx: cmd_idx,
                    },
                    content: ClientRegisterCommandContent::Read,
                }),
                stream,
            )
            .await;
    }

    // then
    for _ in 0..commands_total {
        let response = config.read_response(stream).await.unwrap();
        match response.content {
            RegisterResponseContent::Read(SectorVec(sector)) => {
                assert_eq!(
                    sector,
                    vec![(response.header.request_identifier - commands_total) as u8; 4096]
                )
            }
            _ => panic!("Expected read response"),
        }
    }
}

async fn send_cmd(register_cmd: &RegisterCommand, stream: &mut TcpStream, hmac_client_key: &[u8]) {
    let mut data = Vec::new();
    serialize_register_command(register_cmd, &mut data, hmac_client_key)
        .await
        .unwrap();

    stream.write_all(&data).await.unwrap();
}

fn hmac_tag_is_ok(key: &[u8], data: &[u8]) -> bool {
    let boundary = data.len() - HMAC_TAG_SIZE;
    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(&data[..boundary]);
    mac.verify(&data[boundary..]).is_ok()
}
