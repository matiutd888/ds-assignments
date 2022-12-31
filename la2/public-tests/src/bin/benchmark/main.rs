use std::{convert::TryInto, mem, sync::Arc, time::Duration};

use assignment_2_solution::{
    ClientCommandHeader, ClientRegisterCommand, ClientRegisterCommandContent, RegisterCommand,
    SectorVec,
};
use assignment_2_test_utils::{proxy::ProxyConfig, system::*};
use structopt::StructOpt;
use tokio::{
    sync::{Mutex, Semaphore},
    time::Instant,
};

#[derive(Debug, StructOpt)]
#[structopt(
    name = "atomic_register_benchmark",
    about = "Simple benchmark of atomic register"
)]
struct Opt {
    #[structopt(long = "concurrency", default_value = "64")]
    concurrent_requests: u64,

    #[structopt(short = "c", long = "commands", default_value = "1024")]
    commands: u64,

    #[structopt(short = "i", long = "instances", default_value = "4")]
    instances: u8,

    #[structopt(long = "proxy")]
    proxy: bool,

    // In milliseconds. Requires --proxy
    #[structopt(long = "ping", default_value = "0")]
    ping: u64,

    // Between 0 and 1
    #[structopt(long = "drop_chance", default_value = "0.0")]
    drop_chance: f32,

    // Between 0 and 1
    #[structopt(long = "corrupt_chance", default_value = "0.0")]
    corrupt_chance: f32,
}

#[tokio::main(worker_threads = 3)]
async fn main() {
    env_logger::init();

    let opt = Opt::from_args();

    let port_range_start = 25000;
    let zero_arr = [0u8; 4096 - 8];
    let total_commands = opt.commands;
    let config = Arc::new(TestProcessesConfig::new(
        opt.instances as usize,
        port_range_start,
        if opt.proxy {
            Some(ProxyConfig {
                latency: Duration::from_millis(opt.ping),
                drop_chance: opt.drop_chance,
                corrupt_chance: opt.corrupt_chance,
            })
        } else {
            None
        },
    ));
    let semaphore_val = Semaphore::new(opt.concurrent_requests as usize);
    // Fuck Tokio Semaphore API, why doesn't it have owned variants like RwLock ;_;
    // I know it will live long enough, because of explicit wait for completion and then drop at the end
    let semaphore: &'static Semaphore = unsafe { mem::transmute(&semaphore_val) };

    config.start().await;
    let mut streams = Vec::new();
    for i in 0..opt.instances {
        streams.push(Arc::new(Mutex::new(config.connect(i as usize).await)));
    }
    let streams = Arc::new(streams);

    println!("Commands: {}", opt.commands);
    println!("Concurrency: {}", opt.concurrent_requests);
    println!("Instances: {}", opt.instances);
    println!("Ping: {}", opt.ping);
    println!("Drop chance: {:.3}", opt.drop_chance);
    println!("Corrupt chance: {:.3}", opt.corrupt_chance);
    println!("");

    println!("Starting writing..");
    let start = Instant::now();

    for cmd_idx in 0..total_commands {
        let permit = semaphore.acquire().await.unwrap();
        let streams = streams.clone();
        let config = config.clone();
        tokio::spawn(async move {
            let mut stream = streams[cmd_idx as usize % streams.len()].lock().await;
            let mut data = cmd_idx.to_le_bytes().to_vec();
            data.extend_from_slice(&zero_arr);
            config
                .send_cmd(
                    &RegisterCommand::Client(ClientRegisterCommand {
                        header: ClientCommandHeader {
                            request_identifier: cmd_idx,
                            sector_idx: cmd_idx % TestProcessesConfig::MAX_SECTOR,
                        },
                        content: ClientRegisterCommandContent::Write {
                            data: SectorVec(data),
                        },
                    }),
                    &mut stream,
                )
                .await;
            config.read_response(&mut stream).await.unwrap();
            drop(permit);
        });
    }

    // Wait for end of all writes
    let permits = semaphore
        .acquire_many(opt.concurrent_requests as u32)
        .await
        .unwrap();
    drop(permits);

    let end = Instant::now();
    let duration = end.duration_since(start);
    println!(
        "Done writing. Took {:.3} seconds ({:.3} requests / s)",
        duration.as_secs_f64(),
        opt.commands as f64 / duration.as_secs_f64()
    );

    println!("Start reading");
    let start = Instant::now();

    for cmd_idx in 0..total_commands {
        let permit = semaphore.acquire().await.unwrap();
        let streams = streams.clone();
        let config = config.clone();
        tokio::spawn(async move {
            let mut stream = streams[cmd_idx as usize % streams.len()].lock().await;
            config
                .send_cmd(
                    &RegisterCommand::Client(ClientRegisterCommand {
                        header: ClientCommandHeader {
                            request_identifier: cmd_idx + total_commands,
                            sector_idx: cmd_idx % TestProcessesConfig::MAX_SECTOR,
                        },
                        content: ClientRegisterCommandContent::Read,
                    }),
                    &mut stream,
                )
                .await;
            let response = config.read_response(&mut stream).await.unwrap();

            match response.content {
                RegisterResponseContent::Read(SectorVec(sector)) => {
                    let received_idx = u64::from_le_bytes(sector[0..8].try_into().unwrap());
                    assert_eq!(sector[8..], zero_arr);
                    assert_eq!(
                        received_idx % TestProcessesConfig::MAX_SECTOR,
                        cmd_idx % TestProcessesConfig::MAX_SECTOR
                    );
                }
                _ => panic!("Expected read response"),
            }
            drop(permit);
        });
    }

    // Wait for end of all reads
    let permits = semaphore
        .acquire_many(opt.concurrent_requests as u32)
        .await
        .unwrap();
    drop(permits);

    let end = Instant::now();
    let duration = end.duration_since(start);
    println!(
        "Done reading. Took {:.3} seconds ({:.3} requests / s)",
        duration.as_secs_f64(),
        opt.commands as f64 / duration.as_secs_f64()
    );

    drop(semaphore_val);
}
