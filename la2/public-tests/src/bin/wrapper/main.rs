use assignment_2_solution::{run_register_process, Configuration, PublicConfiguration};
use env_logger;
use std::convert::TryFrom;

const MAX_SECTOR: u64 = (2u64).pow(20) - 1;

const HOST: &str = "127.0.0.1";

fn nth_config(n: u8) -> Configuration {
    let tcp_locations: Vec<(String, u16)> = vec![
        (String::from(HOST), 50000),
        // (String::from(HOST), 50001),
        // (String::from(HOST), 50002),
    ];

    let dir = tempfile::tempdir().unwrap().into_path();
    println!("Dir: {:?}", dir);
    assert!(dir.is_dir());

    Configuration {
        hmac_system_key: [3; 64],
        hmac_client_key: <[u8; 32]>::try_from(
            hex::decode("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                .unwrap(),
        )
        .unwrap(),

        public: PublicConfiguration {
            storage_dir: dir,
            tcp_locations,
            self_rank: n + 1,
            n_sectors: MAX_SECTOR + 1,
        },
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let i_str = std::env::args().nth(1).expect("Not enough args");
    let i = i_str.parse::<u8>().unwrap();

    run_register_process(nth_config(i)).await;
}
