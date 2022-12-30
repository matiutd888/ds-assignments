use log::{debug, warn};
use rand::distributions::{Distribution, Uniform};
use rand::{Rng, SeedableRng};
use tokio::select;
use std::{io::Error, net::SocketAddr, sync::Arc, time::Duration};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::watch;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    time::Instant,
};
#[derive(Clone)]
pub struct ProxyConfig {
    pub latency: Duration,
    pub drop_chance: f32,
    pub corrupt_chance: f32,
}
pub struct ARProxy {
    config: ProxyConfig,
    bind_addr: (String, u16),
    remote_addr: (String, u16),
    enabled_send: watch::Sender<bool>,
}
impl ARProxy {
    pub fn new(
        bind_addr: (String, u16),
        remote_addr: (String, u16),
        config: ProxyConfig,
    ) -> Arc<ARProxy> {
        let (tx, _) = watch::channel(true);
        Arc::new(ARProxy {
            config,
            bind_addr,
            remote_addr,
            enabled_send: tx,
        })
    }
    pub fn enable(&self) {
        let _ = self.enabled_send.send(true);
    }
    pub fn disable(&self) {
        let _ = self.enabled_send.send(false);
    }
    pub async fn run(self: Arc<Self>) {
        let socket = TcpListener::bind(&self.bind_addr).await.unwrap();
        let mut enabled = self.enabled_send.subscribe();
        loop {
            if *enabled.borrow() == false {
                wait_for_enabled(&mut enabled).await;
            }
            let (socket, addr) = match socket.accept().await {
                Ok(x) => x,
                Err(e) => {
                    warn!("[Proxy] Error while accepting: {}", e);
                    continue;
                }
            };
            debug!("[Proxy] Received connection from: {}", addr);
            let clone = self.clone();
            tokio::spawn(async move { clone.clone().handle_connection(socket, addr).await });
        }
    }
    async fn handle_connection(
        self: Arc<Self>,
        socket: TcpStream,
        _addr: SocketAddr,
    ) -> Result<(), Error> {
        let (sender_cts, receiver_cts) = unbounded_channel();
        let (sender_stc, receiver_stc) = unbounded_channel();
        debug!("[Proxy] Connecting to {:?}", &self.remote_addr);
        let out = match TcpStream::connect(&self.remote_addr).await {
            Ok(s) => {
                debug!("[Proxy] Connected to {:?}", &self.remote_addr);
                s
            }
            Err(e) => {
                debug!("[Proxy] Can't connect to {:?}: {}", &self.remote_addr, e);
                return Err(e);
            }
        };
        let (client_read, client_write) = socket.into_split();
        let (server_read, server_write) = out.into_split();
        let clone = self.clone();
        let clone2 = self.clone();
        let clone3 = self.clone();
        let _ = tokio::join!(
            tokio::spawn(async move { self.handle_receiving(client_read, sender_cts).await }),
            tokio::spawn(async move { clone.handle_sending(server_write, receiver_cts).await }),
            tokio::spawn(async move { clone2.handle_receiving(server_read, sender_stc).await }),
            tokio::spawn(async move { clone3.handle_sending(client_write, receiver_stc).await }),
        );
        Ok(())
    }
    async fn handle_sending(
        self: Arc<Self>,
        mut socket: OwnedWriteHalf,
        mut receiver: UnboundedReceiver<(Vec<u8>, Instant)>,
    ) -> Result<(), Error> {
        let enabled = self.enabled_send.subscribe();
        loop {
            let (data, time) = match receiver.recv().await {
                Some(v) => v,
                None => return Ok(()),
            };
            tokio::time::sleep_until(time).await;
            if *enabled.borrow() == false {
                return Ok(());
            }
            socket.write_all(&data).await?;
        }
    }

    async fn handle_receiving(
        self: Arc<Self>,
        mut socket: OwnedReadHalf,
        sender: UnboundedSender<(Vec<u8>, Instant)>,
    ) -> Result<(), Error> {
        let mut buffer = [0u8; 1024];
        let mut enabled = self.enabled_send.subscribe();
        let mut rng = rand::rngs::StdRng::from_rng(rand::thread_rng()).unwrap();
        let chance = Uniform::from(0.0..1.0);
        loop {
            if *enabled.borrow() == false {
                return Ok(());
            }
            select! {
                size = socket.read(&mut buffer) => {
                    let size = match size {
                        Ok(s) => s,
                        Err(e) => {
                            debug!("[Proxy Receiver] Read error, dropping connection: {}", e);
                            return Err(e);
                        }
                    };
                    if size == 0 {
                        debug!("[Proxy Receiver] Read size 0, dropping connection");
                        return Ok(());
                    }

                    let mut data = buffer[..size].to_vec();

                    if chance.sample(&mut rng) < self.config.drop_chance {
                        continue;
                    }

                    if chance.sample(&mut rng) < self.config.corrupt_chance {
                        let idx = rng.gen_range(0..data.len());
                        data[idx] = rng.gen();
                    }

                    match sender.send((data, Instant::now() + self.config.latency)) {
                        Ok(()) => (),
                        Err(e) => {
                            println!(
                                "[Proxy Receiver] Channel send error, dropping connection: {}",
                                e
                            );
                            return Ok(());
                        }
                    }
                }
                _ = enabled.changed() => {
                    if *enabled.borrow() == false {
                        return Ok(());
                    }
                }
            };
        }
    }
}

async fn wait_for_enabled(enabled: &mut watch::Receiver<bool>) {
    loop {
        if *enabled.borrow_and_update() == true {
            return;
        }
        let _ = enabled.changed().await;
    }
}
