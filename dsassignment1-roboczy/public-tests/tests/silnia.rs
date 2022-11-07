use async_channel::{unbounded, Receiver, Sender};
use ntest::timeout;

use std::{
    time::{Duration, Instant},
    vec,
};

use assignment_1_solution::{Handler, ModuleRef, System};

struct Silniarz {
    n: i32,
    curr: i32,
    id: i32,
    duration: Duration,
    send_channel: Sender<i32>,
}

struct Init {
    n: i32,
}

struct SilniaData {
    curr: i32,
}

#[async_trait::async_trait]
impl Handler<Init> for Silniarz {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: Init) {
        self.n = msg.n;
        self.curr = 1;
        println!("{} Initialized Silniarz with n = {}", self.id, self.n);
        // println!("{} Silniarz: Starting to sleep", self.id);
        tokio::time::sleep(self.duration).await;

        let silnia_data = SilniaData { curr: 1 };
        _self_ref.send(silnia_data).await;
    }
}

#[async_trait::async_trait]
impl Handler<SilniaData> for Silniarz {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: SilniaData) {
        if self.n == self.curr {
            println!(
                "{} Silniarz finished calculating, {}! = {}",
                self.id, self.n, msg.curr
            );
            self.send_channel.try_send(msg.curr).unwrap();
        }
        println!("{} Calculating next silniarz step", self.id);
        let mut silnia_data = msg;
        self.curr += 1;
        silnia_data.curr = self.curr * silnia_data.curr;
        tokio::time::sleep(self.duration).await;
        _self_ref.send(silnia_data).await;
    }
}

async fn initialize_silniarz(
    sys: &mut System,
    id: i32,
    n: i32,
    duration: Duration,
) -> (ModuleRef<Silniarz>, Receiver<i32>) {
    let (send, recv) = unbounded();
    let silniarz = Silniarz {
        id: id,
        curr: 0,
        n: -1,
        duration: duration,
        send_channel: send,
    };
    let module_ref = sys.register_module(silniarz).await;
    let init = Init { n: n };
    module_ref.send(init).await;
    (module_ref, recv)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[timeout(300)]
async fn send_doesnt_panic_after_drop() {
    let mut sys = System::new().await;
    let (module_ref, _) = initialize_silniarz(&mut sys, 1, 2, Duration::from_millis(1)).await;
    sys.shutdown().await;
    drop(sys);
    module_ref.send(Init { n: 1 }).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[timeout(30000)]
async fn silniarz_runs_concurrently() {
    let mut sys = System::new().await;

    let n_silniarze = 5;
    let n = 10;
    let duration = Duration::from_millis(50);
    let mut vecs: Vec<Receiver<i32>> = vec![];

    let mut expected = 1;
    for i in 1..(n + 1) {
        expected *= i;
    }

    let start_instant = Instant::now();
    for i in 0..n_silniarze {
        let (_, recv) = initialize_silniarz(&mut sys, i, n, duration).await;
        vecs.push(recv);
    }
    for i in 0..n_silniarze {
        assert!(vecs[i as usize].recv().await.unwrap() == expected);
    }

    let elapsed = start_instant.elapsed();

    let difference = elapsed.as_millis() as i128 - (duration.as_millis() * (n as u128)) as i128;
    assert!(difference.abs() <= 20);
}

#[derive(Clone)]
struct CounterGoal {
    n: u8,
}

struct CounterTillGoal {
    num: u8,
    num_sender: Sender<u8>,
}

#[async_trait::async_trait]
impl Handler<CounterGoal> for CounterTillGoal {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, _msg: CounterGoal) {
        if self.num == _msg.n {
            println!("Sending number!: {}", self.num);
            self.num_sender.send(self.num).await.unwrap();
        }
        self.num += 1;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[timeout(5000)]
async fn ticks_have_no_drift_multiple_counters() {
    let start_instant = Instant::now();

    let n_ticks = 50;
    let n_counters = 10;
    let duration = Duration::from_millis(50);
    let mut system = System::new().await;

    let mut timer_handles = vec![];
    let mut num_receivers: Vec<Receiver<u8>> = vec![];
    for _i in 0..n_counters {
        let (num_sender, num_receiver) = unbounded();
        let counter_ref = system
            .register_module(CounterTillGoal { num: 0, num_sender })
            .await;

        num_receivers.push(num_receiver);
        timer_handles.push(
            counter_ref
                .request_tick(CounterGoal { n: n_ticks }, duration)
                .await,
        );
    }

    for i in 0..n_counters {
        num_receivers[i as usize].recv().await.unwrap();
        timer_handles[i as usize].stop().await;
    }
    system.shutdown().await;

    let elapsed = start_instant.elapsed();
    let difference =
        elapsed.as_millis() as i128 - (duration.as_millis() * (n_ticks as u128 + 1)) as i128;
    println!("Drift! {}", difference.abs());
    assert!(difference.abs() <= 10);
}

#[derive(Clone)]
struct Tick;

struct SleepyCounter {
    num: u8,
    num_sender: Sender<u8>,
    duration: Duration,
}

#[async_trait::async_trait]
impl Handler<Tick> for SleepyCounter {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, _msg: Tick) {
        println!("Sending number!: {}", self.num);
        tokio::time::sleep(self.duration).await;
        self.num_sender.send(self.num).await.unwrap();
        self.num += 1;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[timeout(500)]
async fn doesnt_wait_for_queue_to_empty() {
    let mut system = System::new().await;
    let (num_sender, num_receiver) = unbounded();
    let counter_ref = system
        .register_module(SleepyCounter {
            num: 0,
            num_sender,
            duration: Duration::from_millis(50),
        })
        .await;

    counter_ref.send(Tick).await;
    counter_ref.send(Tick).await;
    counter_ref.send(Tick).await;
    counter_ref.send(Tick).await;
    counter_ref.send(Tick).await;
    counter_ref.send(Tick).await;
    counter_ref.send(Tick).await;

    tokio::time::sleep(Duration::from_millis(170)).await;
    system.shutdown().await;

    let mut received_numbers = Vec::new();
    while let Ok(num) = num_receiver.try_recv() {
        received_numbers.push(num);
    }
    assert_eq!(received_numbers, vec![0, 1, 2]);

    drop(system);
}

struct Counter {
    num: u8,
    num_sender: Sender<u8>,
}

#[async_trait::async_trait]
impl Handler<Tick> for Counter {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, _msg: Tick) {
        println!("Sending number!: {}", self.num);
        self.num_sender.send(self.num).await.unwrap();
        self.num += 1;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[timeout(500)]
async fn must_be_printed_ticks_are_stopped_on_shutdown() {
    let mut system = System::new().await;
    let (num_sender, _r) = unbounded();
    let counter_ref = system.register_module(Counter { num: 0, num_sender }).await;

    let timer_handle = counter_ref
        .request_tick(Tick, Duration::from_millis(50))
        .await;
    
    tokio::time::sleep(Duration::from_millis(170)).await;
    println!("Shutting down!");
    system.shutdown().await;
    drop(system);
    tokio::time::sleep(Duration::from_millis(200)).await;
    // Testing if second stop crashes program
    timer_handle.stop().await;
}
