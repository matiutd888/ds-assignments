use async_channel::{unbounded, Receiver, Sender};
use ntest::timeout;

use std::time::{Duration, Instant};

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

async fn initialize_silniarz(sys: &mut System, id: i32, n: i32, duration: Duration) -> Receiver<i32> {
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
    recv
}

#[tokio::test]
#[timeout(30000)]
async fn silniarz_runs_currectly() {
    let mut sys = System::new().await;
    let start_instant = Instant::now();
    let duration = Duration::from_millis(50);
    let n = 9;
    let id1 = 0;
    let id2 = 1;
    let recv1 = initialize_silniarz(&mut sys, id1, n, duration).await;
    let recv2 = initialize_silniarz(&mut sys, id2, n, duration).await;
    let output1 = recv1.recv().await.unwrap();
    let output2 = recv2.recv().await.unwrap();
    println!("{} finished, {}", id1, output1);
    println!("{} finished, {}", id2, output2);
    sys.shutdown().await;
    let elapsed = start_instant.elapsed();

    let difference = elapsed.as_millis() as i128 - (duration.as_millis() * (n as u128)) as i128;
    println!("{}", difference.abs() <= (duration.as_millis() as i128));
}
