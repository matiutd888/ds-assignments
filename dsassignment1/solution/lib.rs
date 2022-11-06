use async_channel::unbounded;
use async_channel::Receiver;
use async_channel::Sender;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time;

pub trait Message: Send + 'static {}
impl<T: Send + 'static> Message for T {}

pub trait Module: Send + 'static {}
impl<T: Send + 'static> Module for T {}

/// A trait for modules capable of handling messages of type `M`.
#[async_trait::async_trait]
pub trait Handler<M: Message>: Module {
    /// Handles the message. A module must be able to access a `ModuleRef` to itself through `self_ref`.
    async fn handle(&mut self, self_ref: &ModuleRef<Self>, msg: M);
}

/// A handle returned by `ModuleRef::request_tick()`, can be used to stop sending further ticks.
// You can add fields to this struct
pub struct TimerHandle {
    should_stop: Arc<AtomicBool>,
}

impl TimerHandle {
    /// Stops the sending of ticks resulting from the corresponding call to `ModuleRef::request_tick()`.
    /// If the ticks are already stopped, does nothing.
    pub async fn stop(&self) {
        self.should_stop.store(true, Ordering::Release);
    }
}

#[async_trait::async_trait]
trait Handlee<T: Module>: Message {
    async fn get_handled(self: Box<Self>, module_ref: &ModuleRef<T>, t: &mut T);
}

#[async_trait::async_trait]
impl<M: Message, T: Handler<M>> Handlee<T> for M {
    async fn get_handled(self: Box<Self>, module_ref: &ModuleRef<T>, t: &mut T) {
        t.handle(module_ref, *self).await
    }
}

pub struct System {
    is_running: Arc<AtomicBool>,
    join_handles: Vec<Option<JoinHandle<()>>>,
}

impl System {
    fn spawn_module_channel_reader<T: Module>(
        is_running: Arc<AtomicBool>,
        mut module: T,
        module_ref: ModuleRef<T>,
        receiver: Receiver<Box<dyn Handlee<T>>>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut handlers: Vec<Option<JoinHandle<()>>> = vec![];
            loop {
                if !is_running.load(Ordering::Relaxed) {
                    break;
                }
                let handlee = receiver.recv().await.unwrap();

                let module_ref_clone = module_ref.clone();
                // TODO W jaki sposób przekazywać moduł tym funkcjom?
                // handlers.push(Some(tokio::spawn(async move {
                //     handlee.get_handled(&module_ref_clone, &mut module);
                //     ()
                // })));
                handlee.get_handled(&module_ref_clone, &mut module).await;
            }
            wait_for_all_handles(&mut handlers);
            ()
        })
    }

    /// Registers the module in the system.
    /// Returns a `ModuleRef`, which can be used then to send messages to the module.

    pub async fn register_module<T: Module>(&mut self, module: T) -> ModuleRef<T> {
        let (tx, rx) = unbounded::<Box<dyn Handlee<T>>>();
        let module_ref = ModuleRef {
            is_running: self.is_running.clone(),
            send_queue: tx.clone(),
        };
        self.join_handles
            .push(Some(System::spawn_module_channel_reader(
                self.is_running.clone(),
                module,
                module_ref.clone(),
                rx,
            )));
        module_ref
    }

    /// Creates and starts a new instance of the system.
    pub async fn new() -> Self {
        System {
            is_running: Arc::new(AtomicBool::new(true)),
            join_handles: vec![],
        }
    }

    pub async fn shutdown(&mut self) {
        self.is_running.store(false, Ordering::Relaxed);
        wait_for_all_handles(&mut self.join_handles);
    }
}

fn wait_for_all_handles(handlers: &mut Vec<Option<JoinHandle<()>>>) -> () {
    // TODO czy da się to zrobić lepiej?
    for join_handle in handlers.iter_mut() {
        join_handle.take().map(|x| async { x.await });
    }
}

// 1. W jaki sposób

fn log(s: &str) {
    println!("{}", s);
}

/// A reference to a module used for sending messages.
// You can add fields to this struct.
pub struct ModuleRef<T: Module + ?Sized> {
    is_running: Arc<AtomicBool>,
    send_queue: Sender<Box<dyn Handlee<T>>>,
}

impl<T: Module> ModuleRef<T> {
    /// Sends the message to the module.
    pub async fn send<M: Message>(&self, msg: M)
    where
        T: Handler<M>,
    {
        if !self.is_running.load(Ordering::Relaxed) {
            log("Module is not running anymore");
            return;
        }
        // (for example, if a handler was already running when System::shutdown()
        // was called, calls to ModuleRef::send() in that handler must not panic)
        let result = self.send_queue.try_send(Box::new(msg));
        if result.is_err() {
            log("Error in send(), self.send_queue.try_send() failed!");
        }
    }

    /// Schedules a message to be sent to the module periodically with the given interval.
    /// The first tick is sent after the interval elapses.
    /// Every call to this function results in sending new ticks and does not cancel
    /// ticks resulting from previous calls.
    pub async fn request_tick<M>(&self, message: M, delay: Duration) -> TimerHandle
    where
        M: Message + Clone,
        T: Handler<M>,
    {
        println!("delay: as milis {:?}", delay.as_millis());

        let send_q = self.send_queue.clone();
        let should_stop = Arc::new(AtomicBool::new(false));
        let should_stop_clone = should_stop.clone();
        let mut interval = time::interval(delay);
        let is_system_running = self.is_running.clone();
        // let new_msg = Box::new(message);
        tokio::spawn(async move {
            interval.tick().await;
            loop {
                if !is_system_running.load(Ordering::Relaxed) || should_stop.load(Ordering::Relaxed)
                {
                    break;
                }
                interval.tick().await;
                println!("Tick!");
                let message_ref = &message;
                // https://blog.rust-lang.org/inside-rust/2019/10/11/AsyncAwait-Not-Send-Error-Improvements.html
                let msg_clone = Box::new(message_ref.clone());
                send_q.clone().try_send(msg_clone).unwrap();
            }
        });
        TimerHandle {
            should_stop: should_stop_clone,
        }
    }
}

impl<T: Module> Clone for ModuleRef<T> {
    /// Creates a new reference to the same module.
    fn clone(&self) -> Self {
        ModuleRef {
            is_running: self.is_running.clone(),
            send_queue: self.send_queue.clone(),
        }
    }
}
