use async_channel::unbounded;
use async_channel::Receiver;
use async_channel::Sender;

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
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
    timer_stop_sender: Sender<StopMessage>,
}

impl TimerHandle {
    /// Stops the sending of ticks resulting from the corresponding call to `ModuleRef::request_tick()`.
    /// If the ticks are already stopped, does nothing.
    pub async fn stop(&self) {
        _ = self.timer_stop_sender.send(StopMessage).await;
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

struct StopMessage;

pub struct System {
    is_running: Arc<AtomicBool>, // This flag is somewhat unnecessary, but let's add it for faster stopping.
    reader_stop_senders: Vec<Sender<StopMessage>>,
    join_handles: Vec<JoinHandle<()>>,
}

impl System {
    fn spawn_module_channel_reader<T: Module>(
        stop_receiver: Receiver<StopMessage>,
        mut module: T,
        module_ref: ModuleRef<T>,
        message_receiver: Receiver<Box<dyn Handlee<T>>>,
        is_running: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                if !is_running.load(Ordering::Acquire) {
                    break;
                }
                select! {
                    Ok(_) = stop_receiver.recv() => {
                        // Successfully received StopMessage.
                        // If recv() returned Err it would mean that system dropped without a shutdown.
                        // We let the tasks run in that case.
                        break;
                    }
                    message_result = message_receiver.recv() => {
                        if let Ok(handlee) = message_result {
                            let module_ref_clone = module_ref.clone();
                            handlee.get_handled(&module_ref_clone, &mut module).await;
                        } else {
                            // Will never return error as we've got control over one sender
                            log("Error receiving message - receiver channel closed");
                            break;
                        }
                    }
                }
            }
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
        let (stop_sender, stop_receiver) = unbounded();
        self.reader_stop_senders.push(stop_sender);
        self.join_handles
            .push(System::spawn_module_channel_reader(
                stop_receiver,
                module,
                module_ref.clone(),
                rx,
                self.is_running.clone(),
            ));
        module_ref
    }

    /// Creates and starts a new instance of the system.
    pub async fn new() -> Self {
        System {
            is_running: Arc::new(AtomicBool::new(true)),
            reader_stop_senders: vec![],
            join_handles: vec![],
        }
    }

    pub async fn shutdown(&mut self) {
        self.is_running.store(false, Ordering::Release);
        send_stop_messages(&self.reader_stop_senders).await;
        wait_for_all_handles(&mut self.join_handles).await;
    }
}

async fn wait_for_all_handles(handlers: &mut Vec<JoinHandle<()>>) {
    for join_handle in handlers.iter_mut() {
        _ = join_handle.await;
    }
}

async fn send_stop_messages(stop_senders: &Vec<Sender<StopMessage>>) {
    for stop_sender in stop_senders {
        _ = stop_sender.send(StopMessage).await;
    }
}

fn log(s: &str) {
    eprintln!("{}", s);
}

/// A reference to a module used for sending messages.
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
        // If a handler was already running when System::shutdown()
        // was called, calls to ModuleRef::send() in that handler must not panic
        let result = self.send_queue.try_send(Box::new(msg));
        if result.is_err() {
            log("Error in send(), try_send failed");
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
        let (timer_stop_sender, timer_stop_receiver) = unbounded();
        let ret = TimerHandle { timer_stop_sender };
        if delay.is_zero() {
            log("Delay is cannot be zero");
            return ret;
        }
        if !self.is_running.load(Ordering::Acquire) {
            log("System already finished");
            return ret;
        }

        let is_running = self.is_running.clone();
        let send_q = self.send_queue.clone();

        let mut interval = time::interval(delay);
        tokio::spawn(async move {
            interval.tick().await;
            loop {
                if !is_running.load(Ordering::Acquire) {
                    break;
                }
                select! {
                    Ok(_) = timer_stop_receiver.recv() => {
                        break;
                    }
                    _ = interval.tick() => {
                        let result = send_q.try_send(Box::new((&message).clone()));
                        if result.is_err() {
                            // If send failed it means the receiver queue was dropped -> system shut down.
                            // It means it's best to end the tasks.
                            break;
                        }
                    }
                }
            }
        });
        return ret;
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
