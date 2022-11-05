use std::marker::PhantomData;
use std::sync::Mutex;
use std::thread::JoinHandle;
use std::time::Duration;

use async_channel::unbounded;
use async_channel::Receiver;
use async_channel::Sender;
use tokio::sync::futures;
use  futures::stream::FuturesUnordered;


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
pub struct TimerHandle {}

impl TimerHandle {
    /// Stops the sending of ticks resulting from the corresponding call to `ModuleRef::request_tick()`.
    /// If the ticks are already stopped, does nothing.
    pub async fn stop(&self) {
        unimplemented!()
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

// You can add fields to this struct.
pub struct System {}

impl System {
    fn spawn_module_channel_reader<T: Module>(
        sender: Sender<Box<dyn Handlee<T>>>,
        receiver: Receiver<Box<dyn Handlee<T>>>,
        timeDurationReceiver: Receiver<(Box<dyn Handlee<T>>, Duration)>,
    ) -> JoinHandle<()> {
        tokio::spawn(async {
            let timeFutures: FuturesUnordered;
        });
    }

    /// Registers the module in the system.
    /// Returns a `ModuleRef`, which can be used then to send messages to the module.

    pub async fn register_module<T: Module>(&mut self, module: T) -> ModuleRef<T> {
        let (tx, rx) = unbounded::<Box<dyn Handlee<T>>>();
        let (timeSender, timeReceiver) = unbounded::<(Box<dyn Handlee<T>>, Duration)>();
        
        
        // TODO zastanowić się gdzie dać Arc<Mutex<Bool>> na is_finished.
        System::spawn_module_channel_reader(tx, rx, timeReceiver);

        // 1. Stwórz dla modułu aync receive channel i send channel
        // 2. zespawnuj taska dla receive kolejki, który będzie miał jednego wielkiego selecta
        // 2a) Dla każdego timera
        // Ticks requested by System::request_tick()
        // must be delivered at specified time intervals.
        // 2b) dla skończenia normalnego taska z kolejki
        // MessageRef ma klona send cześći kanału. Potrzeba na to mieć jakieś Arc z mutexem zapewne
        unimplemented!()
    }

    /// Creates and starts a new instance of the system.
    pub async fn new() -> Self {}

    /// Gracefully shuts the system down.
    ///
    /// Shutting the system down gracefully (System::shutdown()).
    /// The shutdown should wait for all already started handlers to finish and for
    /// all registered modules to be dropped.
    /// It should not wait for all enqueued messages to be handled.
    /// It does not have to wait for all Tokio tasks to finish, but it must cause all of them to finish (for example, it is acceptable if a task handling ModuleRef::request_tick() finishes an interval after the shutdown).
    // It is undefined what happens when the system is used after shutdown. However, you must ensure that a shutdown will not cause any panics in handlers or Tokio tasks (for example, if a handler was already running when System::shutdown() was called, calls to ModuleRef::send() in that handler must not panic).
    pub async fn shutdown(&mut self) {
        unimplemented!()
    }
}

// 1. W jaki sposób

/// A reference to a module used for sending messages.
// You can add fields to this struct.
pub struct ModuleRef<T: Module + ?Sized> {
    // A dummy marker to satisfy the compiler. It can be removed if type T is
    // used in some other field.
    // Arc<Mutex> because timer can also send messabes to the queue
    send_queue: Sender<Box<dyn Handlee<T>>>,
    // Tutaj jakoś trzeba mechanizm który pozwoli
}

impl<T: Module> ModuleRef<T> {
    /// Sends the message to the module.
    pub async fn send<M: Message>(&self, msg: M)
    where
        T: Handler<M>,
    {
        // TODO tutaj być może trzeba dodać jakieś sprawdzenie flagi czy moduł się skończył
        self.send_queue.try_send(Box::new(msg)).unwrap();
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
        unimplemented!()
    }
}

impl<T: Module> Clone for ModuleRef<T> {
    /// Creates a new reference to the same module.
    fn clone(&self) -> Self {
        unimplemented!()
    }
}
