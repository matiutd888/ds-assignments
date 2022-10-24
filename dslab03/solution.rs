// WARNING: Do not modify definitions of public types or function names in this
// file – your solution will be tested automatically! Implement all missing parts.

use crossbeam_channel::{unbounded, Receiver, Sender};
use rand::Rng;
use std::collections::HashMap;

use std::thread;
use std::thread::JoinHandle;

type Num = u64;
type Ident = u128;

pub(crate) struct FibonacciModule {
    /// Currently hold number from the sequence.
    num: Num,
    /// Index of the required Fibonacci number (the `n`).
    limit: usize,
    /// Identifier of the module.
    id: Ident,
    /// Identifier of the other module.
    other: Option<Ident>,
    /// Queue for outgoing messages.
    queue: Sender<FibonacciSystemMessage>,
}

fn handle_result<T: std::fmt::Debug>(result: Result<(), T>) {
    let x = match result {
        Err(it) => it,
        _ => return,
    };
    eprintln!("{:?}", x);
}

impl FibonacciModule {
    const B_MODULE_START_NUM: u64 = 1;

    /// Create the module and register it in the system.
    pub(crate) fn create(
        initial_number: Num,
        limit: usize,
        queue: Sender<FibonacciSystemMessage>,
    ) -> Ident {
        // For the sake of simplicity, generate a random number and use it
        // as the module's identifier:
        let id = rand::thread_rng().gen();
        let f = FibonacciModule {
            num: initial_number,
            limit,
            id,
            other: None,
            // TODO we clone queues two times per module. Maybe there is a way not to do it? 
            queue: queue.clone(),
        };
        handle_result(queue.send(FibonacciSystemMessage::RegisterModule(f)));
        id
    }

    /// Handle the step-message from the other module.
    ///
    /// Here the next number of the Fibonacci sequence is calculated.
    pub(crate) fn message(&mut self, idx: usize, num: Num) {
        if idx >= self.limit {
            // The calculation is done.
            handle_result(self.queue.send(FibonacciSystemMessage::Done));
            return;
        }

        self.num = self.num + num;

        // Put the following `println!()` statement after performing
        // the update of `self.num`:
        println!("Inside {}, value: {}", self.id, self.num);

        handle_result(self.queue.send(FibonacciSystemMessage::Message {
            id: self.other.unwrap(),
            idx: idx + 1,
            num: self.num,
        }));
    }

    /// Handle the init-message.
    ///
    /// The module finishes its initialization and initiates the calculation
    /// if it is the first to go.
    pub(crate) fn init(&mut self, other: Ident) {
        self.other = Some(other);
        if self.num == FibonacciModule::B_MODULE_START_NUM {
            // We are the b.module, we should start
            handle_result(self.queue.send(FibonacciSystemMessage::Message {
                id: other,
                idx: 1,
                num: 1,
            }));
        }
    }
}

/// Messages sent to/from the modules.
///
/// The `id` field denotes which module should receive the message.
pub(crate) enum FibonacciSystemMessage {
    /// Register the module in the engine.
    ///
    /// Note that this is a struct without named fields: a tuple struct.
    RegisterModule(FibonacciModule),

    /// Finalize module initialization and initiate the calculations.
    ///
    /// `Init` messages should be sent only by the user of the executor system
    /// (in your solution: the `fib()` function).
    Init { id: Ident, other: Ident },

    /// Initiate the next step of the calculations.
    ///
    /// `idx` is the current index in the sequence.
    /// `num` is the current number of the sequence.
    Message { id: Ident, idx: usize, num: Num },

    /// Indicate the end of calculations.
    Done,
}

/// Run the executor.
pub(crate) fn run_executor(rx: Receiver<FibonacciSystemMessage>) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut modules: HashMap<Ident, FibonacciModule> = HashMap::new();

        let register_module = |registered_modules: &mut HashMap<Ident, FibonacciModule>,
                               module_to_register: FibonacciModule| {
            if registered_modules.contains_key(&module_to_register.id) {
                eprintln!("Module {} already registered!", &module_to_register.id);
            }
            registered_modules.insert(module_to_register.id, module_to_register);
        };

        while let Ok(msg) = rx.recv() {
            match msg {
                FibonacciSystemMessage::Init { id, other } => {
                    modules.get_mut(&id).unwrap().init(other)
                }
                FibonacciSystemMessage::RegisterModule(module) => {
                    register_module(&mut modules, module)
                }
                FibonacciSystemMessage::Message { id, idx, num } => {
                    modules.get_mut(&id).unwrap().message(idx, num)
                }
                FibonacciSystemMessage::Done => break,
            }
        }
    })
}

/// Calculate the `n`-th Fibonacci number.
pub(crate) fn fib(n: usize) {
    // Create the queue and two modules:
    let (tx, rx): (
        Sender<FibonacciSystemMessage>,
        Receiver<FibonacciSystemMessage>,
    ) = unbounded();
    let mod_a_id = FibonacciModule::create(0, n, tx.clone());
    let mod_b_id = FibonacciModule::create(1, n, tx.clone());

    // Tests will be rerun in case the assertion fails:
    assert_ne!(mod_a_id, mod_b_id);

    // Initialize the modules by sending `Init` messages:
    let init_msg_b = FibonacciSystemMessage::Init {
        id: mod_b_id,
        other: mod_a_id,
    };
    let init_msg_a = FibonacciSystemMessage::Init {
        id: mod_a_id,
        other: mod_b_id,
    };

    handle_result(tx.send(init_msg_b));
    handle_result(tx.send(init_msg_a));
    // Run the executor:
    run_executor(rx).join().unwrap();
}
