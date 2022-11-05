use std::{time::Duration, thread::sleep};

use tokio::task;

async fn async_function() {
    println!("I am a task created from the async function.")
}

// The `#[tokio::main]` macro creates a runtime, and
// calls `block_on()` passing to it the function as a future:
#[tokio::main]
async fn main_task() {
    println!("I am the main task.");

    let task1 = tokio::spawn(async {
        println!("I am a new task created from the async block.");
    });

    let task2 = tokio::spawn(async_function());

    // 
    let task3 = tokio::spawn(async {
        println!("I am a new task.");
        let dupa = tokio::spawn(async {
            println!("I am a new task spawned by the new task.");
        });
        let dupa = task::spawn_blocking(|| {
            sleep(Duration::from_millis(5000));
            7;
        }).await;
        println!("I finished sleeping.");
    });

    // All above tasks are being now executed asynchronously.

    // Spawning a task returns a handle which allows of waiting
    // for its completion:
    task1.await.unwrap();
    task2.await.unwrap();
    task3.await.unwrap();
}

fn tokio_example() {
    // Although `main_task()` is prefixed with `async`, the below
    // call is synchronous. The `#[tokio::main]` macro wraps the
    // asynchronous function with a synchronous initialization
    // of the Tokio runtime, and calls `block_on()` passing
    // the function as a future:
    main_task();
}

fn main() {
    tokio_example()
}