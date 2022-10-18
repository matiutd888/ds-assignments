mod public_test;
mod solution;

use std::{sync::{Arc, Mutex}, thread, time::{Duration, Instant}};

fn main() {
    // let shared_vec = Arc::new(Mutex::new(Vec::new()));
    let pool = solution::Threadpool::new(10);

    let start = Instant::now();
    for x in 0..5 {
        // let shared_vec_clone = shared_vec.clone();
        pool.submit(Box::new(move || {
            // std::thread::sleep(std::time::Duration::from_millis(500));
            // let mut vec = shared_vec_clone.lock().unwrap();
            // vec.push(x);
            println!("elapsed {:?}: Thread {:#?} goes asleep,", start.elapsed(), x);
            std::thread::sleep(std::time::Duration::from_millis(1000));
        
            println!("elapsed {:?}: Thread {:#?} wakes up", start.elapsed(), x);
        }));
    }
    // let pool = solution::Threadpool::new(4);
        
    //     for i in 0..3 {    
    //         pool.submit(Box::new(move || {
    //             println!("I am thread {} and I am  going to sleep", i);
    //             thread::sleep(Duration::new(1, 0));
    //             println!("I am thread {} and I have woken up!", i);
    //         }));
    //     }
}
