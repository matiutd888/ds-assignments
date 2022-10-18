#[cfg(test)]
mod tests {
    use crate::solution::Threadpool;
    use crossbeam_channel::unbounded;
    use ntest::timeout;
    use std::sync::{Arc, Barrier};
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::{thread, time};

    #[test]
    #[timeout(200)]
    fn smoke_test() {
        let (tx, rx) = unbounded();
        let pool = Threadpool::new(1);

        pool.submit(Box::new(move || {
            tx.send(14).unwrap();
        }));

        assert_eq!(14, rx.recv().unwrap());
    }

    #[test]
    #[timeout(200)]
    fn threadpool_is_sync() {
        let send_only_when_threadpool_is_sync = Arc::new(Threadpool::new(1));
        let (tx, rx) = unbounded();

        let _handle = std::thread::spawn(move || {
            tx.send(send_only_when_threadpool_is_sync).unwrap();
        });

        rx.recv().unwrap();
    }

    #[test]
    #[timeout(200)]
    fn drop_test() {
        let (tx, rx) = unbounded();
        let pool = Threadpool::new(2);
        
        for _ in 0..2 {
            let tx_th = tx.clone();
            pool.submit(Box::new(move || {
                let sleep_time = time::Duration::from_millis(50);
                thread::sleep(sleep_time);
                tx_th.send(14).unwrap();
            }));
        }
        
        assert_eq!(rx.is_empty(), true);
        std::mem::drop(pool);
        assert_eq!(rx.is_empty(), false);

        assert_eq!(14, rx.recv().unwrap());
        assert_eq!(14, rx.recv().unwrap());
    }

    #[test]
    #[timeout(200)]
    fn concurrency_test() {
        let (tx1, rx1) = unbounded();
        let (tx2, rx2) = unbounded();
        let (tx_final, rx_final) = unbounded();
        let pool = Threadpool::new(2);
        
        let tx_final_clone = tx_final.clone();
        pool.submit(Box::new(move || {
            tx1.send(14).unwrap();
            assert_eq!(15, rx2.recv().unwrap());
            tx_final_clone.send(33).unwrap();
        }));

        let tx_final_clone = tx_final.clone();
        pool.submit(Box::new(move || {
            tx2.send(15).unwrap();
            assert_eq!(14, rx1.recv().unwrap());
            tx_final_clone.send(33).unwrap();
        }));

        assert_eq!(33, rx_final.recv().unwrap());
        assert_eq!(33, rx_final.recv().unwrap());
    }

    #[test]
    #[timeout(200)]
    fn many_threads() {
        let count = 100;
        let (tx, rx) = unbounded();
        let pool = Threadpool::new(count);

        let shared = Arc::new(tx);

        for i in 0..count {
            let shared_clone = shared.clone();
            pool.submit(Box::new(move || {
                shared_clone.send(i).unwrap();
            }));
        }

        let mut vector = vec![];
        for _ in 0..count {
            vector.push(rx.recv().unwrap());
        }
        vector.sort();
        assert_eq!(vector, (0..count).collect::<Vec<usize>>());
    }

    #[test]
    fn concurrency() {
        fn long_computation() -> u64 {
            let mut k : u64 = 0;
            for i in 0..10000000 {
                k *= i;
            }
            return k;
        }

        fn for_x_threads(count: usize) {
            let (tx, rx) = unbounded();
            let pool = Threadpool::new(count);

            for _ in 0..count {
                pool.submit(Box::new(|| {long_computation();}));
            }
            pool.submit(Box::new(move || {
                tx.send(14).unwrap();
            }));

            assert_eq!(14, rx.recv().unwrap());
        }

        fn measure_for_x_threads(count: usize) -> std::time::Duration {
            let instant = std::time::Instant::now();
            for_x_threads(count);
            instant.elapsed()
        }

        let single_threaded = measure_for_x_threads(1);

        // CRUCIAL!!! : Assuming  n := number of your CPU cores, put n-1 below!
        // (if you can't be sure, 3 should do)
        let multi_threaded = measure_for_x_threads(5); // <-- put n-1 here!

        let factor = 2; // <-- pure heuresis
        println!("Single-threaded time: {:?}\nMulti-threaded time: {:?}", single_threaded, multi_threaded);
        assert!(multi_threaded < single_threaded ||
                multi_threaded - single_threaded < single_threaded / factor,
                "The execution must not have been parallel!")

    }

        #[test]
    #[timeout(200)]
    fn sum_1000_numbers() {
        let sum: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));

        let pool = Threadpool::new(4);
        for i in 0..1000 {
            let cloned_sum = sum.clone();
            pool.submit(Box::new(move || {
                cloned_sum.fetch_add(i, Ordering::Relaxed);
            }));
        }

        std::mem::drop(pool);

        assert_eq!(sum.load(Ordering::Relaxed), (1000 * 999) / 2);
    }

    /* It's possible to handle panics but not required
    #[test]
    #[timeout(200)]
    fn panics_are_caught() {
        let pool = Threadpool::new(4);

        // Panic on all threads
        for _ in 0..10 {
            pool.submit(Box::new(|| panic!("Im panicking")));
        }

        // Thread pool still works
        let value: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
        let cloned_value = value.clone();
        pool.submit(Box::new(move || {
            cloned_value.store(true, Ordering::Relaxed)
        }));

        std::mem::drop(pool);
        assert_eq!(value.load(Ordering::Relaxed), true);
    }
    */

    #[test]
    #[timeout(200)]
    fn threadpool_is_concurrent() {
        let barrier1 = Arc::new(Barrier::new(4));
        let barrier2 = Arc::new(Barrier::new(4));

        let pool = Threadpool::new(4);
        for _ in 0..4 {
            let cloned_barrier1 = barrier1.clone();
            let cloned_barrier2 = barrier2.clone();
            pool.submit(Box::new(move || {
                cloned_barrier1.wait();
                cloned_barrier2.wait();
            }));
        }
    }

    #[test]
    #[timeout(1000)]
    fn drop_waits() {
        let pool = Threadpool::new(4);

        let value: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
        let cloned_value = value.clone();
        pool.submit(Box::new(move || {
            std::thread::sleep(std::time::Duration::from_millis(800));
            cloned_value.store(true, Ordering::Relaxed);
        }));

        std::mem::drop(pool);

        assert_eq!(value.load(Ordering::Relaxed), true);
    }
}