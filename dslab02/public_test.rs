#[cfg(test)]
mod tests {
    use crate::solution::Threadpool;
    use crossbeam_channel::unbounded;
    use ntest::timeout;
    use std::{sync::Arc, thread, time::Duration};

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
    #[timeout(10000)]
    fn threadpool_concurrency_test() {
        let pool = Threadpool::new(4);
        
        for i in 0..3 {    
            pool.submit(Box::new(move || {
                println!("I am thread {} and I am  going to sleep", i);
                thread::sleep(Duration::new(1, 0));
                println!("I am thread {} and I have woken up!", i);
            }));
        }
    }
}
