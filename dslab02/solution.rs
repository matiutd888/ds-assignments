use std::ops::{DerefMut};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{spawn, JoinHandle};

type Task = Box<dyn FnOnce() + Send>;

// You can define new types (e.g., structs) if you need.
// However, they shall not be public (i.e., do not use the `pub` keyword).

/// The thread pool.
pub struct Threadpool {
    data: Arc<(Mutex<(Vec<Task>, bool)>, Condvar)>,
    handles: Vec<Option<JoinHandle<()>>>, // Add here any fields you need.
                                          // We suggest storing handles of the worker threads, submitted tasks,
                                          // and an information whether the pool is running or it is to be finished.
}

impl Threadpool {
    /// Create new thread pool with `workers_count` workers.
    pub fn new(workers_count: usize) -> Self {
        let vec: Vec<Task> = Vec::new();
        let shared_data: Arc<(Mutex<(Vec<Task>, bool)>, Condvar)> =
            Arc::new((Mutex::new((vec, true)), Condvar::new()));
        let mut worker_handles: Vec<Option<JoinHandle<()>>> = Vec::new();

        for _ in 0..workers_count {
            let cloned_data = shared_data.clone();

            worker_handles.push(Some(spawn(move || {
                Self::worker_loop(cloned_data);
            })));
        }
        Threadpool {
            data: shared_data,
            handles: worker_handles,
        }
    }

    /// Submit a new task.
    /// TODO how does this function should behave when it is ended.
    pub fn submit(&self, task: Task) {
        let (tasks_and_flag, wait_for_task) = &*self.data;
        let mut guard = tasks_and_flag.lock().unwrap();
        let (tasks, _) = guard.deref_mut();
        let should_notify = tasks.is_empty();
        tasks.push(task);
        drop(guard);
        if should_notify {
            wait_for_task.notify_one();
        }
    }

    // We suggest extracting the implementation of the worker to an associated
    // function, like this one (however, it is not a part of the public
    // interface, so you can delete it if you implement it differently):
    fn worker_loop(data: Arc<(Mutex<(Vec<Task>, bool)>, Condvar)>) -> () {
        let (tasks_and_flag, wait_for_task) = &*data;
        
        loop {
            let mut guard = wait_for_task
                .wait_while(tasks_and_flag.lock().unwrap(), |(tasks, flag)| {
                    tasks.is_empty() && *flag
                })
                .unwrap();
            let (tasks, flag) = guard.deref_mut();
        
            if !*flag && tasks.is_empty() {
                drop(guard);
                break;
            }
            let new_task = tasks.pop().unwrap();
            drop(guard);
            new_task();

        }
        return ();
    }
}

impl Drop for Threadpool {
    /// Gracefully end the thread pool.
    ///
    /// It waits until all submitted tasks are executed,
    /// and until all threads are joined.
    fn drop(&mut self) {
        let (tasks_and_flag, wait_for_task) = &*self.data;
        let mut guard = tasks_and_flag.lock().unwrap();
        let (_, flag) = guard.deref_mut();
        *flag = false;

        drop(guard);
        wait_for_task.notify_all();
        for handle in self.handles.iter_mut() {
            handle.take().map(JoinHandle::join);
        }
    }
}
