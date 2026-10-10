use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Default)]
pub struct Shutdown {
    set: Mutex<bool>,
    cv: Condvar,
}

impl Shutdown {
    pub fn trigger(&self) {
        if let Ok(mut set) = self.set.lock() {
            *set = true;
        }
        self.cv.notify_all();
    }

    pub fn is_set(&self) -> bool {
        self.set.lock().map_or(true, |g| *g)
    }

    /// Sleeps for `timeout` or until triggered. Returns true if triggered.
    pub fn wait(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let Ok(mut set) = self.set.lock() else {
            return true;
        };
        while !*set {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            match self.cv.wait_timeout(set, deadline - now) {
                Ok((guard, _)) => set = guard,
                Err(_) => return true,
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use crate::shutdown::Shutdown;

    #[test]
    fn wait_times_out_when_not_triggered() {
        let s = Shutdown::default();
        let start = Instant::now();
        assert!(!s.wait(Duration::from_millis(50)));
        assert!(start.elapsed() >= Duration::from_millis(50));
    }

    #[test]
    fn trigger_wakes_waiters() {
        let s = Arc::new(Shutdown::default());
        let waiter = {
            let s = Arc::clone(&s);
            std::thread::spawn(move || s.wait(Duration::from_secs(30)))
        };
        std::thread::sleep(Duration::from_millis(50));
        let start = Instant::now();
        s.trigger();
        assert!(waiter.join().expect("join"));
        assert!(start.elapsed() < Duration::from_secs(5));
        assert!(s.is_set());
    }
}
