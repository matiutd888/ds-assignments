pub struct Fibonacci {
    mem: [<Fibonacci as Iterator>::Item; 2],
    count: usize,
    has_ended: bool,
}

impl Fibonacci {
    /// Create new `Fibonacci`.
    pub fn new() -> Fibonacci {
        Fibonacci {
            mem: [0, 1],
            count: 0,
            has_ended: false,
        }
    }

    /// Calculate the n-th Fibonacci number.
    ///
    /// This shall not change the state of the iterator.
    /// The calculations shall wrap around at the boundary of u8.
    /// The calculations might be slow (recursive calculations are acceptable).
    pub fn fibonacci(n: usize) -> u8 {
        let mut iter: [u8; 2] = [0, 1];
        if n <= 1 {
            return iter[n];
        };

        for _ in 2..(n + 1) {
            let x: u8 = iter[0].wrapping_add(iter[1]);
            iter[0] = iter[1];
            iter[1] = x;
        }

        return iter[1];
    }
}

impl Iterator for Fibonacci {
    type Item = u128;

    /// Calculate the next Fibonacci number.
    ///
    /// The first call to `next()` shall return the 0th Fibonacci number (i.e., `0`).
    /// The calculations shall not overflow and shall not wrap around. If the result
    /// doesn't fit u128, the sequence shall end (the iterator shall return `None`).
    /// The calculations shall be fast (recursive calculations are **un**acceptable).
    fn next(&mut self) -> Option<Self::Item> {
        if self.has_ended {
            return None;
        }

        if self.count <= 1 {
            self.count += 1;
            return Some(self.mem[self.count - 1]);
        }
        let sum: Option<u128> = self.mem[0].checked_add(self.mem[1]);

        if let Some(new_val) = sum {
            self.count += 1;
            self.mem[0] = self.mem[1];
            self.mem[1] = new_val;
        } else {
            self.has_ended = true;
        }
        sum
    }
}
