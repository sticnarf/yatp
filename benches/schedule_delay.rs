use hdrhistogram::{Histogram};
use std::thread;
use std::time::{Duration, Instant};
use std::cell::UnsafeCell;
use std::iter;

struct Pointer<T>(*mut T);

unsafe impl<T> Send for Pointer<T> {}
unsafe impl<T> Sync for Pointer<T> {}

fn main() {
    let pool = yatp::Builder::new("schedule_delay").max_thread_count(12).build_future_pool();

    let results: Vec<_> = iter::repeat_with(|| UnsafeCell::new(1000000)).take(1_000_000).collect();

    let mut last_instant = Instant::now();
    for i in 0..1_000_000 {
        let now = loop {
            let now = Instant::now();
            let micros = (now - last_instant).as_micros() as u64;
            if micros >= 10 {
                break now;
            }
            thread::yield_now();
        };
        let res = Pointer(results[i].get());
        pool.spawn(async move {
            unsafe { *res.0 = now.elapsed().as_nanos() as u64; }
            for _ in 0..500 {
                std::sync::atomic::spin_loop_hint();
            }
        });
        last_instant = now;
    }

    thread::sleep(Duration::from_secs(5));

    let mut hist = Histogram::<u64>::new_with_bounds(1, 10_000_000, 2).unwrap();
    for res in results {
        hist += unsafe { *res.get() };
    }
    for v in hist.iter_log(1, 2.0) {
        println!("{:?}", v);
    }
    println!("mean: {}", hist.mean());
}
