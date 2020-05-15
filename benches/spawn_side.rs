// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use criterion::*;

mod yatp_callback {
    use criterion::*;

    pub fn spawn_side(c: &mut Criterion) {
        let pool = yatp::Builder::new("spawn_side").build_callback_pool();

        c.bench_function("yatp_callback", |b| {
            b.iter(|| {
                pool.spawn(move |_: &mut yatp::task::callback::Handle<'_>| {});
            })
        });
    }
}

mod yatp_future {
    use criterion::*;
    use yatp::task::future::TaskCell;

    fn spawn_side(c: &mut Criterion, pool: yatp::ThreadPool<TaskCell>, name: &str) {
        c.bench_function(name, |b| {
            b.iter(|| {
                pool.spawn(async move {});
            })
        });
    }

    pub fn spawn_side_single_level(c: &mut Criterion) {
        let pool = yatp::Builder::new("spawn_side").build_future_pool();
        spawn_side(c, pool, "yatp_future_single_level")
    }

    pub fn spawn_side_multilevel(c: &mut Criterion) {
        let pool = yatp::Builder::new("spawn_side").build_multilevel_future_pool();
        spawn_side(c, pool, "yatp_future_multilevel")
    }
}

mod threadpool {
    use criterion::*;

    pub fn spawn_side(c: &mut Criterion) {
        let pool = threadpool::ThreadPool::new(num_cpus::get());
        c.bench_function("threadpool", |b| {
            b.iter(|| {
                pool.execute(move || {});
            })
        });
    }
}

mod tokio {
    use criterion::*;
    use tokio::runtime::Builder;

    pub fn spawn_side(c: &mut Criterion) {
        let pool = Builder::new()
            .threaded_scheduler()
            .core_threads(num_cpus::get())
            .build()
            .unwrap();
        c.bench_function("tokio", |b| {
            b.iter(|| {
                pool.spawn(async move {});
            })
        });
    }
}

mod async_std {
    use criterion::*;

    pub fn spawn_side(c: &mut Criterion) {
        c.bench_function("async-std", |b| {
            b.iter(|| {
                async_std::task::spawn(async move {});
            })
        });
    }
}

criterion_group!(
    spawn_side_group,
    yatp_callback::spawn_side,
    yatp_future::spawn_side_single_level,
    yatp_future::spawn_side_multilevel,
    threadpool::spawn_side,
    tokio::spawn_side,
    async_std::spawn_side
);

criterion_main!(spawn_side_group);
