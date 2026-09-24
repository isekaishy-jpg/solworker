#[path = "../tests/common/mod.rs"]
mod common;

use common::world::{Background, Executor, Schedule, World};
use std::hint::black_box;
use std::io::{BufWriter, Write};
use std::time::Instant;

fn setting(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |value| {
        value.parse().expect("invalid benchmark setting")
    })
}

fn main() {
    let frames = setting("SW_BENCH_FRAMES", 500);
    let warmup = setting("SW_BENCH_WARMUP", 50);
    let instances = setting("SW_BENCH_INSTANCES", 256);
    let chunk = setting("SW_BENCH_CHUNK", 32);
    let workers = setting("SW_BENCH_WORKERS", 1);
    let rounds = setting("SW_BENCH_ROUNDS", 16);
    let competing = setting("SW_BENCH_BACKGROUND", 1) != 0;
    assert!(frames > 0 && workers > 0);
    let mut raw = std::env::var_os("SW_RUN_OUTPUT_DIR").map(|directory| {
        let path = std::path::PathBuf::from(directory).join("world-samples.csv");
        let mut file = BufWriter::new(std::fs::File::create(path).unwrap());
        writeln!(file, "schedule,frame,elapsed_us").unwrap();
        file
    });
    let mut reference_checksum = None;
    println!(
        "schedule,instances,chunk,workers_per_class,rounds,frames,warmup,p50_us,p95_us,p99_us,max_us,mean_us,checksum,background_blocks"
    );
    let selected = std::env::var("SW_BENCH_SCHEDULE").ok();
    if let Some(name) = &selected {
        assert!(
            Schedule::ALL
                .iter()
                .any(|schedule| format!("{schedule:?}") == *name),
            "unknown SW_BENCH_SCHEDULE"
        );
    }
    for schedule in Schedule::ALL {
        if selected
            .as_ref()
            .is_some_and(|name| *name != format!("{schedule:?}"))
        {
            continue;
        }
        let mut executor = Executor::new(workers);
        let mut world = World::new(instances, chunk);
        let background = competing.then(|| Background::start(&executor.runtime));
        for _ in 0..warmup {
            black_box(world.frame(&mut executor, schedule, rounds));
        }
        let mut samples = Vec::with_capacity(frames);
        let mut checksum = 0_u64;
        for _ in 0..frames {
            let start = Instant::now();
            let draws = world.frame(&mut executor, schedule, rounds);
            samples.push(start.elapsed().as_secs_f64() * 1e6);
            for draw in draws {
                checksum = checksum.wrapping_add(black_box(draw.value));
            }
        }
        let background_blocks = background.map_or(0, Background::finish);
        if let Some(expected) = reference_checksum {
            assert_eq!(checksum, expected);
        }
        reference_checksum = Some(checksum);
        if let Some(file) = raw.as_mut() {
            for (frame, elapsed) in samples.iter().enumerate() {
                writeln!(file, "{schedule:?},{frame},{elapsed:.3}").unwrap();
            }
        }
        samples.sort_by(f64::total_cmp);
        let percentile =
            |percent: usize| samples[(frames * percent).div_ceil(100).saturating_sub(1)];
        println!(
            "{schedule:?},{instances},{chunk},{workers},{rounds},{frames},{warmup},{:.3},{:.3},{:.3},{:.3},{:.3},{checksum},{background_blocks}",
            percentile(50),
            percentile(95),
            percentile(99),
            samples[frames - 1],
            samples.iter().sum::<f64>() / frames as f64
        );
        executor.runtime.shutdown().unwrap();
    }
    if let Some(mut file) = raw {
        file.flush().unwrap();
    }
}
