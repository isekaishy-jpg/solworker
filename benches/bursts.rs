#[path = "../tests/common/bursts.rs"]
mod bursts;
#[path = "../tests/common/mod.rs"]
mod common;

use common::world::Schedule;
use std::io::{BufWriter, Write};

fn main() {
    let repeats: usize = std::env::var("SW_BURST_REPEATS").map_or(4, |s| s.parse().unwrap());
    assert!(repeats > 0);
    let config = bursts::Config::default();
    {
        let mut executor = common::world::Executor::new(1);
        let background = common::world::Background::start(&executor.runtime);
        let mut world = common::world::World::new(256, 32);
        for _ in 0..20 {
            std::hint::black_box(world.frame(&mut executor, Schedule::ScopedChunks, 16));
        }
        background.finish();
        executor.runtime.shutdown().unwrap();
    }
    let output = std::env::var_os("SW_RUN_OUTPUT_DIR").map(std::path::PathBuf::from);
    let mut raw = output.as_ref().map(|path| {
        let mut file = BufWriter::new(std::fs::File::create(path.join("burst-frames.csv")).unwrap());
        writeln!(file, "repeat,schedule,pressure,frame,phase,frame_us,pending_cpu,unobserved_tasks,pending_publication,outstanding,scheduler_records,admission_retries,frame_retries,delivery_retries").unwrap();
        file
    });
    println!(
        "repeat,schedule,pressure,generated,published,checksum,peak_backlog,peak_records,cpu_recovery_frames,cpu_recovery_us,publication_recovery_frames,publication_recovery_us,admission_retries,frame_retries,delivery_retries,baseline_p50_us,burst_max_us,recovery_p99_us,settled_p50_us"
    );
    let selected = std::env::var("SW_BURST_SCHEDULE").ok();
    let pressure_filter = std::env::var("SW_BURST_PRESSURE").ok();
    assert!(
        selected
            .as_ref()
            .is_none_or(|name| Schedule::ALL.iter().any(|s| *name == format!("{s:?}")))
    );
    assert!(pressure_filter.as_ref().is_none_or(|name| {
        bursts::Pressure::ALL
            .iter()
            .any(|p| *name == format!("{p:?}"))
    }));
    for repeat in 0..repeats {
        for offset in 0..4 {
            let schedule = Schedule::ALL[(repeat + offset) % 4];
            if selected
                .as_ref()
                .is_some_and(|name| *name != format!("{schedule:?}"))
            {
                continue;
            }
            for p in 0..2 {
                let pressure = bursts::Pressure::ALL[(repeat + p) % 2];
                if pressure_filter
                    .as_ref()
                    .is_some_and(|name| *name != format!("{pressure:?}"))
                {
                    continue;
                }
                let report = bursts::run(schedule, pressure, config, false);
                let quantile = |phase: &str, percent: usize| {
                    let mut times: Vec<_> = report
                        .samples
                        .iter()
                        .filter(|s| s.phase == phase)
                        .map(|s| s.frame_us)
                        .collect();
                    times.sort_by(f64::total_cmp);
                    times[(times.len() * percent).div_ceil(100).saturating_sub(1)]
                };
                let admissions: usize = report.samples.iter().map(|s| s.admission_retries).sum();
                let frame_retries: usize = report.samples.iter().map(|s| s.frame_retries).sum();
                let deliveries: usize = report.samples.iter().map(|s| s.delivery_retries).sum();
                println!(
                    "{repeat},{schedule:?},{pressure:?},{},{},{},{},{},{},{:.3},{},{:.3},{admissions},{frame_retries},{deliveries},{:.3},{:.3},{:.3},{:.3}",
                    report.generated,
                    report.published,
                    report.checksum,
                    report.peak_backlog,
                    report.peak_records,
                    report.cpu_recovery_frames,
                    report.cpu_recovery_us,
                    report.recovery_frames,
                    report.recovery_us,
                    quantile("baseline", 50),
                    quantile("burst", 100),
                    quantile("recovery", 99),
                    quantile("settled", 50)
                );
                if let Some(file) = raw.as_mut() {
                    for (frame, s) in report.samples.iter().enumerate() {
                        writeln!(file, "{repeat},{schedule:?},{pressure:?},{frame},{},{:.3},{},{},{},{},{},{},{},{}",
                            s.phase, s.frame_us, s.pending_cpu, s.unobserved_tasks, s.pending_publication,
                            s.outstanding, s.scheduler_records, s.admission_retries, s.frame_retries, s.delivery_retries).unwrap();
                    }
                }
            }
        }
    }
    if let Some(mut file) = raw {
        file.flush().unwrap();
    }
}
