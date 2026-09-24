mod common;

use common::world::{Background, Executor, Schedule, World};

#[test]
fn moving_combat_frames_match_serial_under_competing_background_preparation() {
    // Odd and uneven partitions exercise stage completion and canonical merging.
    for (instances, chunk) in [(33, 7), (256, 32)] {
        let mut executor = Executor::new(1);
        let mut reference = World::new(instances, chunk);
        let mut worlds: Vec<_> = Schedule::ALL
            .into_iter()
            .map(|schedule| (schedule, World::new(instances, chunk)))
            .collect();
        let background = Background::start(&executor.runtime);
        for frame in 0..24_u64 {
            let expected = reference.frame(&mut executor, Schedule::Serial, 4).to_vec();
            for (schedule, world) in &mut worlds {
                assert_eq!(
                    world.frame(&mut executor, *schedule, 4),
                    expected,
                    "{schedule:?}, frame {frame}, instances {instances}, chunk {chunk}"
                );
            }
        }
        assert!(background.finish() > 0);
        executor.runtime.shutdown().unwrap();
    }
}

#[path = "common/bursts.rs"]
mod bursts;

#[test]
fn bursts_preserve_rejected_work_publish_once_and_recover() {
    for pressure in bursts::Pressure::ALL {
        for schedule in Schedule::ALL {
            let config = bursts::Config {
                baseline_frames: 4,
                burst_frames: 2,
                burst_arrivals: 24,
                settled_frames: 8,
                frame_rounds: 2,
                spike_multiplier: 8,
                job_rounds: 32768,
            };
            let report = bursts::run(schedule, pressure, config, true);
            assert_eq!(report.generated, report.published);
            assert!(report.peak_backlog > pressure.record_limit().min(8));
            assert!(report.peak_records <= pressure.record_limit());
            assert!(report.recovery_frames > 0 && report.recovery_us >= 0.0);
            assert!(report.cpu_recovery_frames <= report.recovery_frames);
            assert!(report.cpu_recovery_us <= report.recovery_us);
            assert!(
                report
                    .samples
                    .iter()
                    .any(|sample| sample.admission_retries > 0)
            );
            assert!(
                report
                    .samples
                    .iter()
                    .any(|sample| sample.delivery_retries > 0)
            );
            assert!(
                report
                    .samples
                    .iter()
                    .all(|sample| sample.scheduler_records <= pressure.record_limit())
            );
            assert!(
                report
                    .samples
                    .iter()
                    .filter(|sample| sample.phase == "settled")
                    .all(|sample| sample.outstanding == 0
                        && sample.pending_cpu == 0
                        && sample.unobserved_tasks == 0
                        && sample.pending_publication == 0
                        && sample.scheduler_records == 0)
            );
            assert!(
                report
                    .samples
                    .iter()
                    .all(|sample| sample.frame_us.is_finite() && sample.frame_us >= 0.0)
            );
            if !matches!(schedule, Schedule::RetainedWaves) {
                assert!(
                    report
                        .samples
                        .iter()
                        .all(|sample| sample.frame_retries == 0)
                );
            }
            assert_ne!(report.checksum, 0);
        }
    }
}
