use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::time::Instant;

use cfb::CompoundFile;
use yak::{CreateOptions, OpenMode, YakDefault as Yak};

const YAK_FILE: &str = "_bench.yak";
const CFB_FILE: &str = "_bench.cfb";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
enum Only {
    Both,
    Yak,
    Cfb,
}

struct Config {
    scenario: String,
    only: Only,
    repeat: usize,
}

impl Config {
    fn run_yak(&self) -> bool {
        self.only != Only::Cfb
    }
    fn run_cfb(&self) -> bool {
        self.only != Only::Yak
    }
}

// ---------------------------------------------------------------------------
// Results accumulator
// ---------------------------------------------------------------------------

struct Accumulator {
    total_yak_ms: f64,
    total_cfb_ms: f64,
}

impl Accumulator {
    fn new() -> Self {
        Self { total_yak_ms: 0.0, total_cfb_ms: 0.0 }
    }

    fn add(&mut self, yak_ms: f64, cfb_ms: f64) {
        self.total_yak_ms += yak_ms;
        self.total_cfb_ms += cfb_ms;
    }

    fn print_summary(&self, only: Only) {
        let green = "\x1b[32m";
        let red = "\x1b[31m";
        let bold = "\x1b[1m";
        let reset = "\x1b[0m";

        eprintln!("{}=== Weighted Overall ==={}", bold, reset);

        match only {
            Only::Yak => {
                eprintln!("  yak total: {:.0} ms", self.total_yak_ms);
            }
            Only::Cfb => {
                eprintln!("  cfb total: {:.0} ms", self.total_cfb_ms);
            }
            Only::Both => {
                eprintln!(
                    "  yak total: {:>8.0} ms    cfb total: {:>8.0} ms",
                    self.total_yak_ms, self.total_cfb_ms
                );
                if self.total_yak_ms > 0.0 && self.total_cfb_ms > 0.0 {
                    let ratio = self.total_cfb_ms / self.total_yak_ms;
                    let (color, winner, factor) = if ratio > 1.0 {
                        (green, "yak", ratio)
                    } else {
                        (red, "cfb", 1.0 / ratio)
                    };
                    eprintln!(
                        "  {}{}{} is {:.2}x faster overall{}",
                        bold, color, winner, factor, reset
                    );
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

struct CleanupGuard<'a> {
    paths: &'a [&'a str],
}

impl Drop for CleanupGuard<'_> {
    fn drop(&mut self) {
        for p in self.paths {
            let _ = std::fs::remove_file(p);
        }
    }
}

fn make_buffer(size: usize) -> Vec<u8> {
    (0u8..=255).cycle().take(size).collect()
}

fn mb_per_sec(bytes: usize, ms: f64) -> f64 {
    let mb = bytes as f64 / (1024.0 * 1024.0);
    mb / (ms / 1000.0)
}

fn print_result(
    scenario: &str,
    yak_ms: f64,
    cfb_ms: f64,
    total_bytes: Option<usize>,
    only: Only,
) {
    let green = "\x1b[32m";
    let red = "\x1b[31m";
    let reset = "\x1b[0m";

    match only {
        Only::Yak => match total_bytes {
            Some(bytes) => eprintln!(
                "  {:<20} yak: {:>8.0} ms ({:>7.1} MB/s)",
                scenario, yak_ms, mb_per_sec(bytes, yak_ms),
            ),
            None => eprintln!("  {:<20} yak: {:>8.0} ms", scenario, yak_ms),
        },
        Only::Cfb => match total_bytes {
            Some(bytes) => eprintln!(
                "  {:<20} cfb: {:>8.0} ms ({:>7.1} MB/s)",
                scenario, cfb_ms, mb_per_sec(bytes, cfb_ms),
            ),
            None => eprintln!("  {:<20} cfb: {:>8.0} ms", scenario, cfb_ms),
        },
        Only::Both => {
            let ratio = cfb_ms / yak_ms;
            let yak_faster = ratio > 1.0;
            let (color, faster, factor) = if yak_faster {
                (green, "yak", ratio)
            } else {
                (red, "cfb", 1.0 / ratio)
            };
            let winner = format!("{}{} {:.2}x faster{}", color, faster, factor, reset);

            match total_bytes {
                Some(bytes) => eprintln!(
                    "  {:<20} yak: {:>8.0} ms ({:>7.1} MB/s)   cfb: {:>8.0} ms ({:>7.1} MB/s)   {}",
                    scenario, yak_ms, mb_per_sec(bytes, yak_ms),
                    cfb_ms, mb_per_sec(bytes, cfb_ms), winner,
                ),
                None => eprintln!(
                    "  {:<20} yak: {:>8.0} ms                  cfb: {:>8.0} ms                  {}",
                    scenario, yak_ms, cfb_ms, winner,
                ),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Yak helpers
// ---------------------------------------------------------------------------

fn create_yak(path: &str) -> Yak {
    Yak::create(
        path,
        CreateOptions {
            block_index_width: 4,
            block_size_shift: 12,
            compressed_block_size_shift: 12,
            password: None,
        },
    )
    .expect("failed to create yak file")
}

fn open_yak(path: &str, mode: OpenMode) -> Yak {
    Yak::open(path, mode).expect("failed to open yak file")
}

// ---------------------------------------------------------------------------
// CFB helpers
// ---------------------------------------------------------------------------

fn create_cfb(path: &str) -> CompoundFile<std::fs::File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .expect("failed to create cfb backing file");
    CompoundFile::create(file).expect("failed to create cfb compound file")
}

fn open_cfb_ro(path: &str) -> CompoundFile<std::fs::File> {
    let file = OpenOptions::new()
        .read(true)
        .open(path)
        .expect("failed to open cfb backing file");
    CompoundFile::open(file).expect("failed to open cfb compound file")
}

// ---------------------------------------------------------------------------
// Repeat helper — runs a bench function N times, returns min of each side
// ---------------------------------------------------------------------------

fn repeat_bench(
    repeat: usize,
    f: impl Fn() -> (f64, f64),
) -> (f64, f64) {
    let mut best_yak = f64::MAX;
    let mut best_cfb = f64::MAX;
    for _ in 0..repeat {
        let (y, c) = f();
        if y > 0.0 && y < best_yak {
            best_yak = y;
        }
        if c > 0.0 && c < best_cfb {
            best_cfb = c;
        }
    }
    (
        if best_yak == f64::MAX { 0.0 } else { best_yak },
        if best_cfb == f64::MAX { 0.0 } else { best_cfb },
    )
}

fn repeat_bench4(
    repeat: usize,
    f: impl Fn() -> (f64, f64, f64, f64),
) -> (f64, f64, f64, f64) {
    let mut best = [f64::MAX; 4];
    for _ in 0..repeat {
        let (a, b, c, d) = f();
        let vals = [a, b, c, d];
        for i in 0..4 {
            if vals[i] > 0.0 && vals[i] < best[i] {
                best[i] = vals[i];
            }
        }
    }
    for v in &mut best {
        if *v == f64::MAX {
            *v = 0.0;
        }
    }
    (best[0], best[1], best[2], best[3])
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

/// Write 5 streams of 30MB each.
fn bench_large_write(run_yak: bool, run_cfb: bool) -> (f64, f64) {
    let _guard = CleanupGuard { paths: &[YAK_FILE, CFB_FILE] };
    let buf = make_buffer(30 * 1024 * 1024);

    let yak_ms = if run_yak {
        let t0 = Instant::now();
        let sfs = create_yak(YAK_FILE);
        for i in 0..5 {
            let name = format!("large_{}.bin", i);
            let handle = sfs.create_stream(&name, false).unwrap();
            sfs.write(&handle, &buf).unwrap();
            sfs.close_stream(handle).unwrap();
        }
        sfs.close().unwrap();
        t0.elapsed().as_secs_f64() * 1000.0
    } else {
        0.0
    };

    let _ = std::fs::remove_file(YAK_FILE);

    let cfb_ms = if run_cfb {
        let t0 = Instant::now();
        let mut comp = create_cfb(CFB_FILE);
        for i in 0..5 {
            let name = format!("/large_{}.bin", i);
            let mut stream = comp.create_stream(&name).unwrap();
            stream.write_all(&buf).unwrap();
        }
        comp.flush().unwrap();
        t0.elapsed().as_secs_f64() * 1000.0
    } else {
        0.0
    };

    (yak_ms, cfb_ms)
}

/// Write 180 streams of 10KB each.
fn bench_small_write(run_yak: bool, run_cfb: bool) -> (f64, f64) {
    let _guard = CleanupGuard { paths: &[YAK_FILE, CFB_FILE] };
    let buf = make_buffer(10 * 1024);

    let yak_ms = if run_yak {
        let t0 = Instant::now();
        let sfs = create_yak(YAK_FILE);
        for i in 0..180 {
            let name = format!("stream_{:04}.bin", i);
            let handle = sfs.create_stream(&name, false).unwrap();
            sfs.write(&handle, &buf).unwrap();
            sfs.close_stream(handle).unwrap();
        }
        sfs.close().unwrap();
        t0.elapsed().as_secs_f64() * 1000.0
    } else {
        0.0
    };

    let _ = std::fs::remove_file(YAK_FILE);

    let cfb_ms = if run_cfb {
        let t0 = Instant::now();
        let mut comp = create_cfb(CFB_FILE);
        for i in 0..180 {
            let name = format!("/stream_{:04}.bin", i);
            let mut stream = comp.create_stream(&name).unwrap();
            stream.write_all(&buf).unwrap();
        }
        comp.flush().unwrap();
        t0.elapsed().as_secs_f64() * 1000.0
    } else {
        0.0
    };

    (yak_ms, cfb_ms)
}

/// Read back 17x10MB + 17x20MB (510MB total).
fn bench_large_read(run_yak: bool, run_cfb: bool) -> (f64, f64) {
    let _guard = CleanupGuard { paths: &[YAK_FILE, CFB_FILE] };

    let size_10mb = 10 * 1024 * 1024usize;
    let size_20mb = 20 * 1024 * 1024usize;
    let streams_per_tier = 17usize;

    let yak_ms = if run_yak {
        // Populate
        {
            let sfs = create_yak(YAK_FILE);
            let buf_10 = make_buffer(size_10mb);
            for i in 0..streams_per_tier {
                let name = format!("large_10m_{}.bin", i);
                let handle = sfs.create_stream(&name, false).unwrap();
                sfs.write(&handle, &buf_10).unwrap();
                sfs.close_stream(handle).unwrap();
            }
            let buf_20 = make_buffer(size_20mb);
            for i in 0..streams_per_tier {
                let name = format!("large_20m_{}.bin", i);
                let handle = sfs.create_stream(&name, false).unwrap();
                sfs.write(&handle, &buf_20).unwrap();
                sfs.close_stream(handle).unwrap();
            }
            sfs.close().unwrap();
        }

        // Read (timed)
        let t0 = Instant::now();
        let sfs = open_yak(YAK_FILE, OpenMode::Read);
        for i in 0..streams_per_tier {
            let name = format!("large_10m_{}.bin", i);
            let handle = sfs.open_stream(&name, OpenMode::Read).unwrap();
            let len = sfs.stream_length(&handle).unwrap() as usize;
            let mut buf = vec![0u8; len];
            sfs.read(&handle, &mut buf).unwrap();
            sfs.close_stream(handle).unwrap();
        }
        for i in 0..streams_per_tier {
            let name = format!("large_20m_{}.bin", i);
            let handle = sfs.open_stream(&name, OpenMode::Read).unwrap();
            let len = sfs.stream_length(&handle).unwrap() as usize;
            let mut buf = vec![0u8; len];
            sfs.read(&handle, &mut buf).unwrap();
            sfs.close_stream(handle).unwrap();
        }
        sfs.close().unwrap();
        t0.elapsed().as_secs_f64() * 1000.0
    } else {
        0.0
    };

    let _ = std::fs::remove_file(YAK_FILE);

    let cfb_ms = if run_cfb {
        // Populate
        {
            let mut comp = create_cfb(CFB_FILE);
            let buf_10 = make_buffer(size_10mb);
            for i in 0..streams_per_tier {
                let name = format!("/large_10m_{}.bin", i);
                let mut stream = comp.create_stream(&name).unwrap();
                stream.write_all(&buf_10).unwrap();
            }
            let buf_20 = make_buffer(size_20mb);
            for i in 0..streams_per_tier {
                let name = format!("/large_20m_{}.bin", i);
                let mut stream = comp.create_stream(&name).unwrap();
                stream.write_all(&buf_20).unwrap();
            }
            comp.flush().unwrap();
        }

        // Read (timed)
        let t0 = Instant::now();
        let mut comp = open_cfb_ro(CFB_FILE);
        for i in 0..streams_per_tier {
            let name = format!("/large_10m_{}.bin", i);
            let mut stream = comp.open_stream(&name).unwrap();
            let mut buf = vec![0u8; size_10mb];
            stream.read_to_end(&mut buf).unwrap();
        }
        for i in 0..streams_per_tier {
            let name = format!("/large_20m_{}.bin", i);
            let mut stream = comp.open_stream(&name).unwrap();
            let mut buf = vec![0u8; size_20mb];
            stream.read_to_end(&mut buf).unwrap();
        }
        t0.elapsed().as_secs_f64() * 1000.0
    } else {
        0.0
    };

    (yak_ms, cfb_ms)
}

/// Read back 2750 streams of 10KB each.
fn bench_small_read(run_yak: bool, run_cfb: bool) -> (f64, f64) {
    let _guard = CleanupGuard { paths: &[YAK_FILE, CFB_FILE] };

    let stream_count = 2750usize;
    let stream_size = 10 * 1024usize;

    let yak_ms = if run_yak {
        // Populate
        {
            let sfs = create_yak(YAK_FILE);
            let buf = make_buffer(stream_size);
            for i in 0..stream_count {
                let name = format!("stream_{:04}.bin", i);
                let handle = sfs.create_stream(&name, false).unwrap();
                sfs.write(&handle, &buf).unwrap();
                sfs.close_stream(handle).unwrap();
            }
            sfs.close().unwrap();
        }

        // Read (timed)
        let t0 = Instant::now();
        let sfs = open_yak(YAK_FILE, OpenMode::Read);
        for i in 0..stream_count {
            let name = format!("stream_{:04}.bin", i);
            let handle = sfs.open_stream(&name, OpenMode::Read).unwrap();
            let len = sfs.stream_length(&handle).unwrap() as usize;
            let mut buf = vec![0u8; len];
            sfs.read(&handle, &mut buf).unwrap();
            sfs.close_stream(handle).unwrap();
        }
        sfs.close().unwrap();
        t0.elapsed().as_secs_f64() * 1000.0
    } else {
        0.0
    };

    let _ = std::fs::remove_file(YAK_FILE);

    let cfb_ms = if run_cfb {
        // Populate
        {
            let mut comp = create_cfb(CFB_FILE);
            let buf = make_buffer(stream_size);
            for i in 0..stream_count {
                let name = format!("/stream_{:04}.bin", i);
                let mut stream = comp.create_stream(&name).unwrap();
                stream.write_all(&buf).unwrap();
            }
            comp.flush().unwrap();
        }

        // Read (timed)
        let t0 = Instant::now();
        let mut comp = open_cfb_ro(CFB_FILE);
        for i in 0..stream_count {
            let name = format!("/stream_{:04}.bin", i);
            let mut stream = comp.open_stream(&name).unwrap();
            let mut buf = vec![0u8; stream_size];
            stream.read_to_end(&mut buf).unwrap();
        }
        t0.elapsed().as_secs_f64() * 1000.0
    } else {
        0.0
    };

    (yak_ms, cfb_ms)
}

/// Write 20 streams of 512KB then delete them all, repeated 5 times.
fn bench_churn(run_yak: bool, run_cfb: bool) -> (f64, f64) {
    let _guard = CleanupGuard { paths: &[YAK_FILE, CFB_FILE] };
    let buf = make_buffer(512 * 1024);
    let n = 20usize;

    let yak_ms = if run_yak {
        let t0 = Instant::now();
        let sfs = create_yak(YAK_FILE);
        for _ in 0..5 {
            for i in 0..n {
                let name = format!("churn_{:04}.bin", i);
                let handle = sfs.create_stream(&name, false).unwrap();
                sfs.write(&handle, &buf).unwrap();
                sfs.close_stream(handle).unwrap();
            }
            for i in 0..n {
                let name = format!("churn_{:04}.bin", i);
                sfs.delete_stream(&name).unwrap();
            }
        }
        sfs.close().unwrap();
        t0.elapsed().as_secs_f64() * 1000.0
    } else {
        0.0
    };

    let _ = std::fs::remove_file(YAK_FILE);

    let cfb_ms = if run_cfb {
        let t0 = Instant::now();
        let mut comp = create_cfb(CFB_FILE);
        for _ in 0..5 {
            for i in 0..n {
                let name = format!("/churn_{:04}.bin", i);
                let mut stream = comp.create_stream(&name).unwrap();
                stream.write_all(&buf).unwrap();
            }
            for i in 0..n {
                let name = format!("/churn_{:04}.bin", i);
                comp.remove_stream(&name).unwrap();
            }
        }
        comp.flush().unwrap();
        t0.elapsed().as_secs_f64() * 1000.0
    } else {
        0.0
    };

    (yak_ms, cfb_ms)
}

/// Write + read one 64MB stream. Returns (yak_write, yak_read, cfb_write, cfb_read).
fn bench_single_stream(run_yak: bool, run_cfb: bool) -> (f64, f64, f64, f64) {
    let _guard = CleanupGuard { paths: &[YAK_FILE, CFB_FILE] };
    let stream_size = 64 * 1024 * 1024usize;
    let buf = make_buffer(stream_size);

    let (yak_write_ms, yak_read_ms) = if run_yak {
        // Write
        let t_w = Instant::now();
        let sfs = create_yak(YAK_FILE);
        let handle = sfs.create_stream("big.bin", false).unwrap();
        sfs.write(&handle, &buf).unwrap();
        sfs.close_stream(handle).unwrap();
        sfs.close().unwrap();
        let w = t_w.elapsed().as_secs_f64() * 1000.0;

        // Read
        let t_r = Instant::now();
        let sfs = open_yak(YAK_FILE, OpenMode::Read);
        let handle = sfs.open_stream("big.bin", OpenMode::Read).unwrap();
        let len = sfs.stream_length(&handle).unwrap() as usize;
        let mut read_buf = vec![0u8; len];
        sfs.read(&handle, &mut read_buf).unwrap();
        sfs.close_stream(handle).unwrap();
        sfs.close().unwrap();
        let r = t_r.elapsed().as_secs_f64() * 1000.0;
        (w, r)
    } else {
        (0.0, 0.0)
    };

    let _ = std::fs::remove_file(YAK_FILE);

    let (cfb_write_ms, cfb_read_ms) = if run_cfb {
        // Write
        let t_w = Instant::now();
        let mut comp = create_cfb(CFB_FILE);
        let mut stream = comp.create_stream("/big.bin").unwrap();
        stream.write_all(&buf).unwrap();
        drop(stream);
        comp.flush().unwrap();
        let w = t_w.elapsed().as_secs_f64() * 1000.0;

        // Read
        let t_r = Instant::now();
        let mut comp = open_cfb_ro(CFB_FILE);
        let mut stream = comp.open_stream("/big.bin").unwrap();
        let mut read_buf = vec![0u8; stream_size];
        stream.read_to_end(&mut read_buf).unwrap();
        let r = t_r.elapsed().as_secs_f64() * 1000.0;
        (w, r)
    } else {
        (0.0, 0.0)
    };

    (yak_write_ms, yak_read_ms, cfb_write_ms, cfb_read_ms)
}

/// Write 2750 streams of 10KB, then read them back (same instance = warm cache).
fn bench_warm_read(run_yak: bool, run_cfb: bool) -> (f64, f64) {
    let _guard = CleanupGuard { paths: &[YAK_FILE, CFB_FILE] };

    let stream_count = 2750usize;
    let stream_size = 10 * 1024usize;

    let yak_ms = if run_yak {
        let sfs = create_yak(YAK_FILE);
        let buf = make_buffer(stream_size);
        for i in 0..stream_count {
            let name = format!("stream_{:04}.bin", i);
            let handle = sfs.create_stream(&name, false).unwrap();
            sfs.write(&handle, &buf).unwrap();
            sfs.close_stream(handle).unwrap();
        }
        let t0 = Instant::now();
        for i in 0..stream_count {
            let name = format!("stream_{:04}.bin", i);
            let handle = sfs.open_stream(&name, OpenMode::Read).unwrap();
            let len = sfs.stream_length(&handle).unwrap() as usize;
            let mut read_buf = vec![0u8; len];
            sfs.read(&handle, &mut read_buf).unwrap();
            sfs.close_stream(handle).unwrap();
        }
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        sfs.close().unwrap();
        ms
    } else {
        0.0
    };

    let _ = std::fs::remove_file(YAK_FILE);

    let cfb_ms = if run_cfb {
        let mut comp = create_cfb(CFB_FILE);
        let buf = make_buffer(stream_size);
        for i in 0..stream_count {
            let name = format!("/stream_{:04}.bin", i);
            let mut stream = comp.create_stream(&name).unwrap();
            stream.write_all(&buf).unwrap();
        }
        let t0 = Instant::now();
        for i in 0..stream_count {
            let name = format!("/stream_{:04}.bin", i);
            let mut stream = comp.open_stream(&name).unwrap();
            let mut read_buf = vec![0u8; stream_size];
            stream.read_to_end(&mut read_buf).unwrap();
        }
        t0.elapsed().as_secs_f64() * 1000.0
    } else {
        0.0
    };

    (yak_ms, cfb_ms)
}

/// Write 10MB then 5000 overwrites at pseudo-random positions.
fn bench_overwrite(run_yak: bool, run_cfb: bool) -> (f64, f64) {
    let _guard = CleanupGuard { paths: &[YAK_FILE, CFB_FILE] };

    let stream_size = 10 * 1024 * 1024usize;
    let write_count = 5000usize;
    let write_size = 4 * 1024usize;
    let initial_buf = make_buffer(stream_size);
    let write_buf = make_buffer(write_size);
    let max_pos = (stream_size - write_size) as u64;

    let yak_ms = if run_yak {
        let sfs = create_yak(YAK_FILE);
        let handle = sfs.create_stream("overwrite.bin", false).unwrap();
        sfs.write(&handle, &initial_buf).unwrap();

        let t0 = Instant::now();
        for i in 0..write_count {
            let pos = ((i as u64).wrapping_mul(2654435761) % max_pos) & !7;
            sfs.seek(&handle, pos).unwrap();
            sfs.write(&handle, &write_buf).unwrap();
        }
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        sfs.close_stream(handle).unwrap();
        sfs.close().unwrap();
        ms
    } else {
        0.0
    };

    let _ = std::fs::remove_file(YAK_FILE);

    let cfb_ms = if run_cfb {
        let mut comp = create_cfb(CFB_FILE);
        {
            let mut stream = comp.create_stream("/overwrite.bin").unwrap();
            stream.write_all(&initial_buf).unwrap();
        }

        let t0 = Instant::now();
        {
            let mut stream = comp.open_stream("/overwrite.bin").unwrap();
            for i in 0..write_count {
                let pos = ((i as u64).wrapping_mul(2654435761) % max_pos) & !7;
                stream.seek(SeekFrom::Start(pos)).unwrap();
                stream.write_all(&write_buf).unwrap();
            }
        }
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        comp.flush().unwrap();
        ms
    } else {
        0.0
    };

    (yak_ms, cfb_ms)
}

/// Create N streams, then time opening M by name (directory lookup).
fn bench_dir_lookup(run_yak: bool, run_cfb: bool) -> (f64, f64) {
    let _guard = CleanupGuard { paths: &[YAK_FILE, CFB_FILE] };

    let total_entries = 4_000usize;
    let lookups = 4_000usize;

    let yak_ms = if run_yak {
        eprint!("    populating {} entries (yak)...", total_entries);
        {
            let sfs = create_yak(YAK_FILE);
            for i in 0..total_entries {
                let name = format!("entry_{:05}.dat", i);
                let handle = sfs.create_stream(&name, false).unwrap();
                sfs.close_stream(handle).unwrap();
            }
            sfs.close().unwrap();
        }
        eprintln!(" done");

        let t0 = Instant::now();
        let sfs = open_yak(YAK_FILE, OpenMode::Read);
        for i in 0..lookups {
            let idx = ((i as u64).wrapping_mul(2654435761) % total_entries as u64) as usize;
            let name = format!("entry_{:05}.dat", idx);
            let handle = sfs.open_stream(&name, OpenMode::Read).unwrap();
            sfs.close_stream(handle).unwrap();
        }
        sfs.close().unwrap();
        t0.elapsed().as_secs_f64() * 1000.0
    } else {
        0.0
    };

    let _ = std::fs::remove_file(YAK_FILE);

    let cfb_ms = if run_cfb {
        eprint!("    populating {} entries (cfb)...", total_entries);
        {
            let mut comp = create_cfb(CFB_FILE);
            for i in 0..total_entries {
                let name = format!("/entry_{:05}.dat", i);
                comp.create_stream(&name).unwrap();
            }
            comp.flush().unwrap();
        }
        eprintln!(" done");

        let t0 = Instant::now();
        let mut comp = open_cfb_ro(CFB_FILE);
        for i in 0..lookups {
            let idx = ((i as u64).wrapping_mul(2654435761) % total_entries as u64) as usize;
            let name = format!("/entry_{:05}.dat", idx);
            let mut stream = comp.open_stream(&name).unwrap();
            let mut buf = Vec::new();
            stream.read_to_end(&mut buf).unwrap();
        }
        t0.elapsed().as_secs_f64() * 1000.0
    } else {
        0.0
    };

    (yak_ms, cfb_ms)
}

/// Fresh vs recycled-block read throughput. Returns (yak_fresh, yak_reuse, cfb_fresh, cfb_reuse).
fn bench_reuse(run_yak: bool, run_cfb: bool) -> (f64, f64, f64, f64) {
    let _guard = CleanupGuard { paths: &[YAK_FILE, CFB_FILE] };

    let stream_size = 512 * 1024usize;
    let n = 50usize;
    let buf = make_buffer(stream_size);

    let (yak_fresh_ms, yak_reuse_ms) = if run_yak {
        let sfs = create_yak(YAK_FILE);

        // Populate (fresh)
        for i in 0..n {
            let name = format!("reuse_{:04}.bin", i);
            let h = sfs.create_stream(&name, false).unwrap();
            sfs.write(&h, &buf).unwrap();
            sfs.close_stream(h).unwrap();
        }

        // Read baseline (fresh)
        let t_fresh = Instant::now();
        for i in 0..n {
            let name = format!("reuse_{:04}.bin", i);
            let h = sfs.open_stream(&name, OpenMode::Read).unwrap();
            let mut rbuf = vec![0u8; stream_size];
            sfs.read(&h, &mut rbuf).unwrap();
            sfs.close_stream(h).unwrap();
        }
        let fresh = t_fresh.elapsed().as_secs_f64() * 1000.0;

        // Delete all
        for i in 0..n {
            let name = format!("reuse_{:04}.bin", i);
            sfs.delete_stream(&name).unwrap();
        }

        // Repopulate (recycled blocks)
        for i in 0..n {
            let name = format!("reuse_{:04}.bin", i);
            let h = sfs.create_stream(&name, false).unwrap();
            sfs.write(&h, &buf).unwrap();
            sfs.close_stream(h).unwrap();
        }

        // Read after reuse
        let t_reuse = Instant::now();
        for i in 0..n {
            let name = format!("reuse_{:04}.bin", i);
            let h = sfs.open_stream(&name, OpenMode::Read).unwrap();
            let mut rbuf = vec![0u8; stream_size];
            sfs.read(&h, &mut rbuf).unwrap();
            sfs.close_stream(h).unwrap();
        }
        let reuse = t_reuse.elapsed().as_secs_f64() * 1000.0;
        sfs.close().unwrap();
        (fresh, reuse)
    } else {
        (0.0, 0.0)
    };

    let _ = std::fs::remove_file(YAK_FILE);

    let (cfb_fresh_ms, cfb_reuse_ms) = if run_cfb {
        let mut comp = create_cfb(CFB_FILE);

        // Populate (fresh)
        for i in 0..n {
            let name = format!("/reuse_{:04}.bin", i);
            let mut stream = comp.create_stream(&name).unwrap();
            stream.write_all(&buf).unwrap();
        }

        // Read baseline (fresh)
        let t_fresh = Instant::now();
        for i in 0..n {
            let name = format!("/reuse_{:04}.bin", i);
            let mut stream = comp.open_stream(&name).unwrap();
            let mut rbuf = vec![0u8; stream_size];
            stream.read_to_end(&mut rbuf).unwrap();
        }
        let fresh = t_fresh.elapsed().as_secs_f64() * 1000.0;

        // Delete all
        for i in 0..n {
            let name = format!("/reuse_{:04}.bin", i);
            comp.remove_stream(&name).unwrap();
        }

        // Repopulate (recycled)
        for i in 0..n {
            let name = format!("/reuse_{:04}.bin", i);
            let mut stream = comp.create_stream(&name).unwrap();
            stream.write_all(&buf).unwrap();
        }

        // Read after reuse
        let t_reuse = Instant::now();
        for i in 0..n {
            let name = format!("/reuse_{:04}.bin", i);
            let mut stream = comp.open_stream(&name).unwrap();
            let mut rbuf = vec![0u8; stream_size];
            stream.read_to_end(&mut rbuf).unwrap();
        }
        let reuse = t_reuse.elapsed().as_secs_f64() * 1000.0;
        (fresh, reuse)
    } else {
        (0.0, 0.0)
    };

    (yak_fresh_ms, yak_reuse_ms, cfb_fresh_ms, cfb_reuse_ms)
}

// ---------------------------------------------------------------------------
// Argument parsing & main
// ---------------------------------------------------------------------------

fn print_usage() {
    eprintln!("Usage: cfb-bench <scenario> [options]");
    eprintln!();
    eprintln!("Compares Yak vs rust-cfb on disk-based stream I/O benchmarks.");
    eprintln!("Both libraries use normal (unencrypted, uncompressed) mode.");
    eprintln!();
    eprintln!("Scenarios:");
    eprintln!("  large-write      Write 5 streams of 30MB each");
    eprintln!("  small-write      Write 180 streams of 10KB each");
    eprintln!("  large-read       Read back 17x10MB + 17x20MB (510MB total)");
    eprintln!("  small-read       Read back 2750 streams of 10KB each");
    eprintln!("  churn            Write then delete many streams (5 rounds)");
    eprintln!("  reuse            Fresh vs recycled-block read throughput");
    eprintln!("  single-stream    Write + read one 64MB stream");
    eprintln!("  warm-read        Write then read 2750x10KB (warm cache)");
    eprintln!("  overwrite        Write 10MB then 5000 overwrites");
    eprintln!("  dir-lookup       Open 4000 streams by name in a 4000-entry directory");
    eprintln!("  all              Run all scenarios in sequence");
    eprintln!();
    eprintln!("Options:");
    eprintln!("  --only yak|cfb   Run only one library");
    eprintln!("  --repeat N       Repeat each scenario N times, report best (default: 1)");
}

fn parse_args() -> Config {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        print_usage();
        std::process::exit(1);
    }

    let scenario = args[1].clone();
    let mut only = Only::Both;
    let mut repeat = 1usize;

    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "--only" => {
                i += 1;
                match args.get(i).map(|s| s.as_str()) {
                    Some("yak") => only = Only::Yak,
                    Some("cfb") => only = Only::Cfb,
                    _ => {
                        eprintln!("--only requires 'yak' or 'cfb'");
                        std::process::exit(1);
                    }
                }
            }
            "--repeat" => {
                i += 1;
                repeat = args
                    .get(i)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or_else(|| {
                        eprintln!("--repeat requires a positive number");
                        std::process::exit(1);
                    });
                if repeat == 0 {
                    repeat = 1;
                }
            }
            other => {
                eprintln!("Unknown option: {}", other);
                eprintln!();
                print_usage();
                std::process::exit(1);
            }
        }
        i += 1;
    }

    Config { scenario, only, repeat }
}

fn run_scenario(name: &str, cfg: &Config, acc: &mut Accumulator) {
    let ry = cfg.run_yak();
    let rc = cfg.run_cfb();
    let n = cfg.repeat;
    let repeat_label = if n > 1 {
        format!("  (best of {})", n)
    } else {
        String::new()
    };

    match name {
        "large-write" => {
            eprintln!("[large-write]  5 x 30MB{}", repeat_label);
            let (y, c) = repeat_bench(n, || bench_large_write(ry, rc));
            print_result("large-write", y, c, Some(5 * 30 * 1024 * 1024), cfg.only);
            acc.add(y, c);
        }
        "small-write" => {
            eprintln!("[small-write]  180 x 10KB{}", repeat_label);
            let (y, c) = repeat_bench(n, || bench_small_write(ry, rc));
            print_result("small-write", y, c, Some(180 * 10 * 1024), cfg.only);
            acc.add(y, c);
        }
        "large-read" => {
            eprintln!("[large-read]  17x10MB + 17x20MB = 510MB{}", repeat_label);
            let (y, c) = repeat_bench(n, || bench_large_read(ry, rc));
            print_result(
                "large-read", y, c,
                Some(17 * 10 * 1024 * 1024 + 17 * 20 * 1024 * 1024),
                cfg.only,
            );
            acc.add(y, c);
        }
        "small-read" => {
            eprintln!("[small-read]  2750 x 10KB{}", repeat_label);
            let (y, c) = repeat_bench(n, || bench_small_read(ry, rc));
            print_result("small-read", y, c, Some(2750 * 10 * 1024), cfg.only);
            acc.add(y, c);
        }
        "churn" => {
            eprintln!("[churn]  5 rounds x 20 x 512KB write+delete{}", repeat_label);
            let (y, c) = repeat_bench(n, || bench_churn(ry, rc));
            print_result("churn", y, c, Some(5 * 20 * 512 * 1024), cfg.only);
            acc.add(y, c);
        }
        "single-stream" => {
            eprintln!("[single-stream]  1 x 64MB write + read{}", repeat_label);
            let (yw, yr, cw, cr) = repeat_bench4(n, || bench_single_stream(ry, rc));
            let bytes = 64 * 1024 * 1024;
            print_result("single-stream W", yw, cw, Some(bytes), cfg.only);
            print_result("single-stream R", yr, cr, Some(bytes), cfg.only);
            acc.add(yw + yr, cw + cr);
        }
        "warm-read" => {
            eprintln!("[warm-read]  2750 x 10KB (same instance){}", repeat_label);
            let (y, c) = repeat_bench(n, || bench_warm_read(ry, rc));
            print_result("warm-read", y, c, Some(2750 * 10 * 1024), cfg.only);
            acc.add(y, c);
        }
        "overwrite" => {
            eprintln!("[overwrite]  10MB + 5000 x 4KB overwrites{}", repeat_label);
            let (y, c) = repeat_bench(n, || bench_overwrite(ry, rc));
            print_result("overwrite", y, c, Some(5000 * 4 * 1024), cfg.only);
            acc.add(y, c);
        }
        "dir-lookup" => {
            eprintln!("[dir-lookup]  4000 lookups in 4000-entry directory{}", repeat_label);
            let (y, c) = repeat_bench(n, || bench_dir_lookup(ry, rc));
            print_result("dir-lookup", y, c, None, cfg.only);
            acc.add(y, c);
        }
        "reuse" => {
            eprintln!("[reuse]  50 x 512KB: fresh vs recycled blocks{}", repeat_label);
            let (yf, yr, cf, cr) = repeat_bench4(n, || bench_reuse(ry, rc));
            let bytes = 50 * 512 * 1024;
            print_result("reuse (fresh)", yf, cf, Some(bytes), cfg.only);
            print_result("reuse (recycled)", yr, cr, Some(bytes), cfg.only);
            acc.add(yf + yr, cf + cr);
        }
        "all" => {
            let scenarios = [
                "large-write",
                "small-write",
                "large-read",
                "small-read",
                "churn",
                "single-stream",
                "warm-read",
                "overwrite",
                "dir-lookup",
                "reuse",
            ];
            eprintln!("=== Running all scenarios ===");
            eprintln!();
            for s in &scenarios {
                run_scenario(s, cfg, acc);
                eprintln!();
            }
            acc.print_summary(cfg.only);
        }
        other => {
            eprintln!("Unknown scenario: {}", other);
            eprintln!();
            print_usage();
            std::process::exit(1);
        }
    }
}

fn main() {
    let cfg = parse_args();
    let mut acc = Accumulator::new();
    run_scenario(&cfg.scenario, &cfg, &mut acc);
}
