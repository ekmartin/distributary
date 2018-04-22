#[macro_use]
extern crate clap;
extern crate distributary;
extern crate hdrhistogram;
extern crate itertools;
extern crate rand;

use std::thread;
use std::time::{Duration, Instant};
use std::path::PathBuf;

use clap::{App, Arg};
use hdrhistogram::Histogram;
use itertools::Itertools;

use distributary::{ControllerBuilder, ControllerHandle, DataType, DurabilityMode, LocalAuthority,
                   PersistenceParameters};

// If we .batch_put a huge amount of rows we'll end up with a deadlock when the base
// domains fill up their TCP buffers trying to send ACKs (which the batch putter
// isn't reading yet, since it's still busy sending).
const BATCH_SIZE: usize = 10000;

// Each row takes up 160 bytes with the current
// serialization scheme (32 for the key, 128 for the value).
const RECIPE: &str = "
CREATE TABLE TableRow (id int, c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 int, PRIMARY KEY(id));
QUERY ReadRow: SELECT * FROM TableRow WHERE id = ?;
";

fn populate(g: &mut ControllerHandle<LocalAuthority>, rows: i64, verbose: bool) {
    let mut mutator = g.get_mutator("TableRow").unwrap();

    // prepopulate
    if verbose {
        eprintln!("Populating with {} rows", rows);
    }

    (0..rows)
        .map(|i| {
            let row: Vec<DataType> = vec![i.into(); 10];
            row
        })
        .chunks(BATCH_SIZE)
        .into_iter()
        .for_each(|chunk| {
            let rs: Vec<Vec<DataType>> = chunk.collect();
            mutator.multi_put(rs).unwrap();
        });

    thread::sleep(Duration::from_secs(5));
}

fn perform_reads(g: &mut ControllerHandle<LocalAuthority>, reads: i64, rows: i64) {
    let mut hist = Histogram::<u64>::new_with_bounds(1, 100_000, 4).unwrap();
    let mut getter = g.get_getter("ReadRow").unwrap();

    let mut rng = rand::thread_rng();
    let row_ids = rand::seq::sample_iter(&mut rng, 0..rows, reads as usize).unwrap();
    // Synchronously read `reads` times, where each read should trigger a full replay from the base.
    for i in row_ids {
        let id: DataType = (i as i64).into();
        let start = Instant::now();
        let rs = getter.lookup(&[id], true).unwrap();
        let elapsed = start.elapsed();
        let us = elapsed.as_secs() * 1_000_000 + elapsed.subsec_nanos() as u64 / 1_000;
        assert_eq!(rs.len(), 1);
        for j in 0..10 {
            assert_eq!(DataType::BigInt(i), rs[0][j]);
        }

        if hist.record(us).is_err() {
            let m = hist.high();
            hist.record(m).unwrap();
        }
    }

    println!("# read {} of {} rows", reads, rows);
    println!("read\t50\t{:.2}\t(all µs)", hist.value_at_quantile(0.5));
    println!("read\t95\t{:.2}\t(all µs)", hist.value_at_quantile(0.95));
    println!("read\t99\t{:.2}\t(all µs)", hist.value_at_quantile(0.99));
    println!("read\t100\t{:.2}\t(all µs)", hist.max());
}

fn main() {
    let args = App::new("vote")
        .version("0.1")
        .about("Benchmarks the latency of full replays in a user-curated news aggregator")
        .arg(
            Arg::with_name("rows")
                .long("rows")
                .value_name("N")
                .default_value("100000")
                .help("Number of rows to prepopulate the database with"),
        )
        .arg(
            Arg::with_name("reads")
                .long("reads")
                .default_value("10000")
                .help("Number of rows to read while benchmarking"),
        )
        .arg(
            Arg::with_name("persist-bases")
                .long("persist-bases")
                .takes_value(false)
                .help("Persist base nodes to disk."),
        )
        .arg(
            Arg::with_name("log-dir")
                .long("log-dir")
                .takes_value(true)
                .help("Absolute path to the directory where the log files will be written."),
        )
        .arg(
            Arg::with_name("retain-logs-on-exit")
                .long("retain-logs-on-exit")
                .takes_value(false)
                .help("Do not delete the base node logs on exit."),
        )
        .arg(
            Arg::with_name("write-batch-size")
                .long("write-batch-size")
                .takes_value(true)
                .default_value("512")
                .help("Size of batches processed at base nodes."),
        )
        .arg(
            Arg::with_name("flush-timeout")
                .long("flush-timeout")
                .takes_value(true)
                .default_value("100000")
                .help("Time to wait before processing a merged packet, in nanoseconds."),
        )
        .arg(
            Arg::with_name("persistence-threads")
                .long("persistence-threads")
                .takes_value(true)
                .default_value("1")
                .help("Number of background threads used by PersistentState."),
        )
        .arg(
            Arg::with_name("shards")
                .long("shards")
                .takes_value(true)
                .default_value("0")
                .help("Shard the graph this many ways (0 = disable sharding)."),
        )
        .arg(Arg::with_name("verbose").long("verbose").short("v"))
        .get_matches();

    let reads = value_t_or_exit!(args, "reads", i64);
    let rows = value_t_or_exit!(args, "rows", i64);
    assert!(reads < rows);

    let verbose = args.is_present("verbose");
    let flush_ns = value_t_or_exit!(args, "flush-timeout", u32);

    let mut persistence = PersistenceParameters::default();
    persistence.flush_timeout = Duration::new(0, flush_ns);
    persistence.persistence_threads = value_t_or_exit!(args, "persistence-threads", i32);
    persistence.queue_capacity = value_t_or_exit!(args, "write-batch-size", usize);
    persistence.persist_base_nodes = args.is_present("persist-bases");
    persistence.log_prefix = "vote-replay".to_string();
    persistence.log_dir = args.value_of("log-dir")
        .and_then(|p| Some(PathBuf::from(p)));
    persistence.mode = if args.is_present("retain-logs-on-exit") {
        DurabilityMode::Permanent
    } else {
        DurabilityMode::DeleteOnExit
    };

    let mut builder = ControllerBuilder::default();
    let sharding = match value_t_or_exit!(args, "shards", usize) {
        0 => None,
        x => Some(x),
    };

    builder.set_read_threads(1);
    builder.set_worker_threads(2);
    builder.set_persistence(persistence);
    builder.set_sharding(sharding);
    if verbose {
        builder.log_with(distributary::logger_pls());
    }

    let mut g = builder.build_local();
    g.install_recipe(RECIPE.to_owned()).unwrap();

    // Prepopulate with n rows:
    populate(&mut g, rows, verbose);

    if verbose {
        eprintln!("Done populating state, now reading articles...");
    }

    perform_reads(&mut g, reads, rows);
}
