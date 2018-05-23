#[macro_use]
extern crate clap;
extern crate distributary;
extern crate futures;
extern crate futures_state_stream;
extern crate memcached;
extern crate mysql;
extern crate rayon;
extern crate tiberius;
extern crate tokio_core;

mod clients;

use clients::localsoup::graph::RECIPE;
use distributary::{
    ControllerBuilder, ControllerHandle, DataType, NodeIndex, PersistenceParameters,
    ZookeeperAuthority,
};
use std::fs;
use std::sync::Arc;
use std::thread;
use std::time::{self, Duration, Instant};
use std::u64;

macro_rules! dur_to_millis {
    ($d:expr) => {{
        $d.as_secs() * 1_000 + $d.subsec_nanos() as u64 / 1_000_000
    }};
}

#[derive(Clone)]
struct Setup {
    pub sharding: Option<usize>,
    pub logging: bool,
    pub persistence_params: PersistenceParameters,
}

impl Setup {
    pub fn new(persistence_params: PersistenceParameters) -> Self {
        Setup {
            sharding: None,
            logging: false,
            persistence_params,
        }
    }
}

struct Graph {
    pub vote: NodeIndex,
    pub article: NodeIndex,
    pub end: NodeIndex,
    pub graph: ControllerHandle<ZookeeperAuthority>,
}

fn make(s: Setup, authority: Arc<ZookeeperAuthority>) -> Graph {
    let mut g = ControllerBuilder::default();
    g.set_sharding(s.sharding);
    g.set_persistence(s.persistence_params.clone());
    g.set_worker_threads(1);
    g.set_read_threads(1);
    if s.logging {
        g.log_with(distributary::logger_pls());
    }

    let mut graph = g.build(authority);
    graph.install_recipe(RECIPE.to_owned()).unwrap();
    let inputs = graph.inputs();
    let outputs = graph.outputs();

    if s.logging {
        println!("inputs {:?}", inputs);
        println!("outputs {:?}", outputs);
    }

    Graph {
        vote: inputs["Vote"],
        article: inputs["Article"],
        end: outputs["ArticleWithVoteCount"],
        graph,
    }
}

// Block until writes have finished.
// This performs a read and checks that the result corresponds to the total amount of votes.
// TODO(ekmartin): Would be nice if there's a way we can do this without having to poll
// Soup for results.
fn wait_for_writes(mut getter: distributary::RemoteGetter, narticles: usize, nvotes: usize) {
    loop {
        let keys: Vec<Vec<_>> = (0..narticles as i64)
            .map(|i| vec![DataType::BigInt(i)])
            .collect();
        let rows = getter.multi_lookup(keys, true);
        let sum: i64 = rows
            .unwrap()
            .into_iter()
            .map(|row| {
                if row.is_empty() {
                    return 0;
                }

                match row[0][2] {
                    DataType::None => 0,
                    DataType::BigInt(i) => i,
                    DataType::Int(i) => i as i64,
                    _ => unreachable!(),
                }
            })
            .sum();

        if sum == nvotes as i64 {
            return;
        }

        thread::sleep(Duration::from_millis(50));
    }
}

fn pre_recovery(
    s: Setup,
    narticles: usize,
    nvotes: usize,
    verbose: bool,
    authority: Arc<ZookeeperAuthority>,
) {
    let mut g = make(s, authority);
    let mut articles = g.graph.get_mutator("Article").unwrap();
    let mut votes = g.graph.get_mutator("Vote").unwrap();

    // prepopulate
    if verbose {
        eprintln!("Populating with {} articles", narticles);
    }

    let a_rows: Vec<_> = (0..(narticles as i64))
        .map(|i| vec![DataType::BigInt(i), format!("Article #{}", i).into()])
        .collect();
    articles.batch_put(a_rows).unwrap();

    if verbose {
        eprintln!("Populating with {} votes", nvotes);
    }

    let v_rows: Vec<_> = (0..nvotes)
        .map(|i| vec![DataType::BigInt((i % narticles) as i64), i.into()])
        .collect();
    votes.batch_put(v_rows).unwrap();
}

fn main() {
    use clap::{App, Arg};

    let args = App::new("vote")
        .version("0.1")
        .about("Benchmarks the recovery time of a user-curated news aggregator")
        .arg(
            Arg::with_name("narticles")
                .short("a")
                .long("articles")
                .value_name("N")
                .default_value("100000")
                .help("Number of articles to prepopulate the database with"),
        )
        .arg(
            Arg::with_name("nvotes")
                .short("v")
                .long("votes")
                .default_value("1000000")
                .help("Number of votes to prepopulate the database with"),
        )
        .arg(
            Arg::with_name("zookeeper-address")
                .long("zookeeper-address")
                .takes_value(true)
                .default_value("127.0.0.1:2181/recovery")
                .help("ZookeeperAuthority address"),
        )
        .arg(Arg::with_name("verbose").long("verbose").short("v"))
        .get_matches();

    let narticles = value_t_or_exit!(args, "narticles", usize);
    let nvotes = value_t_or_exit!(args, "nvotes", usize);
    let verbose = args.is_present("verbose");
    let name = "vote_recovery";
    let persistence_params = distributary::PersistenceParameters::new(
        distributary::DurabilityMode::Permanent,
        512,
        Duration::from_millis(10),
        Some(String::from(name)),
        4,
    );

    let mut s = Setup::new(persistence_params);
    s.logging = verbose;
    s.sharding = None;
    let zk_address = args.value_of("zookeeper-address").unwrap();
    let authority = Arc::new(ZookeeperAuthority::new(zk_address));
    // Prepopulate with narticles and nvotes:
    pre_recovery(s.clone(), narticles, nvotes, verbose, authority.clone());

    if verbose {
        eprintln!("Done populating state, now recovering...");
    }

    let start = Instant::now();
    let mut g = make(s, authority);
    let mut getter = g.graph.get_getter("ArticleWithVoteCount").unwrap();
    let rows = getter.lookup(&[DataType::BigInt(0)], true).unwrap();
    println!(
        "Initial Read Time (ms): {}",
        dur_to_millis!(start.elapsed())
    );

    let count = match rows[0][2] {
        DataType::None => 0,
        DataType::BigInt(i) => i,
        DataType::Int(i) => i as i64,
        _ => unreachable!(),
    };

    assert_eq!(count, (nvotes as i64) / (narticles as i64));

    wait_for_writes(getter, narticles, nvotes);
    let total_elapsed = dur_to_millis!(start.elapsed());
    println!("Total Recovery Time (ms): {}", total_elapsed);

    fs::remove_dir_all(format!("./{}-Article-0.db", name)).unwrap();
    fs::remove_dir_all(format!("./{}-Vote-0.db", name)).unwrap();
}
