#![feature(box_syntax)]
#![feature(conservative_impl_trait)]
#![feature(entry_or_default)]
#![feature(try_from)]
#![deny(unused_extern_crates)]

extern crate arccstr;
extern crate chrono;
extern crate fnv;
extern crate nom_sql;
extern crate petgraph;
extern crate serde;
#[macro_use]
extern crate serde_derive;

#[cfg(any(feature = "web", test))]
#[macro_use]
extern crate serde_json;

pub mod addressing;
pub mod data;
pub mod local;

pub use data::{DataType, Datas, Record, Records};
pub use addressing::{IndexPair, LocalNodeIndex};
pub use local::{KeyType, LookupResult, Map, MemoryState, Row, State, Tag};
pub use petgraph::graph::NodeIndex;

pub type StateMap = local::Map<State>;
