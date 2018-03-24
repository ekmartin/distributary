use petgraph;
use std::cell;

// core types
pub use core::*;
pub use processing::{Ingredient, Miss, ProcessingResult, RawProcessingResult, ReplayContext};
pub use ops::NodeOperator;

// graph types
pub use node::{MaterializationStatus, Node};
pub type Edge = ();
pub type Graph = petgraph::Graph<Node, Edge>;

// dataflow types
pub use payload::{Link, Packet, PacketEvent, ReplayPathSegment, Tracer, TransactionState, SourceChannelIdentifier};
pub use Sharding;

// domain local state
pub type DomainNodes = Map<cell::RefCell<Node>>;
pub type DomainIndex = domain::Index;
pub type ReplicaAddr = (domain::Index, usize);

// channel related types
use channel;
use domain;
// Channel coordinator type specialized for domains
pub type ChannelCoordinator = channel::ChannelCoordinator<(DomainIndex, usize)>;
pub trait Executor {
    fn send_back(&self, SourceChannelIdentifier, Result<i64, ()>);
}

// debug types
pub use debug::DebugEvent;
