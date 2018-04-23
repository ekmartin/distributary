use node::NodeType;
use payload;
use prelude::*;
use std::collections::HashSet;

impl Node {
    pub(crate) fn process(
        &mut self,
        m: &mut Option<Box<Packet>>,
        keyed_by: Option<&Vec<usize>>,
        state: &mut StateMap,
        nodes: &DomainNodes,
        on_shard: Option<usize>,
        swap: bool,
        output: &mut Vec<(ReplicaAddr, Box<Packet>)>,
        executor: Option<&Executor>,
    ) -> (Vec<Miss>, HashSet<Vec<DataType>>) {
        m.as_mut().unwrap().trace(PacketEvent::Process);

        let addr = *self.local_addr();
        match self.inner {
            NodeType::Ingress => {
                let m = m.as_mut().unwrap();
                let tag = m.tag();
                m.map_data(|rs| {
                    materialize(rs, tag, state.get_mut(&addr));
                });
                (vec![], HashSet::new())
            }
            NodeType::Reader(ref mut r) => {
                r.process(m, swap);
                (vec![], HashSet::new())
            }
            NodeType::Egress(None) => unreachable!(),
            NodeType::Egress(Some(ref mut e)) => {
                e.process(m, on_shard.unwrap_or(0), output);
                (vec![], HashSet::new())
            }
            NodeType::Sharder(ref mut s) => {
                s.process(m, addr, on_shard.is_some(), output);
                (vec![], HashSet::new())
            }
            NodeType::Internal(ref mut i) => {
                let mut captured_full = false;
                let mut captured = HashSet::new();
                let mut misses = Vec::new();
                let mut tracer;

                {
                    let m = m.as_mut().unwrap();
                    let from = m.link().src;

                    let mut replay = match (&mut **m,) {
                        (&mut Packet::ReplayPiece {
                            context:
                                payload::ReplayPieceContext::Partial {
                                    ref mut for_keys,
                                    ignore,
                                },
                            ..
                        },) => {
                            use std::mem;
                            assert!(!ignore);
                            assert!(keyed_by.is_some());
                            ReplayContext::Partial {
                                key_cols: keyed_by.unwrap().clone(),
                                keys: mem::replace(for_keys, HashSet::new()),
                            }
                        }
                        (&mut Packet::ReplayPiece {
                            context: payload::ReplayPieceContext::Regular { last },
                            ..
                        },) => ReplayContext::Full { last: last },
                        _ => ReplayContext::None,
                    };

                    let mut set_replay_last = None;
                    tracer = m.tracer().and_then(|t| t.take());
                    m.map_data(|data| {
                        use std::mem;

                        // we need to own the data
                        let old_data = mem::replace(data, Records::default());

                        match i.on_input_raw(from, old_data, &mut tracer, &replay, nodes, state) {
                            RawProcessingResult::Regular(m) => {
                                mem::replace(data, m.results);
                                misses = m.misses;
                            }
                            RawProcessingResult::CapturedFull => {
                                captured_full = true;
                            }
                            RawProcessingResult::ReplayPiece {
                                rows,
                                keys: emitted_keys,
                                captured: were_captured,
                            } => {
                                // we already know that m must be a ReplayPiece since only a
                                // ReplayPiece can release a ReplayPiece.
                                mem::replace(data, rows);
                                captured = were_captured;
                                if let ReplayContext::Partial { ref mut keys, .. } = replay {
                                    *keys = emitted_keys;
                                } else {
                                    unreachable!();
                                }
                            }
                            RawProcessingResult::FullReplay(rs, last) => {
                                // we already know that m must be a (full) ReplayPiece since only a
                                // (full) ReplayPiece can release a FullReplay
                                mem::replace(data, rs);
                                set_replay_last = Some(last);
                            }
                        }
                    });

                    if let Some(new_last) = set_replay_last {
                        if let Packet::ReplayPiece {
                            context: payload::ReplayPieceContext::Regular { ref mut last },
                            ..
                        } = **m
                        {
                            *last = new_last;
                        } else {
                            unreachable!();
                        }
                    }

                    if let ReplayContext::Partial { keys, .. } = replay {
                        if let Packet::ReplayPiece {
                            context:
                                payload::ReplayPieceContext::Partial {
                                    ref mut for_keys, ..
                                },
                            ..
                        } = **m
                        {
                            *for_keys = keys;
                        } else {
                            unreachable!();
                        }
                    }
                }

                if captured_full {
                    *m = None;
                    return (vec![], HashSet::new());
                }

                let m = m.as_mut().unwrap();
                if let Some(t) = m.tracer() {
                    *t = tracer.take();
                }

                if i.get_base().is_some() && executor.is_some() {
                    // Send write-ACKs to all the clients with updates that made
                    // it into this merged packet:
                    let m = &mut **m;
                    let ex = executor.unwrap();
                    match *m {
                        Packet::Message {
                            ref mut senders, ..
                        } => senders.drain(..).for_each(|src| ex.send_back(src, Ok(0))),
                        Packet::Transaction {
                            ref mut senders,
                            state: TransactionState::Committed(ts, ..),
                            ..
                        } => senders.drain(..).for_each(|src| ex.send_back(src, Ok(ts))),
                        _ => {}
                    }
                }

                // When a replay originates at a base node, we replay the data *through* that same
                // base node because its column set may have changed. However, this replay through
                // the base node itself should *NOT* update the materialization, because otherwise
                // it would duplicate each record in the base table every time a replay happens!
                //
                // So: only materialize if either (1) the message we're processing is not a replay,
                // or (2) if the node we're at is not a base.
                if m.is_regular() || i.get_base().is_none() {
                    let tag = match **m {
                        Packet::ReplayPiece {
                            tag,
                            context: payload::ReplayPieceContext::Partial { .. },
                            ..
                        } => {
                            // NOTE: non-partial replays shouldn't be materialized only for a
                            // particular index, and so the tag shouldn't be forwarded to the
                            // materialization code. this allows us to keep some asserts deeper in
                            // the code to check that we don't do partial replays to non-partial
                            // indices, or for unknown tags.
                            Some(tag)
                        }
                        _ => None,
                    };
                    m.map_data(|rs| {
                        materialize(rs, tag, state.get_mut(&addr));
                    });
                }

                // Send write-ACKs to all the clients with updates that made
                // it into this merged packet:
                match (i.get_base(), executor, m) {
                    (
                        Some(..),
                        Some(ex),
                        &mut box Packet::Message {
                            ref mut senders, ..
                        },
                    ) => senders.drain(..).for_each(|src| ex.send_back(src, Ok(0))),
                    (
                        Some(..),
                        Some(ex),
                        &mut box Packet::Transaction {
                            ref mut senders,
                            state: TransactionState::Committed(ts, ..),
                            ..
                        },
                    ) => senders.drain(..).for_each(|src| ex.send_back(src, Ok(ts))),
                    _ => {}
                };

                (misses, captured)
            }
            NodeType::Source | NodeType::Dropped => unreachable!(),
        }
    }

    pub fn process_eviction(
        &mut self,
        from: LocalNodeIndex,
        key_columns: &[usize],
        keys: &mut Vec<Vec<DataType>>,
        tag: Tag,
        on_shard: Option<usize>,
        output: &mut Vec<(ReplicaAddr, Box<Packet>)>,
    ) {
        let addr = *self.local_addr();
        match self.inner {
            NodeType::Egress(Some(ref mut e)) => {
                e.process(
                    &mut Some(Box::new(Packet::EvictKeys {
                        link: Link {
                            src: addr,
                            dst: addr,
                        },
                        tag,
                        keys: keys.to_vec(),
                    })),
                    on_shard.unwrap_or(0),
                    output,
                );
            }
            NodeType::Sharder(ref mut s) => {
                s.process_eviction(key_columns, tag, keys, addr, on_shard.is_some(), output);
            }
            NodeType::Internal(ref mut i) => {
                i.on_eviction(from, key_columns, keys);
            }
            NodeType::Reader(ref mut r) => {
                r.on_eviction(key_columns, &keys[..]);
            }
            NodeType::Ingress => {}
            NodeType::Egress(None) | NodeType::Source | NodeType::Dropped => unreachable!(),
        }
    }
}

pub fn materialize(rs: &mut Records, partial: Option<Tag>, state: Option<&mut Box<State>>) {
    // our output changed -- do we need to modify materialized state?
    if state.is_none() {
        // nope
        return;
    }

    // yes!
    state.unwrap().process_records(rs, partial);
}
