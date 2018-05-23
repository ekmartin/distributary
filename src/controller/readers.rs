use channel::rpc::{RpcSendError, RpcServiceEndpoint};
use dataflow::backlog::SingleReadHandle;
use dataflow::prelude::*;
use dataflow::Readers;
use std::cell::RefCell;
use std::collections::HashMap;

use api::{ReadQuery, ReadReply};

pub(crate) type Rpc = RpcServiceEndpoint<ReadQuery, ReadReply>;

thread_local! {
    static READERS: RefCell<HashMap<
        (NodeIndex, usize),
        SingleReadHandle,
    >> = Default::default();
}

pub(crate) fn handle_message(m: ReadQuery, conn: &mut Rpc, s: &mut Readers) {
    let mut res = conn.send(&match m {
        ReadQuery::Normal {
            target,
            mut keys,
            block,
        } => ReadReply::Normal(READERS.with(move |readers_cache| {
            let mut readers_cache = readers_cache.borrow_mut();
            let reader = readers_cache.entry(target.clone()).or_insert_with(|| {
                let readers = s.lock().unwrap();
                readers.get(&target).unwrap().clone()
            });

            let mut ret = Vec::with_capacity(keys.len());
            ret.resize(keys.len(), Vec::new());

            let dup = |rs: &[Vec<DataType>]| {
                rs.into_iter()
                    .map(|r| r.iter().map(|v| v.deep_clone()).collect())
                    .collect()
            };
            let dup = &dup;

            // first do non-blocking reads for all keys to trigger any replays
            let found = keys
                .iter_mut()
                .map(|key| {
                    let rs = reader.find_and(key, dup, false).map(|r| r.0);
                    (key, rs)
                })
                .enumerate();

            let mut ready = true;
            for (i, (key, v)) in found {
                match v {
                    Ok(Some(rs)) => {
                        // immediate hit!
                        ret[i] = rs;
                        *key = vec![];
                    }
                    Err(()) => {
                        // map not yet ready
                        ready = false;
                        *key = vec![];
                        break;
                    }
                    Ok(None) => {
                        // triggered partial replay
                    }
                }
            }

            if !ready {
                return Err(());
            }

            if !block {
                return Ok(ret);
            }

            // block on all remaining keys
            for (i, key) in keys.iter().enumerate() {
                if key.is_empty() {
                    // already have this value
                } else {
                    // note that this *does* mean we'll trigger replay twice for things that
                    // miss and aren't replayed in time, which is a little sad. but at the same
                    // time, that replay trigger will just be ignored by the target domain.
                    ret[i] = reader
                        .find_and(key, dup, true)
                        .map(|r| r.0.unwrap_or_default())
                        .expect("evmap *was* ready, then *stopped* being ready")
                }
            }

            Ok(ret)
        })),
        ReadQuery::Size { target } => {
            let size = READERS.with(|readers_cache| {
                let mut readers_cache = readers_cache.borrow_mut();
                let reader = readers_cache.entry(target.clone()).or_insert_with(|| {
                    let readers = s.lock().unwrap();
                    readers.get(&target).unwrap().clone()
                });

                reader.len()
            });

            ReadReply::Size(size)
        }
    });

    while let Err(RpcSendError::StillNeedsFlush) = res {
        res = conn.flush();
    }
    if let Err(RpcSendError::Disconnected) = res {
        // something must have gone wrong on the other end...
        // the client sent a request, and then didn't wait for the reply
        return;
    }
    res.unwrap();
}