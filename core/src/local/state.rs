use std::borrow::Cow;
use std::collections::HashMap;
use std::rc::Rc;

use ::*;
use data::SizeOf;
use local::single_state::SingleState;

use bincode;
use rand::{self, Rng};
use rocksdb::{self, DB};

pub enum State {
    InMemory(MemoryState),
    Persistent(PersistentState),
}

impl State {
    pub fn default() -> Self {
        State::InMemory(MemoryState::default())
    }

    pub fn base(name: String, durability_mode: DurabilityMode) -> Self {
        let persistent = PersistentState::initialize(name, durability_mode);
        State::Persistent(persistent)
    }

    /// Add an index keyed by the given columns and replayed to by the given partial tags.
    pub fn add_key(&mut self, columns: &[usize], partial: Option<Vec<Tag>>) {
        match *self {
            State::InMemory(ref mut s) => s.add_key(columns, partial),
            State::Persistent(ref mut s) => s.add_key(columns, partial),
        }
    }

    pub fn is_useful(&self) -> bool {
        match *self {
            State::InMemory(ref s) => s.is_useful(),
            State::Persistent(ref s) => s.indices.len() > 0,
        }
    }

    pub fn is_partial(&self) -> bool {
        match *self {
            State::InMemory(ref s) => s.is_partial(),
            // PersistentStates can't be partial:
            State::Persistent(..) => false,
        }
    }

    pub fn process_records(&mut self, records: &Records) {
        if records.len() == 0 {
            return;
        }

        match *self {
            State::InMemory(ref mut s) => s.process_records(records),
            State::Persistent(ref mut s) => s.process_records(records),
        }
    }

    pub fn insert(&mut self, r: Vec<DataType>, partial_tag: Option<Tag>) -> bool {
        match *self {
            State::InMemory(ref mut s) => s.insert(r, partial_tag),
            State::Persistent(ref mut s) => {
                assert!(partial_tag.is_none(), "Bases can't be partial");
                s.insert(r)
            }
        }
    }

    pub fn remove(&mut self, r: &[DataType]) -> bool {
        match *self {
            State::InMemory(ref mut s) => s.remove(r),
            State::Persistent(ref mut s) => s.remove(r),
        }
    }

    pub fn mark_hole(&mut self, key: &[DataType], tag: &Tag) {
        match *self {
            State::InMemory(ref mut s) => s.mark_hole(key, tag),
            // PersistentStates can't be partial:
            _ => unreachable!(),
        }
    }

    pub fn mark_filled(&mut self, key: Vec<DataType>, tag: &Tag) {
        match *self {
            State::InMemory(ref mut s) => s.mark_filled(key, tag),
            // PersistentStates can't be partial:
            _ => unreachable!(),
        }
    }

    pub fn lookup<'a>(&'a self, columns: &[usize], key: &KeyType) -> LookupResult<'a> {
        match *self {
            State::InMemory(ref s) => s.lookup(columns, key),
            State::Persistent(ref s) => s.lookup(columns, key),
        }
    }

    pub fn rows(&self) -> usize {
        match *self {
            State::InMemory(ref s) => s.rows(),
            State::Persistent(ref s) => s.rows(),
        }
    }

    pub fn keys(&self) -> Vec<Vec<usize>> {
        match *self {
            State::InMemory(ref s) => s.keys(),
            _ => unimplemented!(),
        }
    }

    pub fn cloned_records(&self) -> Vec<Vec<DataType>> {
        match *self {
            State::InMemory(ref s) => s.cloned_records(),
            State::Persistent(ref s) => s.cloned_records(),
        }
    }

    pub fn clear(&mut self) {
        match *self {
            State::InMemory(ref mut s) => s.clear(),
            State::Persistent(ref mut s) => s.clear(),
        }
    }

    pub fn evict_random_keys(&mut self, count: usize) -> (&[usize], Vec<Vec<DataType>>, u64) {
        match *self {
            State::InMemory(ref mut s) => s.evict_random_keys(count),
            _ => unreachable!(),
        }
    }

    /// Evict the listed keys from the materialization targeted by `tag`.
    pub fn evict_keys(&mut self, tag: &Tag, keys: &[Vec<DataType>]) -> (&[usize], u64) {
        match *self {
            State::InMemory(ref mut s) => s.evict_keys(tag, keys),
            _ => unreachable!(),
        }
    }
}

/// PersistentState stores data in SQlite.
pub struct PersistentState {
    // name: String,
    db: DB,
    durability_mode: DurabilityMode,
    indices: Vec<Vec<usize>>,
}

impl PersistentState {
    fn initialize(name: String, durability_mode: DurabilityMode) -> Self {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);

        // Number of threads used by RocksDB:
        // opts.increase_parallelism(4);
        // opts.set_compression_type(rocksdb::DBCompressionType::Snappy);
        // opts.optimize_for_point_lookup(cache_size); ?

        let full_name = format!("{}.db", name);
        let db = DB::open(&opts, &full_name).unwrap();

        Self {
            db,
            durability_mode,
            // name: full_name,
            indices: Default::default(),
        }
    }

    // Puts by primary key first, then retrieves the existing value for each index and appends the
    // newly created primary key value.
    fn insert(&mut self, r: Vec<DataType>) -> bool {
        let primary_index = self.indices[0].iter().map(|i| &r[*i]).collect::<Vec<_>>();
        println!("built pk value {:?} out of {:?}", primary_index, r);
        let primary_key = bincode::serialize(&primary_index).unwrap();
        // Wrap it in a vec to always maintain the same data type for both the primary and other
        // indices: Vec<Vec<DataType>>
        let row = bincode::serialize(&vec![&r]).unwrap();
        // We assume the first index is a primary key, which means we can't have multiple
        // rows for the first index, and that we don't have to retrieve an existing value
        // to append to.
        // TODO(ekmartin): This would force each table to have a primary key, which is
        // probably not what we want.
        self.db.put(&primary_key, &row).unwrap();

        // TODO(ekmartin): maybe the index key should include placeholders for the columns that
        // aren't part of the index, otherwise we could have a case where we have e.g.:
        // Columns: a, b, c, d
        // Index 1: (a, b)
        // Index 2: (c, d)
        // A retrieval for (a, b) would then collide with (c, d) with the current scheme.
        for columns in &self.indices[1..] {
            // Construct a key with the index values, and serialize it with bincode:
            let index = columns.iter().map(|i| &r[*i]).collect::<Vec<_>>();
            println!("built regular index {:?} out of {:?}", index, r);
            let key = bincode::serialize(&index).unwrap();
            // Since an index can point to multiple primary keys, we attempt to retrieve the
            // existing rows this index points to, to add our new primary key to that.
            let existing = self.db.get(&key).unwrap();
            let mut values = if let Some(v) = existing {
                let v: Vec<Vec<DataType>> = bincode::deserialize(&&*v).unwrap();
                v
            } else {
                vec![]
            };

            // To avoid having to clone all the values in primary_index we turn
            // our Vec<Vec<DataType>> into Vec<Vec<&DataType>>, which lets us clone
            // only primary_index itself - not each column.
            let mut rows: Vec<Vec<&DataType>> = values
                .iter()
                .map(|row| row.iter().map(|d| d).collect())
                .collect();

            rows.push(primary_index.clone());
            println!("pointing {:?} -> {:?}", index, rows);
            self.db
                .put(&key, &bincode::serialize(&rows).unwrap())
                .unwrap();
        }

        true
    }

    fn remove(&mut self, r: &[DataType]) -> bool {
        for columns in self.indices.iter() {
            let index = columns.iter().map(|i| &r[*i]).collect::<Vec<_>>();
            let key = bincode::serialize(&index).unwrap();
            self.db.delete(&key).unwrap();
        }

        true
    }

    fn add_key(&mut self, columns: &[usize], partial: Option<Vec<Tag>>) {
        assert!(partial.is_none(), "Bases can't be partial");
        let c = Vec::from(columns);
        if !self.indices.contains(&c) {
            self.indices.push(c);
        }

        // Put all the existing values for the new index.
        // TODO: maybe not even do rows() here, just straight for the iterator.
        if self.rows() > 0 {
            // TODO: this shouldn't use cloned_records, since it'd need to deserialize
            // we should just retrieve the raw bytes and put them directly.
            // for row in self.cloned_records() {
            //     let index = columns.iter().map(|i| &r[i]).collect::<Vec<_>>();
            //     let r = bincode::serialize(&row).unwrap();
            //     let key = bincode::serialize(&index).unwrap();
            //     self.db.put(&key, &r).unwrap();
            // }
        }
    }

    fn process_records(&mut self, records: &Records) {
        for r in records.iter() {
            match *r {
                Record::Positive(ref r) => {
                    self.insert(r.clone());
                }
                Record::Negative(ref r) => {
                    self.remove(r);
                }
                Record::DeleteRequest(..) => unreachable!(),
            }
        }
    }

    fn lookup(&self, columns: &[usize], key: &KeyType) -> LookupResult {
        let values = match *key {
            KeyType::Single(a) => vec![a],
            KeyType::Double(ref r) => vec![&r.0, &r.1],
            KeyType::Tri(ref r) => vec![&r.0, &r.1, &r.2],
            KeyType::Quad(ref r) => vec![&r.0, &r.1, &r.2, &r.3],
            KeyType::Quin(ref r) => vec![&r.0, &r.1, &r.2, &r.3, &r.4],
            KeyType::Sex(ref r) => vec![&r.0, &r.1, &r.2, &r.3, &r.4, &r.5],
        };

        println!("looking up key {:?}", values);
        let k = bincode::serialize(&values).unwrap();
        let pks_or_row: Vec<Vec<DataType>> = match self.db.get(&k).unwrap() {
            Some(data) => bincode::deserialize(&*data).unwrap(),
            None => return LookupResult::Some(Cow::Owned(vec![])),
        };

        let rows: Vec<Vec<DataType>> = if columns == &self.indices[0][..] {
            pks_or_row
        } else {
            // If this wasn't a primary index, pks_or_row now
            // points to a series of primary keys that we need
            // to retrieve the actual values for through additional .get() requests.
            pks_or_row
                .into_iter()
                .flat_map(|pk| {
                    println!("retrieving pk {:?}", pk);
                    let k = bincode::serialize(&pk).unwrap();
                    let data = self.db
                        .get(&k)
                        .unwrap()
                        .expect("index points to non-existant primary key value");
                    let rs: Vec<Vec<DataType>> = bincode::deserialize(&*data).unwrap();
                    rs
                })
                .collect()
        };

        let data = rows.into_iter()
            .map(|row| Row(Rc::new(row)))
            .collect::<Vec<_>>();

        LookupResult::Some(Cow::Owned(data))
    }

    fn cloned_records(&self) -> Vec<Vec<DataType>> {
        // let mut statement = self.db.prepare_cached("SELECT row FROM store").unwrap();

        // let rows = statement
        //     .query_map(&[], Self::map_rows)
        //     .unwrap()
        //     .map(|row| row.unwrap())
        //     .collect::<Vec<_>>();

        vec![]
    }

    fn rows(&self) -> usize {
        0
        // either iterator or to get an estimated number,
        // A: Use GetIntProperty(cf_handle, “rocksdb.estimate-num-keys") to obtain an estimated
        // number of keys stored in a column family, or use
        // GetAggregatedIntProperty(“rocksdb.estimate-num-keys", &num_keys) to obtain an estimated
        // number of keys stored in the whole RocksDB database.
        // unimplemented!()
    }

    fn clear(&self) {
        // maybe just rm the db and create a new?
        unimplemented!()
    }
}

impl Drop for PersistentState {
    fn drop(&mut self) {
        if self.durability_mode != DurabilityMode::Permanent {
            // We'd like to destroy the database files here as well,
            // but to do so we need to drop self.db first. How would we do that?
            // DB::destroy(&rocksdb::Options::default(), &self.name).unwrap()
        }
    }
}

#[derive(Default)]
pub struct MemoryState {
    state: Vec<SingleState>,
    by_tag: HashMap<Tag, usize>,
    mem_size: u64,
}

impl MemoryState {
    /// Returns the index in `self.state` of the index keyed on `cols`, or None if no such index
    /// exists.
    fn state_for(&self, cols: &[usize]) -> Option<usize> {
        self.state.iter().position(|s| s.key() == cols)
    }

    fn add_key(&mut self, columns: &[usize], partial: Option<Vec<Tag>>) {
        let (i, exists) = if let Some(i) = self.state_for(columns) {
            // already keyed by this key; just adding tags
            (i, true)
        } else {
            // will eventually be assigned
            (self.state.len(), false)
        };

        if let Some(ref p) = partial {
            for &tag in p {
                self.by_tag.insert(tag, i);
            }
        }

        if exists {
            return;
        }

        self.state
            .push(SingleState::new(columns, partial.is_some()));

        if !self.state.is_empty() && partial.is_none() {
            // we need to *construct* the index!
            let (new, old) = self.state.split_last_mut().unwrap();

            if !old.is_empty() {
                assert!(!old[0].partial());
                for rs in old[0].values() {
                    for r in rs {
                        new.insert_row(Row(r.0.clone()));
                    }
                }
            }
        }
    }

    /// Returns a Vec of all keys that currently exist on this materialization.
    fn keys(&self) -> Vec<Vec<usize>> {
        self.state.iter().map(|s| s.key().to_vec()).collect()
    }

    /// Returns whether this state is currently keyed on anything. If not, then it cannot store any
    /// infromation and is thus "not useful".
    fn is_useful(&self) -> bool {
        !self.state.is_empty()
    }

    fn is_partial(&self) -> bool {
        self.state.iter().any(|s| s.partial())
    }

    fn process_records(&mut self, records: &Records) {
        // Might consider using a write batch if it helps here (might not help when we're not
        // waiting for fsyncs).
        for r in records.iter() {
            match *r {
                Record::Positive(ref r) => {
                    let hit = self.insert(r.clone(), None);
                    debug_assert!(hit);
                }
                Record::Negative(ref r) => {
                    let hit = self.remove(r);
                    debug_assert!(hit);
                }
                Record::DeleteRequest(..) => unreachable!(),
            }
        }
    }

    fn insert(&mut self, r: Vec<DataType>, partial_tag: Option<Tag>) -> bool {
        let r = Rc::new(r);

        if let Some(tag) = partial_tag {
            let i = match self.by_tag.get(&tag) {
                Some(i) => *i,
                None => {
                    // got tagged insert for unknown tag. this will happen if a node on an old
                    // replay path is now materialized. must return true to avoid any records
                    // (which are destined for a downstream materialization) from being pruned.
                    return true;
                }
            };
            self.mem_size += r.deep_size_of();
            self.state[i].insert_row(Row(r))
        } else {
            let mut hit_any = false;
            for i in 0..self.state.len() {
                hit_any |= self.state[i].insert_row(Row(r.clone()));
            }
            if hit_any {
                self.mem_size += r.deep_size_of();
            }
            hit_any
        }
    }

    fn remove(&mut self, r: &[DataType]) -> bool {
        let mut hit = false;
        for s in &mut self.state {
            if let Some(row) = s.remove_row(r, &mut hit) {
                if Rc::strong_count(&row.0) == 1 {
                    self.mem_size = self.mem_size.checked_sub(row.deep_size_of()).unwrap();
                }
            }
        }

        hit
    }

    fn rows(&self) -> usize {
        self.state.iter().map(|s| s.rows()).sum()
    }

    fn mark_filled(&mut self, key: Vec<DataType>, tag: &Tag) {
        debug_assert!(!self.state.is_empty(), "filling uninitialized index");
        let index = self.by_tag[tag];
        self.state[index].mark_filled(key);
    }

    fn mark_hole(&mut self, key: &[DataType], tag: &Tag) {
        debug_assert!(!self.state.is_empty(), "filling uninitialized index");
        let index = self.by_tag[tag];
        self.state[index].mark_hole(key);
    }

    fn lookup<'a>(&'a self, columns: &[usize], key: &KeyType) -> LookupResult<'a> {
        debug_assert!(!self.state.is_empty(), "lookup on uninitialized index");
        let index = self.state_for(columns)
            .expect("lookup on non-indexed column set");
        self.state[index].lookup(key)
    }

    /// Return a copy of all records. Panics if the state is only partially materialized.
    fn cloned_records(&self) -> Vec<Vec<DataType>> {
        fn fix<'a>(rs: &'a Vec<Row>) -> impl Iterator<Item = Vec<DataType>> + 'a {
            rs.iter().map(|r| Vec::clone(&**r))
        }

        assert!(!self.state[0].partial());
        self.state[0].values().flat_map(fix).collect()
    }

    fn clear(&mut self) {
        for s in &mut self.state {
            s.clear();
        }
        self.mem_size = 0;
    }

    /// Evict `count` randomly selected keys, returning key colunms of the index chosen to evict
    /// from along with the keys evicted and the number of bytes evicted.
    fn evict_random_keys(&mut self, count: usize) -> (&[usize], Vec<Vec<DataType>>, u64) {
        let mut rng = rand::thread_rng();
        let index = rng.gen_range(0, self.state.len());
        let (bytes_freed, keys) = self.state[index].evict_random_keys(count, &mut rng);
        self.mem_size = self.mem_size.saturating_sub(bytes_freed);
        (self.state[index].key(), keys, bytes_freed)
    }

    /// Evict the listed keys from the materialization targeted by `tag`, returning the key columns
    /// of the index that was evicted from and the number of bytes evicted.
    fn evict_keys(&mut self, tag: &Tag, keys: &[Vec<DataType>]) -> (&[usize], u64) {
        let index = self.by_tag[tag];
        let bytes = self.state[index].evict_keys(keys);
        self.mem_size = self.mem_size.saturating_sub(bytes);
        (self.state[index].key(), bytes)
    }
}

impl SizeOf for State {
    fn size_of(&self) -> u64 {
        use std::mem::size_of;

        size_of::<Self>() as u64
    }

    fn deep_size_of(&self) -> u64 {
        match *self {
            State::InMemory(ref s) => s.mem_size,
            State::Persistent(..) => self.size_of(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::time::{SystemTime, UNIX_EPOCH};

    // Ensures correct handling of state file names, by deleting used state files in Drop.
    struct StateName {
        name: String,
    }

    impl StateName {
        fn new(prefix: &str) -> Self {
            let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            let name = format!(
                "{}.{}.{}",
                prefix,
                current_time.as_secs(),
                current_time.subsec_nanos()
            );

            Self { name }
        }
    }

    // Removes the log files matching the glob ./{log_name}-*.json.
    // Used to clean up after recovery tests, where a persistent log is created.
    impl Drop for StateName {
        fn drop(&mut self) {
            // TODO: might have to use the same options here?
            DB::destroy(&rocksdb::Options::default(), format!("{}.db", self.name)).unwrap()
        }
    }

    fn setup_persistent(name: String) -> State {
        State::base(name, DurabilityMode::MemoryOnly)
    }

    #[test]
    fn persistent_state_is_partial() {
        let n = StateName::new("persistent_state_is_partial");
        let state = setup_persistent(n.name.clone());
        assert!(!state.is_partial());
    }

    #[test]
    fn persistent_state_single_key() {
        let n = StateName::new("persistent_state_single_key");
        let mut state = setup_persistent(n.name.clone());
        let columns = &[0];
        let row: Vec<DataType> = vec![10.into(), "Cat".into()];
        state.add_key(columns, None);
        state.insert(row, None);

        match state.lookup(columns, &KeyType::Single(&5.into())) {
            LookupResult::Some(rows) => assert_eq!(rows.len(), 0),
            LookupResult::Missing => panic!("PersistentStates can't be materialized"),
        };

        match state.lookup(columns, &KeyType::Single(&10.into())) {
            LookupResult::Some(rows) => {
                let data = &*rows[0];
                assert_eq!(data[0], 10.into());
                assert_eq!(data[1], "Cat".into());
            }
            _ => panic!(),
        }
    }

    #[test]
    fn persistent_state_multi_key() {
        let n = StateName::new("persistent_state_multi_key");
        let mut state = setup_persistent(n.name.clone());
        let columns = &[0, 2];
        let row: Vec<DataType> = vec![10.into(), "Cat".into(), 20.into()];
        state.add_key(columns, None);
        assert!(state.insert(row.clone(), None));

        match state.lookup(columns, &KeyType::Double((1.into(), 2.into()))) {
            LookupResult::Some(rows) => assert_eq!(rows.len(), 0),
            LookupResult::Missing => panic!("PersistentStates can't be materialized"),
        };

        match state.lookup(columns, &KeyType::Double((10.into(), 20.into()))) {
            LookupResult::Some(rows) => {
                assert_eq!(&*rows[0], &row);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn persistent_state_multiple_indices() {
        let n = StateName::new("persistent_state_multiple_indices");
        let mut state = setup_persistent(n.name.clone());
        let first: Vec<DataType> = vec![10.into(), "Cat".into(), 1.into()];
        let second: Vec<DataType> = vec![20.into(), "Cat".into(), 1.into()];
        state.add_key(&[0], None);
        state.add_key(&[1, 2], None);
        assert!(state.insert(first.clone(), None));
        assert!(state.insert(second.clone(), None));

        match state.lookup(&[0], &KeyType::Single(&10.into())) {
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&*rows[0], &first);
            }
            _ => panic!(),
        }

        match state.lookup(&[1, 2], &KeyType::Double(("Cat".into(), 1.into()))) {
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 2);
                assert_eq!(&*rows[0], &first);
                assert_eq!(&*rows[1], &second);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn persistent_state_different_indices() {
        let n = StateName::new("persistent_state_different_indices");
        let mut state = setup_persistent(n.name.clone());
        let first: Vec<DataType> = vec![10.into(), "Cat".into()];
        let second: Vec<DataType> = vec![20.into(), "Bob".into()];
        state.add_key(&[0], None);
        state.add_key(&[1], None);
        assert!(state.insert(first.clone(), None));
        assert!(state.insert(second.clone(), None));

        match state.lookup(&[0], &KeyType::Single(&10.into())) {
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&*rows[0], &first);
            }
            _ => panic!(),
        }

        match state.lookup(&[1], &KeyType::Single(&"Bob".into())) {
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(&*rows[0], &second);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn persistent_state_remove() {
        let n = StateName::new("persistent_state_remove");
        let mut state = setup_persistent(n.name.clone());
        let columns = &[0];
        let first: Vec<DataType> = vec![10.into(), "Cat".into()];
        let second: Vec<DataType> = vec![20.into(), "Bob".into()];
        state.add_key(columns, None);
        assert!(state.insert(first.clone(), None));
        assert!(state.insert(second.clone(), None));
        assert!(state.remove(&first));

        match state.lookup(columns, &KeyType::Single(&first[0])) {
            LookupResult::Some(rows) => assert_eq!(rows.len(), 0),
            LookupResult::Missing => panic!("PersistentStates can't be materialized"),
        };

        match state.lookup(columns, &KeyType::Single(&second[0])) {
            LookupResult::Some(rows) => {
                assert_eq!(&*rows[0], &second);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn persistent_state_is_useful() {
        let n = StateName::new("persistent_state_is_useful");
        let mut state = setup_persistent(n.name.clone());
        let columns = &[0];
        assert!(!state.is_useful());
        state.add_key(columns, None);
        assert!(state.is_useful());
    }

    #[test]
    fn persistent_state_rows() {
        let n = StateName::new("persistent_state_rows");
        let mut state = setup_persistent(n.name.clone());
        let columns = &[0];
        let first: Vec<DataType> = vec![10.into(), "Cat".into()];
        let second: Vec<DataType> = vec![20.into(), "Bob".into()];
        state.add_key(columns, None);
        assert_eq!(state.rows(), 0);
        assert!(state.insert(first.clone(), None));
        assert_eq!(state.rows(), 1);
        assert!(state.insert(second.clone(), None));
        assert_eq!(state.rows(), 2);
    }

    #[test]
    fn persistent_state_cloned_records() {
        let n = StateName::new("persistent_state_cloned_records");
        let mut state = setup_persistent(n.name.clone());
        let columns = &[0];
        let first: Vec<DataType> = vec![10.into(), "Cat".into()];
        let second: Vec<DataType> = vec![20.into(), "Bob".into()];
        state.add_key(columns, None);
        assert!(state.insert(first.clone(), None));
        assert!(state.insert(second.clone(), None));
        assert_eq!(state.cloned_records(), vec![first, second]);
    }

    #[test]
    fn persistent_state_clear() {
        let n = StateName::new("persistent_state_clear");
        let mut state = setup_persistent(n.name.clone());
        let columns = &[0];
        let first: Vec<DataType> = vec![10.into(), "Cat".into()];
        let second: Vec<DataType> = vec![20.into(), "Bob".into()];
        state.add_key(columns, None);
        assert!(state.insert(first.clone(), None));
        assert!(state.insert(second.clone(), None));

        state.clear();
        assert_eq!(state.rows(), 0);
    }

    #[test]
    fn persistent_state_drop() {
        let name = ".s-o_u#p.";
        let db_name = format!("{}.db", name);
        let path = Path::new(&db_name);
        {
            let _state = State::base(String::from(name), DurabilityMode::DeleteOnExit);
            assert!(path.exists());
        }

        assert!(!path.exists());
    }

    #[test]
    fn persistent_state_old_records_new_index() {
        let n = StateName::new("persistent_state_old_records_new_index");
        let mut state = setup_persistent(n.name.clone());
        let row: Vec<DataType> = vec![10.into(), "Cat".into()];
        state.add_key(&[0], None);
        assert!(state.insert(row.clone(), None));
        state.add_key(&[1], None);

        match state.lookup(&[1], &KeyType::Single(&row[1])) {
            LookupResult::Some(rows) => assert_eq!(&*rows[0], &row),
            _ => unreachable!(),
        };
    }

    fn process_records(mut state: State) {
        let records: Records = vec![
            (vec![1.into(), "A".into()], true),
            (vec![2.into(), "B".into()], true),
            (vec![3.into(), "C".into()], true),
            (vec![1.into(), "A".into()], false),
        ].into();

        state.add_key(&[0], None);
        state.process_records(&records);

        // Make sure the first record has been deleted:
        match state.lookup(&[0], &KeyType::Single(&records[0][0])) {
            LookupResult::Some(rows) => assert_eq!(rows.len(), 0),
            _ => unreachable!(),
        };

        // Then check that the rest exist:
        for i in 1..3 {
            let record = &records[i];
            match state.lookup(&[0], &KeyType::Single(&record[0])) {
                LookupResult::Some(rows) => assert_eq!(&*rows[0], &**record),
                _ => unreachable!(),
            };
        }
    }

    #[test]
    fn persistent_state_process_records() {
        let n = StateName::new("persistent_state_process_records");
        let state = setup_persistent(n.name.clone());
        process_records(state);
    }

    #[test]
    fn memory_state_process_records() {
        let state = State::default();
        process_records(state);
    }

    #[test]
    fn memory_state_old_records_new_index() {
        let mut state = State::default();
        let row: Vec<DataType> = vec![10.into(), "Cat".into()];
        state.add_key(&[0], None);
        assert!(state.insert(row.clone(), None));
        state.add_key(&[1], None);

        match state.lookup(&[1], &KeyType::Single(&row[1])) {
            LookupResult::Some(rows) => assert_eq!(&*rows[0], &row),
            _ => unreachable!(),
        };
    }
}
