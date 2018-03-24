use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use ::*;
use data::SizeOf;
use local::single_state::SingleState;
use serde_json;

use rand::{self, Rng};
use rusqlite::{self, Connection};
use rusqlite::types::{ToSql, ToSqlOutput};

pub enum State {
    InMemory(MemoryState),
    Persistent(PersistentState),
}

impl State {
    pub fn default() -> Self {
        State::InMemory(MemoryState::default())
    }

    pub fn base() -> Self {
        State::Persistent(PersistentState::initialize())
    }

    /// Add an index keyed by the given columns and replayed to by the given partial tags.
    pub fn add_key(&mut self, columns: &[usize], partial: Option<Vec<Tag>>) {
        match *self {
            State::InMemory(ref mut s) => s.add_key(columns, partial),
            State::Persistent(ref mut s) => s.add_key(columns, partial),
        }
    }

    pub fn keys(&self) -> Vec<Vec<usize>> {
        match *self {
            State::InMemory(ref s) => s.keys(),
            _ => unreachable!(),
        }
    }

    pub fn is_useful(&self) -> bool {
        match *self {
            State::InMemory(ref s) => s.is_useful(),
            _ => unreachable!(),
        }
    }

    pub fn is_partial(&self) -> bool {
        match *self {
            State::InMemory(ref s) => s.is_partial(),
            State::Persistent(..) => false,
        }
    }

    pub fn insert(&mut self, r: Vec<DataType>, partial_tag: Option<Tag>) -> bool {
        match *self {
            State::InMemory(ref mut s) => s.insert(r, partial_tag),
            State::Persistent(ref mut s) => s.insert(r, partial_tag),
        }
    }

    pub fn remove(&mut self, r: &[DataType]) -> bool {
        match *self {
            State::InMemory(ref mut s) => s.remove(r),
            _ => unreachable!(),
        }
    }

    pub fn mark_hole(&mut self, key: &[DataType], tag: &Tag) {
        match *self {
            State::InMemory(ref mut memory_state) => memory_state.mark_hole(key, tag),
            // PersistentStates can't be partial:
            _ => unreachable!(),
        }
    }

    pub fn mark_filled(&mut self, key: Vec<DataType>, tag: &Tag) {
        match *self {
            State::InMemory(ref mut memory_state) => memory_state.mark_filled(key, tag),
            // PersistentStates can't be partial:
            _ => unreachable!(),
        }
    }

    pub fn lookup<'a>(&'a self, columns: &[usize], key: &KeyType) -> LookupResult<'a> {
        match *self {
            State::InMemory(ref memory_state) => memory_state.lookup(columns, key),
            State::Persistent(ref s) => s.lookup(columns, key),
        }
    }

    pub fn rows(&self) -> usize {
        match *self {
            State::InMemory(ref memory_state) => memory_state.rows(),
            _ => unreachable!(),
        }
    }

    pub fn cloned_records(&self) -> Vec<Vec<DataType>> {
        match *self {
            State::InMemory(ref memory_state) => memory_state.cloned_records(),
            _ => unreachable!(),
        }
    }

    pub fn clear(&mut self) {
        match *self {
            State::InMemory(ref mut memory_state) => memory_state.clear(),
            _ => unreachable!(),
        }
    }

    pub fn evict_random_keys(&mut self, count: usize) -> (&[usize], Vec<Vec<DataType>>) {
        match *self {
            State::InMemory(ref mut memory_state) => memory_state.evict_random_keys(count),
            _ => unreachable!(),
        }
    }

    /// Evict the listed keys from the materialization targeted by `tag`.
    pub fn evict_keys(&mut self, tag: &Tag, keys: &[Vec<DataType>]) {
        match *self {
            State::InMemory(ref mut memory_state) => memory_state.evict_keys(tag, keys),
            _ => unreachable!(),
        }
    }
}

pub struct PersistentState {
    connection: Connection,
    statements: HashMap<Vec<usize>, String>,
    indices: HashSet<usize>,
}

impl ToSql for DataType {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput> {
        Ok(match *self {
            DataType::None => unreachable!(),
            DataType::Int(n) => ToSqlOutput::from(n),
            DataType::BigInt(n) => ToSqlOutput::from(n),
            DataType::Real(i, f) => {
                let value = (i as f64) + (f as f64) * 1.0e-9;
                ToSqlOutput::from(value)
            }
            DataType::Text(..) | DataType::TinyText(..) => ToSqlOutput::from(self.to_string()),
            DataType::Timestamp(ts) => ToSqlOutput::from(ts.format("%+").to_string()),
        })
    }
}

impl PersistentState {
    fn initialize() -> Self {
        let connection = Connection::open("sqlite_db").unwrap();
        connection
            .execute("CREATE TABLE store (row BLOB)", &[])
            .unwrap();

        Self {
            connection,
            statements: Default::default(),
            indices: Default::default(),
        }
    }

    fn add_key(&mut self, columns: &[usize], partial: Option<Vec<Tag>>) {
        assert!(partial.is_none(), "Bases can't be partial");
        for index in columns.iter() {
            if self.indices.contains(index) {
                continue;
            }

            self.indices.insert(*index);
            self.connection
                .execute(
                    &format!("ALTER TABLE store ADD COLUMN index_{} TEXT", index),
                    &[],
                )
                .unwrap();
        }

        let clauses = columns
            .iter()
            .enumerate()
            .map(|(i, column)| format!("index_{} = ?{}", column, i + 1))
            .collect::<Vec<_>>()
            .join(", ");

        println!("clauses {:?}", clauses);
        let statement = format!("SELECT row FROM store WHERE {}", clauses);
        self.connection.prepare_cached(&statement).unwrap();
        self.statements.insert(Vec::from(columns), statement);
    }

    fn insert(&mut self, r: Vec<DataType>, partial_tag: Option<Tag>) -> bool {
        assert!(partial_tag.is_none(), "Bases can't be partial");
        let columns = format!(
            "row, {}",
            self.indices
                .iter()
                .map(|index| format!("index_{}", index))
                .collect::<Vec<String>>()
                .join(", ")
        );

        let placeholders = (1..(self.indices.len() + 2))
            .map(|placeholder| format!("?{}", placeholder))
            .collect::<Vec<String>>()
            .join(", ");

        println!("INSERT INTO STORE ({}) VALUES ({})", columns, placeholders);
        let mut statement = self.connection
            .prepare_cached(&format!(
                "INSERT INTO store ({}) VALUES ({})",
                columns, placeholders
            ))
            .unwrap();

        let row = serde_json::to_string(&r).unwrap();
        // match self.indices.len() {
        //      1 => statement.execute(&[&row, ]
        //      _ => unreachable!()
        //      // KeyType::Double((a, b)) => statement.execute(&[&a, &b]),
        //      // KeyType::Tri((a, b, c)) => statement.execute(&[&a, &b, &c]),
        //      // KeyType::Quad((a, b, c, d)) => statement.execute(&[&a, &b, &c, &d]),
        //      // KeyType::Quin((a, b, c, d, e)) => statement.execute(&[&a, &b, &c, &d, &e]),
        //      // KeyType::Sex((a, b, c, d, e, f)) => statement.execute(&[&a, &b, &c, &d, &e, &f]),
        // }.unwrap();

        let mut values: Vec<&ToSql> = vec![&row];
        let mut index_values = self.indices
            .iter()
            .map(|index| {
                println!("index value {}", r[*index]);
                &r[*index] as &ToSql
            })
            .collect::<Vec<&ToSql>>();

        values.append(&mut index_values);
        statement.execute(&values[..]).unwrap();
        true
    }

    fn lookup(&self, columns: &[usize], key: &KeyType) -> LookupResult {
        let mut statement = self.connection
            .prepare_cached(&self.statements[&Vec::from(columns)])
            .unwrap();

        println!("looking up {}", self.statements[&Vec::from(columns)]);
        let rows = match *key {
             KeyType::Single(a) => statement.query_map(&[a], |row| {
                 let string: String = row.get(0);
                 println!("got row {:?}", string);
                 let data_row: Vec<DataType> = serde_json::from_str(&string).unwrap();
                 data_row
             }),
             _ => unreachable!()
             // KeyType::Double((a, b)) => statement.execute(&[&a, &b]),
             // KeyType::Tri((a, b, c)) => statement.execute(&[&a, &b, &c]),
             // KeyType::Quad((a, b, c, d)) => statement.execute(&[&a, &b, &c, &d]),
             // KeyType::Quin((a, b, c, d, e)) => statement.execute(&[&a, &b, &c, &d, &e]),
             // KeyType::Sex((a, b, c, d, e, f)) => statement.execute(&[&a, &b, &c, &d, &e, &f]),
        };

        let data = rows.unwrap()
            .map(|row| Row(Rc::new(row.unwrap())))
            .collect::<Vec<_>>();

        LookupResult::Some(Cow::Owned(data))
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
            // FIXME: self.rows += ?
            self.mem_size += r.deep_size_of();
            self.state[i].insert_row(Row(r))
        } else {
            let mut hit_any = true;
            self.mem_size += r.deep_size_of();
            for i in 0..self.state.len() {
                hit_any = self.state[i].insert_row(Row(r.clone())) || hit_any;
            }
            hit_any
        }
    }

    fn remove(&mut self, r: &[DataType]) -> bool {
        let mut hit = false;
        let mut removed = false;

        for s in &mut self.state {
            s.remove_row(r, &mut hit, &mut removed);
        }

        if removed {
            self.mem_size = self.mem_size
                .saturating_sub((*r).iter().fold(0u64, |acc, d| acc + d.deep_size_of()));
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
    }

    fn unalias_for_state(&mut self) {
        let left = self.state.drain(..).last();
        if let Some(left) = left {
            self.state.push(left);
        }
    }

    /// Evict `count` randomly selected keys, returning key colunms of the index chosen to evict
    /// from along with the keys evicted.
    fn evict_random_keys(&mut self, count: usize) -> (&[usize], Vec<Vec<DataType>>) {
        let mut rng = rand::thread_rng();
        let index = rng.gen_range(0, self.state.len());
        let (bytes_freed, keys) = self.state[index].evict_random_keys(count, &mut rng);
        self.mem_size = self.mem_size.saturating_sub(bytes_freed);
        (self.state[index].key(), keys)
    }

    /// Evict the listed keys from the materialization targeted by `tag`.
    fn evict_keys(&mut self, tag: &Tag, keys: &[Vec<DataType>]) {
        self.mem_size = self.mem_size
            .saturating_sub(self.state[self.by_tag[tag]].evict_keys(keys));
    }
}

impl<'a> Drop for MemoryState {
    fn drop(&mut self) {
        self.unalias_for_state();
        self.clear();
    }
}

impl SizeOf for State {
    fn size_of(&self) -> u64 {
        use std::mem::size_of;

        size_of::<Self>() as u64
    }

    fn deep_size_of(&self) -> u64 {
        match *self {
            State::InMemory(ref memory_state) => memory_state.mem_size,
            _ => unreachable!(),
        }
    }
}
