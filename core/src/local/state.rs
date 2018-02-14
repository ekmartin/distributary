use ::*;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use local::keyed_state::KeyedState;
use local::single_state::SingleState;
use serde_json;

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

    pub fn iter(&self) -> rahashmap::Values<DataType, Vec<Row>> {
        match *self {
            State::InMemory(ref s) => s.iter(),
            _ => unreachable!(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match *self {
            State::InMemory(ref s) => s.is_empty(),
            _ => unreachable!(),
        }
    }

    pub fn len(&self) -> usize {
        match *self {
            State::InMemory(ref s) => s.len(),
            _ => unreachable!(),
        }
    }

    pub fn nkeys(&self) -> usize {
        match *self {
            State::InMemory(ref s) => s.nkeys(),
            _ => unreachable!(),
        }
    }

    pub fn mark_hole(&mut self, key: &[DataType], tag: &Tag) {
        match *self {
            State::InMemory(ref mut s) => s.mark_hole(key, tag),
            _ => unreachable!(),
        }
    }

    pub fn mark_filled(&mut self, key: Vec<DataType>, tag: &Tag) {
        match *self {
            State::InMemory(ref mut s) => s.mark_filled(key, tag),
            _ => unreachable!(),
        }
    }

    pub fn lookup<'a>(&'a self, columns: &[usize], key: &KeyType) -> LookupResult<'a> {
        match *self {
            State::InMemory(ref s) => s.lookup(columns, key),
            State::Persistent(ref s) => s.lookup(columns, key),
        }
    }

    pub fn cloned_records(&self) -> Vec<Vec<DataType>> {
        match *self {
            State::InMemory(ref s) => s.cloned_records(),
            _ => unreachable!(),
        }
    }

    pub fn clear(&mut self) {
        match *self {
            State::InMemory(ref mut s) => s.clear(),
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
        let connection = Connection::open_in_memory().unwrap();
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
    rows: usize,
}

impl MemoryState {
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

        self.state.push(SingleState {
            key: Vec::from(columns),
            state: columns.into(),
            partial: partial.is_some(),
        });

        if !self.is_empty() && partial.is_none() {
            // we need to *construct* the index!
            let (new, old) = self.state.split_last_mut().unwrap();
            let mut insert = move |rs: &Vec<Row>| {
                for r in rs {
                    new.insert(Row(r.0.clone()));
                }
            };
            match old[0].state {
                KeyedState::Single(ref map) => for rs in map.values() {
                    insert(rs);
                },
                KeyedState::Double(ref map) => for rs in map.values() {
                    insert(rs);
                },
                KeyedState::Tri(ref map) => for rs in map.values() {
                    insert(rs);
                },
                KeyedState::Quad(ref map) => for rs in map.values() {
                    insert(rs);
                },
                KeyedState::Quin(ref map) => for rs in map.values() {
                    insert(rs);
                },
                KeyedState::Sex(ref map) => for rs in map.values() {
                    insert(rs);
                },
            }
        }
    }

    /// Returns a Vec of all keys that currently exist on this materialization.
    fn keys(&self) -> Vec<Vec<usize>> {
        self.state.iter().map(|s| &s.key).cloned().collect()
    }

    /// Returns whether this state is currently keyed on anything. If not, then it cannot store any
    /// infromation and is thus "not useful".
    fn is_useful(&self) -> bool {
        !self.state.is_empty()
    }

    fn is_partial(&self) -> bool {
        self.state.iter().any(|s| s.partial)
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
            self.state[i].insert(Row(r))
        } else {
            let mut hit_any = true;
            self.rows = self.rows.saturating_add(1);
            for i in 0..self.state.len() {
                hit_any = self.state[i].insert(Row(r.clone())) || hit_any;
            }
            hit_any
        }
    }

    fn remove(&mut self, r: &[DataType]) -> bool {
        let mut hit = false;
        let mut removed = false;
        let fix = |removed: &mut bool, rs: &mut Vec<Row>| {
            // rustfmt
            if let Some(i) = rs.iter().position(|rsr| &rsr[..] == r) {
                rs.swap_remove(i);
                *removed = true;
            }
        };

        for s in &mut self.state {
            match s.state {
                KeyedState::Single(ref mut map) => {
                    if let Some(ref mut rs) = map.get_mut(&r[s.key[0]]) {
                        fix(&mut removed, rs);
                        hit = true;
                    }
                }
                KeyedState::Double(ref mut map) => {
                    // TODO: can we avoid the Clone here?
                    let key = (r[s.key[0]].clone(), r[s.key[1]].clone());
                    if let Some(ref mut rs) = map.get_mut(&key) {
                        fix(&mut removed, rs);
                        hit = true;
                    }
                }
                KeyedState::Tri(ref mut map) => {
                    let key = (
                        r[s.key[0]].clone(),
                        r[s.key[1]].clone(),
                        r[s.key[2]].clone(),
                    );
                    if let Some(ref mut rs) = map.get_mut(&key) {
                        fix(&mut removed, rs);
                        hit = true;
                    }
                }
                KeyedState::Quad(ref mut map) => {
                    let key = (
                        r[s.key[0]].clone(),
                        r[s.key[1]].clone(),
                        r[s.key[2]].clone(),
                        r[s.key[3]].clone(),
                    );
                    if let Some(ref mut rs) = map.get_mut(&key) {
                        fix(&mut removed, rs);
                        hit = true;
                    }
                }
                KeyedState::Quin(ref mut map) => {
                    let key = (
                        r[s.key[0]].clone(),
                        r[s.key[1]].clone(),
                        r[s.key[2]].clone(),
                        r[s.key[3]].clone(),
                        r[s.key[4]].clone(),
                    );
                    if let Some(ref mut rs) = map.get_mut(&key) {
                        fix(&mut removed, rs);
                        hit = true;
                    }
                }
                KeyedState::Sex(ref mut map) => {
                    let key = (
                        r[s.key[0]].clone(),
                        r[s.key[1]].clone(),
                        r[s.key[2]].clone(),
                        r[s.key[3]].clone(),
                        r[s.key[4]].clone(),
                        r[s.key[5]].clone(),
                    );
                    if let Some(ref mut rs) = map.get_mut(&key) {
                        fix(&mut removed, rs);
                        hit = true;
                    }
                }
            }
        }

        if removed {
            self.rows = self.rows.saturating_sub(1);
        }

        hit
    }

    fn iter(&self) -> rahashmap::Values<DataType, Vec<Row>> {
        for index in &self.state {
            if let KeyedState::Single(ref map) = index.state {
                if index.partial {
                    unimplemented!();
                }
                return map.values();
            }
        }
        // TODO: allow iter without single key (breaks return type)
        unimplemented!();
    }

    fn is_empty(&self) -> bool {
        self.state.is_empty() || self.state[0].state.is_empty()
    }

    fn len(&self) -> usize {
        self.rows
    }

    fn nkeys(&self) -> usize {
        if self.state.is_empty() {
            0
        } else {
            self.state[0].state.len()
        }
    }

    fn mark_filled(&mut self, key: Vec<DataType>, tag: &Tag) {
        debug_assert!(!self.state.is_empty(), "filling uninitialized index");
        let i = self.by_tag[tag];
        let index = &mut self.state[i];
        let mut key = key.into_iter();
        let replaced = match index.state {
            KeyedState::Single(ref mut map) => map.insert(key.next().unwrap(), Vec::new()),
            KeyedState::Double(ref mut map) => {
                map.insert((key.next().unwrap(), key.next().unwrap()), Vec::new())
            }
            KeyedState::Tri(ref mut map) => map.insert(
                (
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                ),
                Vec::new(),
            ),
            KeyedState::Quad(ref mut map) => map.insert(
                (
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                ),
                Vec::new(),
            ),
            KeyedState::Quin(ref mut map) => map.insert(
                (
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                ),
                Vec::new(),
            ),
            KeyedState::Sex(ref mut map) => map.insert(
                (
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                ),
                Vec::new(),
            ),
        };
        assert!(replaced.is_none());
    }

    fn mark_hole(&mut self, key: &[DataType], tag: &Tag) {
        debug_assert!(!self.state.is_empty(), "filling uninitialized index");
        let i = self.by_tag[tag];
        let index = &mut self.state[i];
        let removed = match index.state {
            KeyedState::Single(ref mut map) => map.remove(&key[0]),
            KeyedState::Double(ref mut map) => map.remove(&(key[0].clone(), key[1].clone())),
            KeyedState::Tri(ref mut map) => {
                map.remove(&(key[0].clone(), key[1].clone(), key[2].clone()))
            }
            KeyedState::Quad(ref mut map) => map.remove(&(
                key[0].clone(),
                key[1].clone(),
                key[2].clone(),
                key[3].clone(),
            )),
            KeyedState::Quin(ref mut map) => map.remove(&(
                key[0].clone(),
                key[1].clone(),
                key[2].clone(),
                key[3].clone(),
                key[4].clone(),
            )),
            KeyedState::Sex(ref mut map) => map.remove(&(
                key[0].clone(),
                key[1].clone(),
                key[2].clone(),
                key[3].clone(),
                key[4].clone(),
                key[5].clone(),
            )),
        };
        // mark_hole should only be called on keys we called mark_filled on
        assert!(removed.is_some());
    }

    fn lookup<'a>(&'a self, columns: &[usize], key: &KeyType) -> LookupResult<'a> {
        debug_assert!(!self.state.is_empty(), "lookup on uninitialized index");
        let index = &self.state[self.state_for(columns)
                                    .expect("lookup on non-indexed column set")];
        if let Some(rs) = index.state.lookup(key) {
            LookupResult::Some(Cow::Borrowed(&rs[..]))
        } else {
            if index.partial {
                // partially materialized, so this is a hole (empty results would be vec![])
                LookupResult::Missing
            } else {
                LookupResult::Some(Cow::Owned(vec![]))
            }
        }
    }

    /// Return a copy of all records. Panics if the state is only partially materialized.
    pub fn cloned_records(&self) -> Vec<Vec<DataType>> {
        fn fix<'a>(rs: &'a Vec<Row>) -> impl Iterator<Item = Vec<DataType>> + 'a {
            rs.iter().map(|r| Vec::clone(&**r))
        }

        match self.state[0].state {
            KeyedState::Single(ref map) => map.values().flat_map(fix).collect(),
            KeyedState::Double(ref map) => map.values().flat_map(fix).collect(),
            KeyedState::Tri(ref map) => map.values().flat_map(fix).collect(),
            KeyedState::Quad(ref map) => map.values().flat_map(fix).collect(),
            KeyedState::Quin(ref map) => map.values().flat_map(fix).collect(),
            KeyedState::Sex(ref map) => map.values().flat_map(fix).collect(),
        }
    }

    fn clear(&mut self) {
        self.rows = 0;
        for s in &mut self.state {
            match s.state {
                KeyedState::Single(ref mut map) => map.clear(),
                KeyedState::Double(ref mut map) => map.clear(),
                KeyedState::Tri(ref mut map) => map.clear(),
                KeyedState::Quad(ref mut map) => map.clear(),
                KeyedState::Quin(ref mut map) => map.clear(),
                KeyedState::Sex(ref mut map) => map.clear(),
            }
        }
    }

    /// Returns the index in `self.state` of the index keyed on `cols`, or None if no such index
    /// exists.
    fn state_for(&self, cols: &[usize]) -> Option<usize> {
        self.state.iter().position(|s| &s.key[..] == cols)
    }

    fn unalias_for_state(&mut self) {
        let left = self.state.drain(..).last();
        if let Some(left) = left {
            self.state.push(left);
        }
    }
}

impl<'a> Drop for MemoryState {
    fn drop(&mut self) {
        self.unalias_for_state();
        self.clear();
    }
}
