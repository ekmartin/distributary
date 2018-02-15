use ::*;
use std::borrow::Cow;
use std::ops::{Deref, Index, IndexMut};
use std::iter::FromIterator;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use serde_json;

use rusqlite::{self, Connection};
use rusqlite::types::{ToSql, ToSqlOutput};

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub struct Tag(pub u32);
impl Tag {
    pub fn id(&self) -> u32 {
        self.0
    }
}

#[derive(Serialize, Deserialize)]
pub struct Map<T> {
    n: usize,
    things: Vec<Option<T>>,
}

impl<T> Default for Map<T> {
    fn default() -> Self {
        Map {
            n: 0,
            things: Vec::default(),
        }
    }
}

impl<T: Clone> Clone for Map<T> {
    fn clone(&self) -> Self {
        Map {
            n: self.n,
            things: self.things.clone(),
        }
    }
}

pub enum Entry<'a, V: 'a> {
    Vacant(VacantEntry<'a, V>),
    Occupied(OccupiedEntry<'a, V>),
}

pub struct VacantEntry<'a, V: 'a> {
    map: &'a mut Map<V>,
    index: LocalNodeIndex,
}

pub struct OccupiedEntry<'a, V: 'a> {
    map: &'a mut Map<V>,
    index: LocalNodeIndex,
}

impl<'a, V> Entry<'a, V> {
    pub fn or_insert(self, default: V) -> &'a mut V {
        match self {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(default),
        }
    }
    pub fn or_default(self) -> &'a mut V
    where
        V: Default,
    {
        match self {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(V::default()),
        }
    }
    pub fn or_insert_with<F: FnOnce() -> V>(self, default: F) -> &'a mut V {
        match self {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(default()),
        }
    }
}

impl<'a, V> VacantEntry<'a, V> {
    pub fn insert(self, value: V) -> &'a mut V {
        let index = self.index;
        self.map.insert(index, value);
        &mut self.map[&index]
    }
}

impl<'a, V> OccupiedEntry<'a, V> {
    pub fn get(&self) -> &V {
        let index = self.index;
        &self.map[&index]
    }
    pub fn get_mut(&mut self) -> &mut V {
        let index = self.index;
        &mut self.map[&index]
    }
    pub fn into_mut(self) -> &'a mut V {
        let index = self.index;
        &mut self.map[&index]
    }
    pub fn insert(&mut self, value: V) -> V {
        let index = self.index;
        self.map.insert(index, value).unwrap()
    }
    pub fn remove(self) -> V {
        let index = self.index;
        self.map.remove(&index).unwrap()
    }
}

impl<T> Map<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, addr: LocalNodeIndex, value: T) -> Option<T> {
        let i = addr.id();

        if i >= self.things.len() {
            let diff = i - self.things.len() + 1;
            self.things.reserve(diff);
            for _ in 0..diff {
                self.things.push(None);
            }
        }

        let old = self.things[i].take();
        self.things[i] = Some(value);
        if old.is_none() {
            self.n += 1;
        }
        old
    }

    pub fn get(&self, addr: &LocalNodeIndex) -> Option<&T> {
        self.things.get(addr.id()).and_then(|v| v.as_ref())
    }

    pub fn get_mut(&mut self, addr: &LocalNodeIndex) -> Option<&mut T> {
        self.things.get_mut(addr.id()).and_then(|v| v.as_mut())
    }

    pub fn contains_key(&self, addr: &LocalNodeIndex) -> bool {
        self.things
            .get(addr.id())
            .map(|v| v.is_some())
            .unwrap_or(false)
    }

    pub fn remove(&mut self, addr: &LocalNodeIndex) -> Option<T> {
        let i = addr.id();
        if i >= self.things.len() {
            return None;
        }

        let ret = self.things[i].take();
        if ret.is_some() {
            self.n -= 1;
        }
        ret
    }

    pub fn iter<'a>(&'a self) -> Box<Iterator<Item = (LocalNodeIndex, &'a T)> + 'a> {
        Box::new(self.things.iter().enumerate().filter_map(|(i, t)| {
            t.as_ref()
                .map(|v| (unsafe { LocalNodeIndex::make(i as u32) }, v))
        }))
    }

    pub fn iter_mut<'a>(&'a mut self) -> Box<Iterator<Item = (LocalNodeIndex, &'a mut T)> + 'a> {
        Box::new(self.things.iter_mut().enumerate().filter_map(|(i, t)| {
            t.as_mut()
                .map(|v| (unsafe { LocalNodeIndex::make(i as u32) }, v))
        }))
    }

    pub fn values<'a>(&'a self) -> Box<Iterator<Item = &'a T> + 'a> {
        Box::new(self.things.iter().filter_map(|t| t.as_ref()))
    }

    pub fn len(&self) -> usize {
        self.n
    }

    pub fn entry(&mut self, key: LocalNodeIndex) -> Entry<T> {
        if self.contains_key(&key) {
            Entry::Occupied(OccupiedEntry {
                map: self,
                index: key,
            })
        } else {
            Entry::Vacant(VacantEntry {
                map: self,
                index: key,
            })
        }
    }
}

use std::fmt;
impl<T> fmt::Debug for Map<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<'a, T> Index<&'a LocalNodeIndex> for Map<T> {
    type Output = T;
    fn index(&self, index: &LocalNodeIndex) -> &Self::Output {
        self.get(index).unwrap()
    }
}
impl<'a, T> IndexMut<&'a LocalNodeIndex> for Map<T> {
    fn index_mut(&mut self, index: &LocalNodeIndex) -> &mut Self::Output {
        self.get_mut(index).unwrap()
    }
}

impl<T> FromIterator<(LocalNodeIndex, T)> for Map<T> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (LocalNodeIndex, T)>,
    {
        use std::collections::BTreeMap;

        // we've got to be a bit careful here, as the nodes may come in any order
        // we therefore sort them first
        let sorted = BTreeMap::from_iter(iter.into_iter().map(|(ni, v)| (ni.id(), v)));

        // no entries -- fine
        if sorted.is_empty() {
            return Map::default();
        }

        let n = sorted.len();
        let end = sorted.keys().last().unwrap() + 1;
        let mut vs = Vec::with_capacity(end);
        for (i, v) in sorted {
            for _ in vs.len()..i {
                vs.push(None);
            }
            vs.push(Some(v));
        }

        Map { n: n, things: vs }
    }
}

impl<T: 'static> IntoIterator for Map<T> {
    type Item = (LocalNodeIndex, T);
    type IntoIter = Box<Iterator<Item = Self::Item>>;
    fn into_iter(self) -> Self::IntoIter {
        Box::new(
            self.things
                .into_iter()
                .enumerate()
                .filter_map(|(i, v)| v.map(|v| (unsafe { LocalNodeIndex::make(i as u32) }, v))),
        )
    }
}

use std::collections::hash_map;
use fnv::FnvHashMap;

#[derive(Clone)]
pub struct Row(Rc<Vec<DataType>>);

unsafe impl Send for Row {}

impl Row {
    pub fn unpack(self) -> Vec<DataType> {
        Rc::try_unwrap(self.0).unwrap()
    }
}

impl Deref for Row {
    type Target = Vec<DataType>;
    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

#[derive(Clone)]
pub enum KeyType<'a> {
    Single(&'a DataType),
    Double((DataType, DataType)),
    Tri((DataType, DataType, DataType)),
    Quad((DataType, DataType, DataType, DataType)),
    Quin((DataType, DataType, DataType, DataType, DataType)),
    Sex((DataType, DataType, DataType, DataType, DataType, DataType)),
}

enum KeyedState {
    Single(FnvHashMap<DataType, Vec<Row>>),
    Double(FnvHashMap<(DataType, DataType), Vec<Row>>),
    Tri(FnvHashMap<(DataType, DataType, DataType), Vec<Row>>),
    Quad(FnvHashMap<(DataType, DataType, DataType, DataType), Vec<Row>>),
    Quin(FnvHashMap<(DataType, DataType, DataType, DataType, DataType), Vec<Row>>),
    Sex(FnvHashMap<(DataType, DataType, DataType, DataType, DataType, DataType), Vec<Row>>),
}

impl<'a> KeyType<'a> {
    pub fn from<I>(other: I) -> Self
    where
        I: IntoIterator<Item = &'a DataType>,
        <I as IntoIterator>::IntoIter: ExactSizeIterator,
    {
        let mut other = other.into_iter();
        let len = other.len();
        let mut more = move || other.next().unwrap();
        match len {
            0 => unreachable!(),
            1 => KeyType::Single(more()),
            2 => KeyType::Double((more().clone(), more().clone())),
            3 => KeyType::Tri((more().clone(), more().clone(), more().clone())),
            4 => KeyType::Quad((
                more().clone(),
                more().clone(),
                more().clone(),
                more().clone(),
            )),
            5 => KeyType::Quin((
                more().clone(),
                more().clone(),
                more().clone(),
                more().clone(),
                more().clone(),
            )),
            6 => KeyType::Sex((
                more().clone(),
                more().clone(),
                more().clone(),
                more().clone(),
                more().clone(),
                more().clone(),
            )),
            _ => unimplemented!(),
        }
    }
}

impl KeyedState {
    pub fn is_empty(&self) -> bool {
        match *self {
            KeyedState::Single(ref m) => m.is_empty(),
            KeyedState::Double(ref m) => m.is_empty(),
            KeyedState::Tri(ref m) => m.is_empty(),
            KeyedState::Quad(ref m) => m.is_empty(),
            KeyedState::Quin(ref m) => m.is_empty(),
            KeyedState::Sex(ref m) => m.is_empty(),
        }
    }

    pub fn len(&self) -> usize {
        match *self {
            KeyedState::Single(ref m) => m.len(),
            KeyedState::Double(ref m) => m.len(),
            KeyedState::Tri(ref m) => m.len(),
            KeyedState::Quad(ref m) => m.len(),
            KeyedState::Quin(ref m) => m.len(),
            KeyedState::Sex(ref m) => m.len(),
        }
    }

    pub fn lookup<'a>(&'a self, key: &KeyType) -> Option<&'a Vec<Row>> {
        match (self, key) {
            (&KeyedState::Single(ref m), &KeyType::Single(k)) => m.get(k),
            (&KeyedState::Double(ref m), &KeyType::Double(ref k)) => m.get(k),
            (&KeyedState::Tri(ref m), &KeyType::Tri(ref k)) => m.get(k),
            (&KeyedState::Quad(ref m), &KeyType::Quad(ref k)) => m.get(k),
            (&KeyedState::Quin(ref m), &KeyType::Quin(ref k)) => m.get(k),
            (&KeyedState::Sex(ref m), &KeyType::Sex(ref k)) => m.get(k),
            _ => unreachable!(),
        }
    }
}

impl<'a> Into<KeyedState> for &'a [usize] {
    fn into(self) -> KeyedState {
        match self.len() {
            0 => unreachable!(),
            1 => KeyedState::Single(FnvHashMap::default()),
            2 => KeyedState::Double(FnvHashMap::default()),
            3 => KeyedState::Tri(FnvHashMap::default()),
            4 => KeyedState::Quad(FnvHashMap::default()),
            5 => KeyedState::Quin(FnvHashMap::default()),
            6 => KeyedState::Sex(FnvHashMap::default()),
            x => panic!("invalid compound key of length: {}", x),
        }
    }
}

pub enum LookupResult<'a> {
    Some(Cow<'a, [Row]>),
    Missing,
}

struct SingleState {
    key: Vec<usize>,
    state: KeyedState,
    partial: Option<Vec<Tag>>,
}

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
            State::Persistent(ref mut s) => s.remove(r),
        }
    }

    pub fn iter(&self) -> hash_map::Values<DataType, Vec<Row>> {
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
            indices: Default::default(),
        }
    }

    // Joins together a SQL clause on the form of
    // index_0 = columns[0], index_1 = columns[1]...
    fn build_clause<'a, I>(columns: I) -> String
    where
        I: Iterator<Item = &'a usize>,
    {
        columns
            .enumerate()
            .map(|(i, column)| format!("index_{} = ?{}", column, i + 1))
            .collect::<Vec<_>>()
            .join(", ")
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

        let mut statement = self.connection
            .prepare_cached(&format!(
                "INSERT INTO store ({}) VALUES ({})",
                columns, placeholders
            ))
            .unwrap();

        let row = serde_json::to_string(&r).unwrap();
        let mut values: Vec<&ToSql> = vec![&row];
        let mut index_values = self.indices
            .iter()
            .map(|index| &r[*index] as &ToSql)
            .collect::<Vec<&ToSql>>();

        values.append(&mut index_values);
        statement.execute(&values[..]).unwrap();
        true
    }

    fn lookup(&self, columns: &[usize], key: &KeyType) -> LookupResult {
        let clauses = Self::build_clause(columns.iter());
        let query = format!("SELECT row FROM store WHERE {}", clauses);
        let mut statement = self.connection.prepare_cached(&query).unwrap();

        let mapper = |result: &rusqlite::Row| -> Vec<DataType> {
            let row: String = result.get(0);
            serde_json::from_str(&row).unwrap()
        };

        let rows = match *key {
            KeyType::Single(a) => statement.query_map(&[a], mapper),
            KeyType::Double(ref r) => statement.query_map(&[&r.0, &r.1], mapper),
            KeyType::Tri(ref r) => statement.query_map(&[&r.0, &r.1, &r.2], mapper),
            KeyType::Quad(ref r) => statement.query_map(&[&r.0, &r.1, &r.2, &r.3], mapper),
            KeyType::Quin(ref r) => statement.query_map(&[&r.0, &r.1, &r.2, &r.3, &r.4], mapper),
            KeyType::Sex(ref r) => {
                statement.query_map(&[&r.0, &r.1, &r.2, &r.3, &r.4, &r.5], mapper)
            }
        };

        let data = rows.unwrap()
            .map(|row| Row(Rc::new(row.unwrap())))
            .collect::<Vec<_>>();

        LookupResult::Some(Cow::Owned(data))
    }

    fn remove(&mut self, r: &[DataType]) -> bool {
        let clauses = Self::build_clause(self.indices.iter());
        let index_values = self.indices
            .iter()
            .map(|index| &r[*index] as &ToSql)
            .collect::<Vec<&ToSql>>();

        let query = format!("DELETE FROM store WHERE {}", clauses);
        let mut statement = self.connection.prepare_cached(&query).unwrap();
        statement.execute(&index_values[..]).unwrap() > 0
    }
}

pub struct MemoryState {
    state: Vec<SingleState>,
    by_tag: HashMap<Tag, usize>,
    rows: usize,
}

impl Default for MemoryState {
    fn default() -> Self {
        MemoryState {
            state: Vec::new(),
            by_tag: HashMap::new(),
            rows: 0,
        }
    }
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

        let is_partial = partial.is_some();
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
            partial: partial,
        });

        if !self.is_empty() {
            // we need to *construct* the index!
            if is_partial {
                // partial views can start out empty
                return;
            }

            let (new, old) = self.state.split_last_mut().unwrap();
            let mut insert = move |rs: &Vec<Row>| {
                for r in rs {
                    MemoryState::insert_into(new, Row(r.0.clone()));
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

    fn keys(&self) -> Vec<Vec<usize>> {
        self.state.iter().map(|s| &s.key).cloned().collect()
    }

    fn is_useful(&self) -> bool {
        !self.state.is_empty()
    }

    fn is_partial(&self) -> bool {
        self.state.iter().any(|s| s.partial.is_some())
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
            MemoryState::insert_into(&mut self.state[i], Row(r))
        } else {
            let mut hit_any = true;
            self.rows = self.rows.saturating_add(1);
            for i in 0..self.state.len() {
                hit_any = MemoryState::insert_into(&mut self.state[i], Row(r.clone())) || hit_any;
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
                _ => {
                    match s.state {
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
                        KeyedState::Single(..) => unreachable!(),
                    }
                }
            }
        }

        if removed {
            self.rows = self.rows.saturating_sub(1);
        }

        hit
    }

    fn iter(&self) -> hash_map::Values<DataType, Vec<Row>> {
        for index in &self.state {
            if let KeyedState::Single(ref map) = index.state {
                if index.partial.is_some() {
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
            if index.partial.is_some() {
                // partially materialized, so this is a hole (empty results would be vec![])
                LookupResult::Missing
            } else {
                LookupResult::Some(Cow::Owned(vec![]))
            }
        }
    }

    fn cloned_records(&self) -> Vec<Vec<DataType>> {
        match self.state[0].state {
            KeyedState::Single(ref map) => map.values().flat_map(MemoryState::fix).collect(),
            KeyedState::Double(ref map) => map.values().flat_map(MemoryState::fix).collect(),
            KeyedState::Tri(ref map) => map.values().flat_map(MemoryState::fix).collect(),
            KeyedState::Quad(ref map) => map.values().flat_map(MemoryState::fix).collect(),
            KeyedState::Quin(ref map) => map.values().flat_map(MemoryState::fix).collect(),
            KeyedState::Sex(ref map) => map.values().flat_map(MemoryState::fix).collect(),
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

    fn state_for(&self, cols: &[usize]) -> Option<usize> {
        self.state.iter().position(|s| &s.key[..] == cols)
    }

    /// Insert the given record into the given state.
    ///
    /// Returns false if a hole was encountered (and the record hence not inserted).
    fn insert_into(s: &mut SingleState, r: Row) -> bool {
        use std::collections::hash_map::Entry;
        match s.state {
            KeyedState::Single(ref mut map) => {
                // treat this specially to avoid the extra Vec
                debug_assert_eq!(s.key.len(), 1);
                // i *wish* we could use the entry API here, but it would mean an extra clone
                // in the common case of an entry already existing for the given key...
                if let Some(ref mut rs) = map.get_mut(&r[s.key[0]]) {
                    rs.push(r);
                    return true;
                } else if s.partial.is_some() {
                    // trying to insert a record into partial materialization hole!
                    return false;
                }
                map.insert(r[s.key[0]].clone(), vec![r]);
            }
            _ => match s.state {
                KeyedState::Double(ref mut map) => {
                    let key = (r[s.key[0]].clone(), r[s.key[1]].clone());
                    match map.entry(key) {
                        Entry::Occupied(mut rs) => rs.get_mut().push(r),
                        Entry::Vacant(..) if s.partial.is_some() => return false,
                        rs @ Entry::Vacant(..) => rs.or_default().push(r),
                    }
                }
                KeyedState::Tri(ref mut map) => {
                    let key = (
                        r[s.key[0]].clone(),
                        r[s.key[1]].clone(),
                        r[s.key[2]].clone(),
                    );
                    match map.entry(key) {
                        Entry::Occupied(mut rs) => rs.get_mut().push(r),
                        Entry::Vacant(..) if s.partial.is_some() => return false,
                        rs @ Entry::Vacant(..) => rs.or_default().push(r),
                    }
                }
                KeyedState::Quad(ref mut map) => {
                    let key = (
                        r[s.key[0]].clone(),
                        r[s.key[1]].clone(),
                        r[s.key[2]].clone(),
                        r[s.key[3]].clone(),
                    );
                    match map.entry(key) {
                        Entry::Occupied(mut rs) => rs.get_mut().push(r),
                        Entry::Vacant(..) if s.partial.is_some() => return false,
                        rs @ Entry::Vacant(..) => rs.or_default().push(r),
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
                    match map.entry(key) {
                        Entry::Occupied(mut rs) => rs.get_mut().push(r),
                        Entry::Vacant(..) if s.partial.is_some() => return false,
                        rs @ Entry::Vacant(..) => rs.or_default().push(r),
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
                    match map.entry(key) {
                        Entry::Occupied(mut rs) => rs.get_mut().push(r),
                        Entry::Vacant(..) if s.partial.is_some() => return false,
                        rs @ Entry::Vacant(..) => rs.or_default().push(r),
                    }
                }
                KeyedState::Single(..) => unreachable!(),
            },
        }

        true
    }

    fn fix<'a>(rs: &'a Vec<Row>) -> impl Iterator<Item = Vec<DataType>> + 'a {
        rs.iter().map(|r| Vec::clone(&**r))
    }
}

impl<'a> MemoryState {
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

impl IntoIterator for MemoryState {
    type Item = Vec<Vec<DataType>>;
    type IntoIter = Box<Iterator<Item = Self::Item>>;
    fn into_iter(mut self) -> Self::IntoIter {
        // we need to make sure that the records eventually get dropped, so we need to ensure there
        // is only one index left (which therefore owns the records), and then cast back to the
        // original boxes.
        self.unalias_for_state();
        let own = |rs: Vec<Row>| match rs.into_iter()
            .map(|r| Rc::try_unwrap(r.0))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(rs) => rs,
            Err(_) => unreachable!("rc still not owned after unaliasing"),
        };
        self.state
            .drain(..)
            .last()
            .map(move |index| -> Self::IntoIter {
                match index.state {
                    KeyedState::Single(map) => Box::new(map.into_iter().map(move |(_, v)| own(v))),
                    KeyedState::Double(map) => Box::new(map.into_iter().map(move |(_, v)| own(v))),
                    KeyedState::Tri(map) => Box::new(map.into_iter().map(move |(_, v)| own(v))),
                    KeyedState::Quad(map) => Box::new(map.into_iter().map(move |(_, v)| own(v))),
                    KeyedState::Quin(map) => Box::new(map.into_iter().map(move |(_, v)| own(v))),
                    KeyedState::Sex(map) => Box::new(map.into_iter().map(move |(_, v)| own(v))),
                }
            })
            .unwrap()
    }
}
