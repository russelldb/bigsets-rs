use bigsets::config::StorageConfig;
use bigsets::{ActorId, Operation, PendingBuffer, Server, SqliteStorage};
use bytes::Bytes;
use proptest::string::bytes_regex;
use proptest::test_runner::Config;
use proptest::{prelude::*, sample::select};
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest, prop_state_machine};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use tempfile::TempDir;

/// The most basic possible add wins set, to use as a full state model
#[derive(Clone, Debug)]
struct AddWinsSet {
    adds: BTreeMap<Bytes, BTreeSet<u64>>,
    removes: BTreeMap<Bytes, BTreeSet<u64>>,
}

impl AddWinsSet {
    fn new() -> Self {
        Self {
            adds: BTreeMap::new(),
            removes: BTreeMap::new(),
        }
    }

    fn add(&mut self, elem: Bytes, tag: u64) {
        if let Some(tags) = self.adds.get(&elem) {
            self.removes.entry(elem.clone()).or_default().extend(tags);
        }

        self.adds.entry(elem).or_default().insert(tag);
    }

    fn remove(&mut self, elem: Bytes) {
        if let Some(tags) = self.adds.get(&elem) {
            self.removes.entry(elem).or_default().extend(tags);
        }
    }

    fn members(&self) -> BTreeSet<Bytes> {
        let mut members = BTreeSet::new();
        for (elem, tags) in self.adds.iter() {
            if self.removes.get(elem).map_or(true, |r| !tags.is_subset(r)) {
                members.insert(elem.clone());
            }
        }
        members
    }

    fn cardinality(&self) -> usize {
        self.members().len()
    }

    fn is_member(&self, elem: &Bytes) -> bool {
        self.members().contains(elem)
    }

    fn merge(&mut self, other: Self) {
        for (elem, tags) in other.removes {
            self.removes.entry(elem.clone()).or_default().extend(tags);
        }
        for (elem, tags) in other.adds {
            self.adds.entry(elem.clone()).or_default().extend(tags);
        }
    }
}

type NodeId = u16;

/// The operations on a Set
#[derive(Clone, Debug)]
pub enum SetOp {
    Add(Bytes),
    Remove(Bytes),
}

/// The operations on the cluster
#[derive(Clone, Debug)]
pub enum Op {
    Update(NodeId, SetOp),
    Replicate(NodeId, NodeId),
}

// Setup the state machine test using the `prop_state_machine!` macro
prop_state_machine! {
    #![proptest_config(Config {
        // Enable verbose mode to make the state machine test print the
        // transitions for each case.
        verbose: 1,
        .. Config::default()
    })]

    #[test]
    fn bigsets_stateful(
        sequential
        // The number of transitions to be generated for each case. This can
        // be a single numerical value or a range as in here.
        10..100
        =>
        Cluster<BigsetNode>
    );
}

trait Node {
    type State;

    fn new(id: u16) -> Self;
    fn apply_op(&mut self, op: &SetOp, time: u64);
    fn get_state(&self) -> Self::State;
    fn merge_state(&mut self, state: Self::State);

    fn members(&self) -> BTreeSet<Bytes>;
    fn cardinality(&self) -> usize;
    fn is_member(&self, elem: &Bytes) -> bool;
}

#[derive(Clone, Debug)]
struct ModelNode {
    inner: AddWinsSet,
}

impl Node for ModelNode {
    type State = AddWinsSet;

    fn apply_op(&mut self, op: &SetOp, time: u64) {
        match op {
            SetOp::Add(bytes) => self.inner.add(bytes.clone(), time),
            SetOp::Remove(bytes) => self.inner.remove(bytes.clone()),
        }
    }
    fn get_state(&self) -> Self::State {
        self.inner.clone()
    }
    fn merge_state(&mut self, state: Self::State) {
        self.inner.merge(state);
    }

    fn members(&self) -> BTreeSet<Bytes> {
        self.inner.members()
    }

    fn cardinality(&self) -> usize {
        self.inner.cardinality()
    }

    fn is_member(&self, elem: &Bytes) -> bool {
        self.inner.is_member(elem)
    }

    fn new(_id: u16) -> Self {
        Self {
            inner: AddWinsSet::new(),
        }
    }
}
const SET_NAME: &'static str = "testset";

#[derive(Debug)]
struct BigsetNode {
    _temp_dir: TempDir,
    actor_id: ActorId,
    out_buffer: Vec<Operation>,
    pending_buffer: PendingBuffer,
    server: Arc<Server<SqliteStorage>>,
    rt: tokio::runtime::Runtime,
}

impl Clone for BigsetNode {
    fn clone(&self) -> Self {
        // Create a new runtime for the cloned instance
        let rt = tokio::runtime::Runtime::new().unwrap();

        // For TempDir, we need to create a new one since it's not Clone
        let temp_dir = TempDir::new().unwrap();

        // Need to recreate the storage with the new temp directory
        let db_path = temp_dir.path().join("test.db");
        let config = StorageConfig {
            sqlite_cache_size: 1000,
            sqlite_busy_timeout: 5000,
        };

        let storage = Arc::new(SqliteStorage::open(&db_path, &config).unwrap());

        // Create a new server with the same actor_id but new storage
        let server =
            rt.block_on(async { Server::new(self.actor_id.clone(), storage).await.unwrap() });

        Self {
            _temp_dir: temp_dir,
            actor_id: self.actor_id.clone(),
            out_buffer: self.out_buffer.clone(),
            pending_buffer: self.pending_buffer.clone(),
            server: Arc::new(server),
            rt,
        }
    }
}

impl Node for BigsetNode {
    type State = Vec<Operation>; // Operations are the replication unit

    fn apply_op(&mut self, op: &SetOp, _time: u64) {
        self.rt.block_on(async {
            println!("applying {:?} to {:?}", op, self.actor_id);
            let res = match op {
                SetOp::Add(bytes) => self.server.sadd(SET_NAME, &[bytes.clone()]).await,
                SetOp::Remove(bytes) => self.server.srem(SET_NAME, &[bytes.clone()]).await,
            };

            println!("result {:?}", res);

            match res {
                Ok((_, Some(rep_op))) => self.out_buffer.push(rep_op),
                Ok(_) => (),
                Err(e) => panic!("error {} applying op {:?}", e, op),
            }
        })
    }

    fn get_state(&self) -> Self::State {
        self.out_buffer.clone()
    }
    fn merge_state(&mut self, ops: Self::State) {
        // we can be smarter and not add any ops that were sent from us (the dot says who)
        for op in ops {
            println!("adding op {:?} to pending", op);
            self.pending_buffer.add(op);
        }

        self.rt.block_on(async {
            // now just run through pending until none can be applied or it is empty
            let mut progress = Some(true);
            while progress.take().unwrap_or(false) {
                for op in self.pending_buffer.drain() {
                    println!("applying {:?} to {:?}", op, self.actor_id);
                    if self
                        .server
                        .apply_remote_operation(op.clone())
                        .await
                        .unwrap()
                    {
                        println!("applied {:?} to {:?}", op, self.actor_id);
                        progress = Some(true);
                        // If we want to get closer to the model, we can send these ops on by adding to our out buffer
                    } else {
                        self.pending_buffer.add(op);
                    }
                }
            }
        })
    }

    fn members(&self) -> BTreeSet<Bytes> {
        self.rt.block_on(async {
            match self.server.smembers(SET_NAME, None).await.unwrap() {
                bigsets::CommandResult::BytesArray(items) => items.into_iter().collect(),
                _ => panic!("Unexpected command result"),
            }
        })
    }

    fn cardinality(&self) -> usize {
        // tired and lazy, use members for now
        self.members().len()
    }

    fn is_member(&self, elem: &Bytes) -> bool {
        self.members().contains(elem)
    }

    fn new(id: u16) -> Self {
        let actor_id = ActorId::from_node_id(id);
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let config = StorageConfig {
            sqlite_cache_size: 1000,
            sqlite_busy_timeout: 5000,
        };

        let storage = Arc::new(SqliteStorage::open(&db_path, &config).unwrap());

        let rt = tokio::runtime::Runtime::new().unwrap();
        let server = rt.block_on(async {
            let server = Server::new(actor_id, storage).await.unwrap();
            server
        });
        Self {
            _temp_dir: temp_dir,
            actor_id,
            out_buffer: Vec::new(),
            pending_buffer: PendingBuffer::new(1000), // just making it up
            server: Arc::new(server),
            rt,
        }
    }
}

#[derive(Clone, Debug)]
struct Cluster<N: Node> {
    clock: u64,
    nodes: BTreeMap<NodeId, N>,
}

impl<N: Node> Cluster<N> {
    fn new(size: u16) -> Self {
        let mut s = Self {
            clock: 0,
            nodes: BTreeMap::new(),
        };

        for id in 1..=size {
            s.add_node(id, N::new(id))
        }
        s
    }

    fn add_node(&mut self, id: NodeId, node: N) {
        self.nodes.insert(id, node);
    }

    fn apply_update(&mut self, node_id: NodeId, op: SetOp) {
        if let Some(node) = self.nodes.get_mut(&node_id) {
            self.clock += 1;
            node.apply_op(&op, self.clock);
        }
    }

    fn replicate(&mut self, from: NodeId, to: NodeId) {
        if let Some(state) = self.nodes.get(&from).map(|n| n.get_state()) {
            if let Some(node) = self.nodes.get_mut(&to) {
                node.merge_state(state);
            }
        }
    }

    fn node_members(&self, id: NodeId) -> BTreeSet<Bytes> {
        self.nodes
            .get(&id)
            .map_or(BTreeSet::new(), |node| node.members())
    }
}

impl ReferenceStateMachine for Cluster<ModelNode> {
    type State = Self;
    type Transition = Op;

    fn init_state() -> BoxedStrategy<Self::State> {
        Just(Cluster::new(3)).boxed()
    }

    fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
        let node_ids = state.nodes.keys().cloned().collect::<Vec<_>>();

        let nodes_with_members = state
            .nodes
            .iter()
            .filter_map(|(id, node)| {
                let members = node.members();
                if !members.is_empty() {
                    Some((*id, members))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let add_strat = (
            select(node_ids.clone()),
            bytes_regex("[a-zA-Z0-9]{1,10}").unwrap(),
        )
            .prop_map(|(id, data)| {
                let bytes = Bytes::from(data);
                let op = SetOp::Add(bytes);
                Op::Update(id, op)
            })
            .boxed();

        let update_strat = if !nodes_with_members.is_empty() {
            let rem_strat = select(nodes_with_members)
                .prop_flat_map(|(id, members)| {
                    let members = members.into_iter().collect::<Vec<_>>();
                    let elem = select(members);
                    (Just(id), elem)
                })
                .prop_map(|(id, elem)| Op::Update(id, SetOp::Remove(elem)))
                .boxed();
            prop_oneof![rem_strat, add_strat].boxed()
        } else {
            add_strat
        };

        // Create a strategy for replication between nodes
        let replicate_strat = (1u16..=3u16, 1u16..=3u16)
            .prop_filter("source and target must be different", |(from, to)| {
                from != to
            })
            .prop_map(|(from, to)| Op::Replicate(from, to))
            .boxed();
        prop_oneof![replicate_strat, update_strat].boxed()
    }

    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        match transition {
            Op::Update(node_id, op) => {
                state.apply_update(*node_id, op.clone());
            }
            Op::Replicate(from, to) => {
                state.replicate(*from, *to);
            }
        }
        state
    }

    fn preconditions(state: &Self::State, transition: &Self::Transition) -> bool {
        match transition {
            Op::Update(node, op) => match op {
                SetOp::Add(_) => true,
                SetOp::Remove(bytes) => state.node_members(*node).contains(bytes),
            },
            Op::Replicate(from, to) => from != to,
        }
    }
}

impl StateMachineTest for Cluster<BigsetNode> {
    type SystemUnderTest = Self;
    type Reference = Cluster<ModelNode>;

    fn init_test(
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        Cluster::new(3)
    }

    fn apply(
        mut state: Self::SystemUnderTest,
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        transition: <Self::Reference as ReferenceStateMachine>::Transition,
    ) -> Self::SystemUnderTest {
        match transition {
            Op::Update(node_id, op) => {
                state.apply_update(node_id, op);
            }
            Op::Replicate(from, to) => {
                state.replicate(from, to);
            }
        }
        state
    }

    fn check_invariants(
        state: &Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) {
        for id in state.nodes.keys() {
            let node_members = state.node_members(*id);
            let ref_node_members = ref_state.node_members(*id);
            println!("node {:?}, members: {:?}", id, node_members);
            println!("ref node {:?}, members: {:?}", id, ref_node_members);

            assert_eq!(node_members, ref_node_members, "{:?}", ref_state);
        }
    }
}
