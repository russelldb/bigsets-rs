use bigsets::{Operation, Server, SqliteStorage};
use bytes::Bytes;
use proptest::prelude::*;
use proptest::test_runner::Config;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest, prop_state_machine};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use tokio::runtime::Handle;

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
            if self.removes.get(elem).map_or(false, |r| !tags.is_subset(r)) {
                members.insert(elem.clone());
            }
        }
        members
    }

    fn cardinality(&self) -> usize {
        self.members().len()
    }

    fn is_member(&self, elem: Bytes) -> bool {
        self.members().contains(&elem)
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

type NodeId = u64;

/// The operations on a Set
#[derive(Clone, Debug)]
pub enum SetOp {
    Add(Bytes),
    Remove(Bytes),
}

/// The operations on the cluster
#[derive(Clone, Debug)]
pub enum Ops {
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

    fn apply_op(&mut self, op: &SetOp);
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
    type State = AddWinsSet; // The whole thing is the state

    fn apply_op(&mut self, op: &SetOp) {
        todo!()
    }
    fn get_state(&self) -> Self::State {
        self.inner.clone()
    }
    fn merge_state(&mut self, state: Self::State) {
        self.inner.merge(state);
    }

    fn members(&self) -> BTreeSet<Bytes> {
        todo!()
    }

    fn cardinality(&self) -> usize {
        todo!()
    }

    fn is_member(&self, elem: &Bytes) -> bool {
        todo!()
    }
}

#[derive(Clone, Debug)]
struct BigsetNode {
    server: Arc<Server<SqliteStorage>>,
    rt: Handle,
}

impl Node for BigsetNode {
    type State = Vec<Operation>; // Operations are the replication unit

    fn apply_op(&mut self, op: &SetOp) {
        todo!()
    }
    fn get_state(&self) -> Self::State {
        todo!()
    }
    fn merge_state(&mut self, ops: Self::State) {
        todo!()
    }

    fn members(&self) -> BTreeSet<Bytes> {
        todo!()
    }

    fn cardinality(&self) -> usize {
        todo!()
    }

    fn is_member(&self, elem: &Bytes) -> bool {
        todo!()
    }
}

#[derive(Clone, Debug)]
struct Cluster<N: Node> {
    nodes: BTreeMap<NodeId, N>,
}

impl<N: Node> Cluster<N> {
    fn new() -> Self {
        Self {
            nodes: BTreeMap::new(),
        }
    }

    fn add_node(&mut self, id: NodeId, node: N) {
        self.nodes.insert(id, node);
    }

    fn apply_update(&mut self, node_id: NodeId, op: SetOp) {
        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.apply_op(&op);
        }
    }

    fn replicate(&mut self, from: NodeId, to: NodeId) {
        if let Some(state) = self.nodes.get(&from).map(|n| n.get_state()) {
            if let Some(node) = self.nodes.get_mut(&to) {
                node.merge_state(state);
            }
        }
    }

    // Verification: check all nodes converge
    fn all_nodes_converged(&self) -> bool {
        let members_sets: Vec<_> = self.nodes.values().map(|n| n.members()).collect();
        members_sets.windows(2).all(|w| w[0] == w[1])
    }
}

impl ReferenceStateMachine for Cluster<ModelNode> {
    type State = Self;
    type Transition = Ops;

    fn init_state() -> BoxedStrategy<Self::State> {
        // Create cluster with some initial nodes
        Just(Cluster::new()).boxed()
    }

    fn transitions(_state: &Self::State) -> BoxedStrategy<Self::Transition> {
        todo!()
    }

    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        match transition {
            Ops::Update(node_id, op) => {
                state.apply_update(*node_id, op.clone());
            }
            Ops::Replicate(from, to) => {
                state.replicate(*from, *to);
            }
        }
        state
    }

    fn preconditions(_state: &Self::State, _transition: &Self::Transition) -> bool {
        true // Or check node exists, etc.
    }
}

impl StateMachineTest for Cluster<BigsetNode> {
    type SystemUnderTest = Self;
    type Reference = Cluster<ModelNode>;

    fn init_test(
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        todo!()
    }

    fn apply(
        state: Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        transition: <Self::Reference as ReferenceStateMachine>::Transition,
    ) -> Self::SystemUnderTest {
        todo!()
    }
}
