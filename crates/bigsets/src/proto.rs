// Generated protobuf code for replication protocol
pub mod replication {
    include!(concat!(env!("OUT_DIR"), "/bigsets.replication.rs"));
}

// Don't glob re-export to avoid naming conflicts with crate::types
// Users should access protobuf types via proto::replication::*

use crate::types::{Dot, OpType, Operation, VersionVector};

/// Convert internal Operation to protobuf Operation
pub fn operation_to_proto(op: &Operation) -> replication::Operation {
    let context = version_vector_to_proto(&op.context);

    let op_type = match &op.op_type {
        OpType::Add {
            elements,
            dot,
            removed_dots,
        } => Some(replication::operation::OpType::Add(replication::AddOp {
            elements: elements.clone(),
            dot: Some(dot_to_proto(dot)),
            removed_dots: removed_dots.iter().map(dot_to_proto).collect(),
        })),
        OpType::Remove {
            elements,
            dot,
            removed_dots,
        } => Some(replication::operation::OpType::Remove(
            replication::RemoveOp {
                elements: elements.clone(),
                dot: Some(dot_to_proto(dot)),
                removed_dots: removed_dots.iter().map(dot_to_proto).collect(),
            },
        )),
    };

    replication::Operation {
        set_name: op.set_name.clone(),
        context: Some(context),
        op_type,
    }
}

/// Convert protobuf Operation to internal Operation
pub fn proto_to_operation(proto: &replication::Operation) -> Option<Operation> {
    let context = proto_to_version_vector(proto.context.as_ref()?)?;

    let op_type = match proto.op_type.as_ref()? {
        replication::operation::OpType::Add(add_op) => OpType::Add {
            elements: add_op.elements.clone(),
            dot: proto_to_dot(add_op.dot.as_ref()?)?,
            removed_dots: add_op
                .removed_dots
                .iter()
                .filter_map(proto_to_dot)
                .collect(),
        },
        replication::operation::OpType::Remove(rem_op) => OpType::Remove {
            elements: rem_op.elements.clone(),
            dot: proto_to_dot(rem_op.dot.as_ref()?)?,
            removed_dots: rem_op
                .removed_dots
                .iter()
                .filter_map(proto_to_dot)
                .collect(),
        },
    };

    Some(Operation {
        set_name: proto.set_name.clone(),
        op_type,
        context,
    })
}

fn dot_to_proto(dot: &Dot) -> replication::Dot {
    replication::Dot {
        actor_id: dot.actor_id.to_bytes().to_vec().into(),
        counter: dot.counter,
    }
}

fn proto_to_dot(proto: &replication::Dot) -> Option<Dot> {
    let actor_id = crate::types::ActorId::from_bytes(&proto.actor_id).ok()?;
    Some(Dot {
        actor_id,
        counter: proto.counter,
    })
}

fn version_vector_to_proto(vv: &VersionVector) -> replication::VersionVector {
    let entries = vv
        .counters
        .iter()
        .map(|(actor_id, &counter)| replication::VectorEntry {
            actor_id: actor_id.to_bytes().to_vec().into(),
            counter,
        })
        .collect();

    replication::VersionVector { entries }
}

fn proto_to_version_vector(proto: &replication::VersionVector) -> Option<VersionVector> {
    let mut counters = std::collections::HashMap::new();
    for entry in &proto.entries {
        let actor_id = crate::types::ActorId::from_bytes(&entry.actor_id).ok()?;
        counters.insert(actor_id, entry.counter);
    }
    Some(VersionVector { counters })
}
