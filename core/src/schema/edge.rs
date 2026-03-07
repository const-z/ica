use std::{fmt::Debug, hash::Hash};

use super::{EdgeId, NodeId};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Edge<A, T: Debug + Hash + Eq> {
    pub id: EdgeId<T>,
    pub from: NodeId<T>,
    pub to: NodeId<T>,
    pub attrs: A,
}

impl<A, T: Copy + Debug + Hash + Eq> Edge<A, T> {
    pub fn new(id: EdgeId<T>, from: NodeId<T>, to: NodeId<T>, attrs: A) -> Self {
        Self {
            id,
            from,
            to,
            attrs,
        }
    }
}
