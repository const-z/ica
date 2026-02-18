use std::{fmt::Debug, hash::Hash};

use super::{EdgeId, NodeId};

#[derive(Debug, Clone)]
pub struct Edge<A, T: Copy + Debug + Hash + Eq> {
    pub id: EdgeId<T>,
    pub from: NodeId<T>,
    pub to: NodeId<T>,
    pub attrs: A,
}
