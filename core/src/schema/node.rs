use std::{fmt::Debug, hash::Hash};

use super::NodeId;

#[derive(Debug, Clone)]
pub struct Node<A, T: Copy + Debug + Hash + Eq> {
    pub id: NodeId<T>,
    pub attrs: A,
}
