use std::{fmt::Debug, hash::Hash};

use super::NodeId;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Node<A, T: Debug + Hash + Eq> {
    pub id: NodeId<T>,
    pub attrs: A,
}
