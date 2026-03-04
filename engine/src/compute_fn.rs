use std::{collections::HashMap, fmt::Debug, hash::Hash, sync::Arc};

use ica_core::{NodeId, Schema};
use tokio::sync::RwLock;

pub async fn compute<NA, EA, T, C>(
    schema: Arc<RwLock<Schema<NA, EA, T>>>,
    seeds: Arc<RwLock<HashMap<NodeId<T>, f64>>>,
    f: C,
) where
    C: Fn(NodeId<T>, f64),
    T: Debug + Hash + Eq + Clone + Default,
    NA: Default,
    EA: Default,
{
    let schema = schema.read().await;
    let mut seeds = seeds.write().await;

    schema.compute(|node_id, children| {
        let state = if children.is_empty() {
            *seeds.get(node_id).unwrap_or(&0.0)
        } else {
            let sum: f64 = children
                .iter()
                .map(|c| seeds.get(&c.from).unwrap_or(&0.0))
                .sum();
            sum / (children.len() as f64)
        };

        seeds.insert(node_id.clone(), state);

        f(node_id.clone(), state);
    });
}

// pub async fn compute<C>(
//     root_node_id: NodeIdString,
//     schema: Arc<RwLock<DomainSchema>>,
//     seeds: Arc<RwLock<HashMap<NodeId<String>, f64>>>,
//     f: C,
// ) where
//     C: Fn(NodeIdString, f64),
// {
//     let schema = schema.read().await;
//     let mut seeds = seeds.write().await;

//     schema.compute(root_node_id, |node_id, children| {
//         let state = if children.is_empty() {
//             *seeds.get(node_id).unwrap_or(&0.0)
//         } else {
//             let sum: f64 = children
//                 .iter()
//                 .map(|c| seeds.get(&c.from).unwrap_or(&0.0))
//                 .sum();
//             sum / (children.len() as f64)
//         };

//         seeds.insert(node_id.clone(), state);

//         f(node_id.clone(), state);
//     });
// }
