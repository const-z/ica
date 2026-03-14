use ica_core::{Attributes, NodeId, Schema};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

pub async fn compute<C>(
    schema: Arc<RwLock<Schema<Attributes, Attributes, Attributes, String>>>,
    mut f: C,
) where
    C: FnMut(NodeId<String>, f64),
{
    let schema = schema.read().await;
    let mut seeds: HashMap<NodeId<String>, f64> = HashMap::new();

    schema.compute(|node, children| {
        let state = if let Some(node_type) = node.attrs.get_text("type")
            && node_type == "INCIDENT"
        {
            node.attrs.get_float("severity").unwrap_or(0.0)
        } else if children.is_empty() {
            *seeds.get(&node.id).unwrap_or(&0.0)
        } else {
            let state: f64 = children
                .iter()
                .map(|c| {
                    let weight = c.attrs.get_float("weight").unwrap_or(1.0);

                    1.0 - seeds.get(&c.from).unwrap_or(&0.0) * weight
                })
                .reduce(|acc, i| acc * i)
                .unwrap_or(0.0);

            1.0 - state
        };

        seeds.insert(node.id.clone(), state);

        f(node.id.clone(), state);
    });
}
