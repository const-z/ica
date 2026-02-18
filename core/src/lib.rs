pub mod schema;

// Re-export the most important types at the crate root for convenience.
pub use schema::{
    AttributeKey, AttributeValue, Attributes, Edge, EdgeId, HasId, Node, NodeId, Schema,
};

#[cfg(test)]
mod core_tests {
    use crate::schema::ChildInfluence;

    use super::*;
    use std::collections::HashMap;

    #[test]
    fn attributes_insert_and_get_work() {
        let mut attrs = Attributes::new();
        assert!(attrs.is_empty());

        attrs.insert("name", AttributeValue::Text("system-a".to_string()));
        attrs.insert("critical", AttributeValue::Boolean(true));
        attrs.insert("rto_hours", AttributeValue::Integer(4));

        assert_eq!(attrs.len(), 3);
        assert!(!attrs.is_empty());

        let name = attrs.get("name");
        assert!(matches!(name, Some(AttributeValue::Text(v)) if v == "system-a"));

        let critical = attrs.get("critical");
        assert!(matches!(critical, Some(AttributeValue::Boolean(true))));
    }

    #[test]
    fn node_and_edge_have_correct_ids() {
        let node_attrs = Attributes::new();
        let node = Node {
            id: NodeId(1),
            attrs: node_attrs,
        };
        assert_eq!(node.id(), NodeId(1));

        let edge_attrs = Attributes::new();
        let edge = Edge {
            id: EdgeId(10),
            from: NodeId(1),
            to: NodeId(2),
            attrs: edge_attrs,
        };
        assert_eq!(edge.id(), EdgeId(10));
        assert_eq!(edge.from, NodeId(1));
        assert_eq!(edge.to, NodeId(2));
    }

    #[test]
    fn adds_nodes_and_edges_and_counts_them() {
        let mut g: Schema<Attributes, Attributes, u64> = Schema::new();
        assert!(g.is_empty());
        assert_eq!(g.node_count(), 0);
        assert_eq!(g.edge_count(), 0);
        let mut counter: u64 = 0;

        let n1 = {
            counter += 1;
            let id = NodeId(counter);
            g.insert_node(id, Attributes::new());
            id
        };
        let n2 = {
            counter += 1;
            let id = NodeId(counter);
            g.insert_node(id, Attributes::new());
            id
        };
        assert_eq!(g.node_count(), 2);
        assert!(!g.is_empty());
        assert!(g.node(n1).is_some());
        assert!(g.node(n2).is_some());

        let e1 = {
            counter += 1;
            let id = EdgeId(counter);
            g.insert_edge(id, n1, n2, Attributes::new());
            id
        };
        assert_eq!(g.edge_count(), 1);
        let edge = g.edge(e1).expect("edge must exist");
        assert_eq!(edge.from, n1);
        assert_eq!(edge.to, n2);
    }

    #[test]
    fn outgoing_and_incoming_edges_iterate_correctly() {
        let mut g: Schema<Attributes, Attributes, u64> = Schema::new();
        let mut counter = 0;

        let a = {
            counter += 1;
            let id = NodeId(counter);
            g.insert_node(id, Attributes::new());
            id
        };
        let b = {
            counter += 1;
            let id = NodeId(counter);
            g.insert_node(id, Attributes::new());
            id
        };
        let c = {
            counter += 1;
            let id = NodeId(counter);
            g.insert_node(id, Attributes::new());
            id
        };

        let _ab = {
            counter += 1;
            let id = EdgeId(counter);
            g.insert_edge(id, a, b, Attributes::new());
            id
        };
        let _ac = {
            counter += 1;
            let id = EdgeId(counter);
            g.insert_edge(id, a, c, Attributes::new());
            id
        };
        let _ba = {
            counter += 1;
            let id = EdgeId(counter);
            g.insert_edge(id, b, a, Attributes::new());
            id
        };

        let outgoing_from_a: Vec<_> = g.outgoing_edges(a).collect();
        assert_eq!(outgoing_from_a.len(), 2);
        assert!(outgoing_from_a.iter().all(|e| e.from == a));

        let incoming_to_a: Vec<_> = g.incoming_edges(a).collect();
        assert_eq!(incoming_to_a.len(), 1);
        assert!(incoming_to_a.iter().all(|e| e.to == a));
    }

    #[test]
    fn removing_node_removes_incident_edges() {
        let mut g: Schema<Attributes, Attributes, u64> = Schema::new();
        let mut counter = 0;

        let a = {
            counter += 1;
            let id = NodeId(counter);
            g.insert_node(id, Attributes::new());
            id
        };
        let b = {
            counter += 1;
            let id = NodeId(counter);
            g.insert_node(id, Attributes::new());
            id
        };
        let c = {
            counter += 1;
            let id = NodeId(counter);
            g.insert_node(id, Attributes::new());
            id
        };

        let _ab = {
            counter += 1;
            let id = EdgeId(counter);
            g.insert_edge(id, a, b, Attributes::new());
            id
        };
        let _bc = {
            counter += 1;
            let id = EdgeId(counter);
            g.insert_edge(id, b, c, Attributes::new());
            id
        };
        let _ca = {
            counter += 1;
            let id = EdgeId(counter);
            g.insert_edge(id, c, a, Attributes::new());
            id
        };

        assert_eq!(g.edge_count(), 3);

        let removed = g.remove_node(b);
        assert!(removed.is_some());

        assert_eq!(g.edge_count(), 1);
        let remaining: Vec<_> = g.edges().collect();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].from, c);
        assert_eq!(remaining[0].to, a);
    }

    #[test]
    fn compute_influence_propagates_from_children_to_parents() {
        let mut g: Schema<Attributes, Attributes, u64> = Schema::new();
        let mut counter = 0;

        let root = {
            counter += 1;
            let id = NodeId(counter);
            g.insert_node(id, Attributes::new());
            id
        };
        let mid = {
            counter += 1;
            let id = NodeId(counter);
            g.insert_node(id, Attributes::new());
            id
        };
        let leaf2 = {
            counter += 1;
            let id = NodeId(counter);
            g.insert_node(id, Attributes::new());
            id
        };
        let leaf1 = {
            counter += 1;
            let id = NodeId(counter);
            g.insert_node(id, Attributes::new());
            id
        };

        {
            counter += 1;
            let id = EdgeId(counter);
            g.insert_edge(id, leaf1, mid, Attributes::new());
        }
        {
            counter += 1;
            let id = EdgeId(counter);
            g.insert_edge(id, mid, root, Attributes::new());
        }
        {
            counter += 1;
            let id = EdgeId(counter);
            g.insert_edge(id, leaf2, mid, Attributes::new());
        }

        let mut known: HashMap<NodeId<u64>, f64> = HashMap::new();
        known.insert(leaf1, 1.0);
        known.insert(leaf2, 0.5);

        let result = g.compute_influence(root, &known, |_parent, children| {
            if children.is_empty() {
                0.0
            } else {
                let sum: f64 = children.iter().map(|c| c.influence).sum();
                sum / (children.len() as f64)
            }
        });

        assert_eq!(result[&leaf1], 1.0);
        assert_eq!(result[&leaf2], 0.5);
        assert_eq!(result[&mid], 0.75);
        assert_eq!(result[&root], 0.75);
    }

    #[test]
    fn compute_with_string_ids() {
        let mut g = Schema::<Attributes, Attributes, &str>::new();
        g.insert_node(NodeId("root"), Attributes::new());
        g.insert_node(NodeId("mid"), Attributes::new());
        g.insert_node(NodeId("l1"), Attributes::new());
        g.insert_node(NodeId("l2"), Attributes::new());
        g.insert_edge(
            EdgeId("mid->root"),
            NodeId("mid"),
            NodeId("root"),
            Attributes::new(),
        );
        g.insert_edge(
            EdgeId("l1->mid"),
            NodeId("l1"),
            NodeId("mid"),
            Attributes::new(),
        );
        g.insert_edge(
            EdgeId("l2->mid"),
            NodeId("l2"),
            NodeId("mid"),
            Attributes::new(),
        );

        assert_eq!(g.node_count(), 4);
        assert_eq!(g.edge_count(), 3);

        let root = g.node(NodeId("root")).unwrap();
        assert_eq!(root.id(), NodeId("root"));
        let l1 = g.node(NodeId("l1")).unwrap();
        assert_eq!(l1.id(), NodeId("l1"));

        let mut known: HashMap<NodeId<&str>, f64> = HashMap::new();

        fn compute_fn(
            _parent: &Node<Attributes, &str>,
            children: &[ChildInfluence<'_, Attributes, Attributes, &str>],
        ) -> f64 {
            if children.is_empty() {
                0.0
            } else {
                let sum: f64 = children.iter().map(|c| c.influence).sum();
                sum / (children.len() as f64)
            }
        }

        let result = g.compute_influence(NodeId("root"), &known, compute_fn);

        assert_eq!(result[&NodeId("l1")], 0.0);
        assert_eq!(result[&NodeId("l2")], 0.0);
        assert_eq!(result[&NodeId("mid")], 0.0);
        assert_eq!(result[&NodeId("root")], 0.0);

        known.insert(NodeId("l1"), 1.0);
        known.insert(NodeId("l2"), 0.5);

        let result = g.compute_influence(NodeId("root"), &known, compute_fn);

        assert_eq!(result[&NodeId("l1")], 1.0);
        assert_eq!(result[&NodeId("l2")], 0.5);
        assert_eq!(result[&NodeId("mid")], 0.75);
        assert_eq!(result[&NodeId("root")], 0.75);
    }

    #[test]
    fn compute_with_custom_attributes() {
        #[derive(Debug, Clone, Default, PartialEq)]
        enum NodeType {
            #[default]
            Asset,
            Incident,
        }

        #[derive(Debug, Clone, Default)]
        struct NodeAttributes {
            external_id: String,
            node_type: NodeType,
        }

        #[derive(Debug, Clone, Default)]
        struct EdgeAttributes {
            external_id: String,
            weight: f64,
        }

        let mut g = Schema::<NodeAttributes, EdgeAttributes, u64>::new();

        g.insert_node(
            NodeId(1),
            NodeAttributes {
                external_id: "root".to_string(),
                node_type: NodeType::Asset,
            },
        );

        g.insert_node(
            NodeId(2),
            NodeAttributes {
                external_id: "mid".to_string(),
                node_type: NodeType::Incident,
            },
        );

        g.insert_node(
            NodeId(3),
            NodeAttributes {
                external_id: "l1".to_string(),
                node_type: NodeType::Incident,
            },
        );

        g.insert_node(
            NodeId(4),
            NodeAttributes {
                external_id: "l2".to_string(),
                node_type: NodeType::Incident,
            },
        );

        g.insert_edge(
            EdgeId(1),
            NodeId(2),
            NodeId(1),
            EdgeAttributes {
                external_id: "mid->root".to_string(),
                weight: 0.5,
            },
        );

        g.insert_edge(
            EdgeId(2),
            NodeId(3),
            NodeId(2),
            EdgeAttributes {
                external_id: "l1->mid".to_string(),
                weight: 0.5,
            },
        );

        g.insert_edge(
            EdgeId(3),
            NodeId(4),
            NodeId(2),
            EdgeAttributes {
                external_id: "l2->mid".to_string(),
                weight: 0.75,
            },
        );

        assert_eq!(g.node(NodeId(4)).unwrap().attrs.external_id, "l2");
        assert_eq!(
            g.node(NodeId(4)).unwrap().attrs.node_type,
            NodeType::Incident
        );
        assert_eq!(g.edge(EdgeId(3)).unwrap().attrs.external_id, "l2->mid");

        fn compute_fn(
            _parent: &Node<NodeAttributes, u64>,
            children: &[ChildInfluence<'_, NodeAttributes, EdgeAttributes, u64>],
        ) -> f64 {
            if children.is_empty() {
                0.0
            } else {
                let sum: f64 = children
                    .iter()
                    .map(|c| c.influence * c.edge.attrs.weight)
                    .sum();
                sum / (children.len() as f64)
            }
        }

        let mut known: HashMap<NodeId<u64>, f64> = HashMap::new();

        let result = g.compute_influence(NodeId(1), &known, compute_fn);

        assert_eq!(result[&NodeId(4)], 0.0);
        assert_eq!(result[&NodeId(3)], 0.0);
        assert_eq!(result[&NodeId(2)], 0.0);
        assert_eq!(result[&NodeId(1)], 0.0);

        known.insert(NodeId(3), 1.0);
        known.insert(NodeId(4), 0.5);

        let result = g.compute_influence(NodeId(1), &known, compute_fn);

        assert_eq!(result[&NodeId(4)], 0.5);
        assert_eq!(result[&NodeId(3)], 1.0);
        assert_eq!(result[&NodeId(2)], 0.4375);
        assert_eq!(result[&NodeId(1)], 0.21875);
    }

    #[test]
    fn compute_yet_another_schema() {
        #[derive(Debug, Clone, Default)]
        struct EdgeAttributes {
            weight: f64,
        }

        let mut g = Schema::<Attributes, EdgeAttributes, &str>::new();

        g.insert_node(NodeId("root"), Attributes::new());
        g.insert_node(NodeId("mid1"), Attributes::new());
        g.insert_node(NodeId("mid2"), Attributes::new());
        g.insert_node(NodeId("l1"), Attributes::new());

        g.insert_edge(
            EdgeId("mid1->root"),
            NodeId("mid1"),
            NodeId("root"),
            EdgeAttributes { weight: 1.0 },
        );

        g.insert_edge(
            EdgeId("mid2->root"),
            NodeId("mid2"),
            NodeId("root"),
            EdgeAttributes { weight: 1.0 },
        );

        g.insert_edge(
            EdgeId("l1->mid1"),
            NodeId("l1"),
            NodeId("mid1"),
            EdgeAttributes { weight: 1.0 },
        );

        g.insert_edge(
            EdgeId("l1->mid2"),
            NodeId("l1"),
            NodeId("mid2"),
            EdgeAttributes { weight: 0.5 },
        );

        fn compute_fn(
            _parent: &Node<Attributes, &str>,
            children: &[ChildInfluence<'_, Attributes, EdgeAttributes, &str>],
        ) -> f64 {
            if children.is_empty() {
                0.0
            } else {
                let sum: f64 = children
                    .iter()
                    .map(|c| c.influence * c.edge.attrs.weight)
                    .sum();
                sum / (children.len() as f64)
            }
        }

        let mut known: HashMap<NodeId<&str>, f64> = HashMap::new();

        let result = g.compute_influence(NodeId("root"), &known, compute_fn);

        assert_eq!(result[&NodeId("l1")], 0.0);
        assert_eq!(result[&NodeId("mid2")], 0.0);
        assert_eq!(result[&NodeId("mid1")], 0.0);
        assert_eq!(result[&NodeId("root")], 0.0);

        known.insert(NodeId("l1"), 1.0);

        let result = g.compute_influence(NodeId("root"), &known, compute_fn);

        assert_eq!(result[&NodeId("l1")], 1.0);
        assert_eq!(result[&NodeId("mid2")], 0.5);
        assert_eq!(result[&NodeId("mid1")], 1.0);
        assert_eq!(result[&NodeId("root")], 0.75);
    }
}
