use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    hash::Hash,
};

pub mod attributes;
pub mod edge;
pub mod node;

pub use attributes::{AttributeKey, AttributeValue, Attributes};
pub use edge::Edge;
pub use node::Node;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId<T: Copy + Debug + Hash + Eq>(pub T);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EdgeId<T: Copy + Debug + Hash>(pub T);

pub trait HasId {
    type Id: Copy;

    fn id(&self) -> Self::Id;
}

impl<A, T: Copy + Debug + Hash + Eq> HasId for Node<A, T> {
    type Id = NodeId<T>;

    fn id(&self) -> Self::Id {
        self.id
    }
}

impl<A, T: Copy + Debug + Hash + Eq> HasId for Edge<A, T> {
    type Id = EdgeId<T>;

    fn id(&self) -> Self::Id {
        self.id
    }
}

#[derive(Debug, Default)]
pub struct Schema<NA, EA, T: Copy + Debug + Hash + Eq> {
    nodes: HashMap<NodeId<T>, Node<NA, T>>,
    edges: HashMap<EdgeId<T>, Edge<EA, T>>,
}

#[derive(Debug)]
pub struct ChildInfluence<'a, NA, EA, T: Copy + Debug + Hash + Eq> {
    pub child: &'a Node<NA, T>,
    pub edge: &'a Edge<EA, T>,
    pub influence: f64,
}

impl<NA: Default, EA: Default, T: Copy + Default + Debug + Hash + Eq> Schema<NA, EA, T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    pub fn insert_node(&mut self, id: NodeId<T>, attrs: NA) {
        if self.nodes.contains_key(&id) {
            panic!("node with id {:?} already exists", id);
        }

        let node = Node { id, attrs };
        self.nodes.insert(id, node);
    }

    pub fn remove_node(&mut self, id: NodeId<T>) -> Option<Node<NA, T>> {
        self.edges
            .retain(|_, edge| edge.from != id && edge.to != id);
        self.nodes.remove(&id)
    }

    pub fn node(&self, id: NodeId<T>) -> Option<&Node<NA, T>> {
        self.nodes.get(&id)
    }

    pub fn node_mut(&mut self, id: NodeId<T>) -> Option<&mut Node<NA, T>> {
        self.nodes.get_mut(&id)
    }

    pub fn insert_edge(&mut self, id: EdgeId<T>, from: NodeId<T>, to: NodeId<T>, attrs: EA) {
        assert!(
            self.nodes.contains_key(&from),
            "attempt to add edge from unknown node"
        );
        assert!(
            self.nodes.contains_key(&to),
            "attempt to add edge to unknown node"
        );

        if self.edges.contains_key(&id) {
            panic!("edge with id {:?} already exists", id);
        }

        let edge = Edge {
            id,
            from,
            to,
            attrs,
        };
        self.edges.insert(id, edge);
    }

    pub fn remove_edge(&mut self, id: EdgeId<T>) -> Option<Edge<EA, T>> {
        self.edges.remove(&id)
    }

    pub fn edge(&self, id: EdgeId<T>) -> Option<&Edge<EA, T>> {
        self.edges.get(&id)
    }

    pub fn edge_mut(&mut self, id: EdgeId<T>) -> Option<&mut Edge<EA, T>> {
        self.edges.get_mut(&id)
    }

    pub fn nodes(&self) -> impl Iterator<Item = &Node<NA, T>> {
        self.nodes.values()
    }

    pub fn edges(&self) -> impl Iterator<Item = &Edge<EA, T>> {
        self.edges.values()
    }

    pub fn outgoing_edges(&self, from: NodeId<T>) -> impl Iterator<Item = &Edge<EA, T>> {
        self.edges.values().filter(move |e| e.from == from)
    }

    pub fn incoming_edges(&self, to: NodeId<T>) -> impl Iterator<Item = &Edge<EA, T>> {
        self.edges.values().filter(move |e| e.to == to)
    }

    /// Обход путей для вычисления значений соответствует путям от корня
    /// до нижних уровней, но по рёбрам идём в обратную сторону — от
    /// родителя к детям.
    ///
    /// - Если узел уже имеет значение в `known`, то оно используется.
    /// - Иначе, вычисляется значение на основе дочерних узлов используя функцию combine.
    /// - Результаты кэшируются в `result` для повторного использования.
    ///
    /// # Parameters
    ///
    /// - `root`: идентификатор корневого узла, в который в итоге сходятся
    ///   все пути графа.
    /// - `known`: map of pre-computed influence values for some nodes
    ///   (for example, leaves where influence is defined directly).
    /// - `combine`: пользовательская функция, которая:
    ///   - принимает ссылку на узел, состояние которого нужно вычислить, и срез `ChildInfluence` с данными для вычисления.
    ///   - возвращает расчитанное значение состояния (обычно в диапазоне \[0.0, 1.0\]).
    ///
    /// # Returns
    ///
    /// - состояние узлов на схеме.
    ///
    /// # Panics
    ///
    /// - Если найдена циклическая связь.
    /// - Не найден узел.
    /// - Не найден дочерний узел.
    pub fn compute_influence<F>(
        &self,
        root: NodeId<T>,
        known: &HashMap<NodeId<T>, f64>,
        combine: F,
    ) -> HashMap<NodeId<T>, f64>
    where
        F: for<'a> Fn(&'a Node<NA, T>, &'a [ChildInfluence<'a, NA, EA, T>]) -> f64,
    {
        fn dfs<NA: Default, EA: Default, T: Copy + Default + Debug + Hash + Eq, F>(
            schema: &Schema<NA, EA, T>,
            node_id: NodeId<T>,
            known: &HashMap<NodeId<T>, f64>,
            result: &mut HashMap<NodeId<T>, f64>,
            visiting: &mut HashSet<NodeId<T>>,
            combine: &F,
        ) where
            F: for<'a> Fn(&'a Node<NA, T>, &'a [ChildInfluence<'a, NA, EA, T>]) -> f64,
        {
            if result.contains_key(&node_id) {
                return;
            }

            if let Some(&value) = known.get(&node_id) {
                result.insert(node_id, value);
                return;
            }

            if !visiting.insert(node_id) {
                panic!(
                    "schema contains a directed cycle involving node {:?}",
                    node_id
                );
            }

            let mut children_values: Vec<ChildInfluence<'_, NA, EA, T>> = Vec::new();

            for edge in schema.incoming_edges(node_id) {
                let child_id = edge.from;
                if !result.contains_key(&child_id) {
                    dfs(schema, child_id, known, result, visiting, combine);
                }
                if let Some(&child_value) = result.get(&child_id) {
                    let child_node = schema
                        .node(child_id)
                        .expect("schema is internally inconsistent: missing child node");
                    children_values.push(ChildInfluence {
                        child: child_node,
                        edge,
                        influence: child_value,
                    });
                }
            }

            let node = schema
                .node(node_id)
                .expect("schema is internally inconsistent: missing node");
            let value = combine(node, &children_values);
            result.insert(node_id, value);
            visiting.remove(&node_id);
        }

        let mut result: HashMap<NodeId<T>, f64> = HashMap::new();
        let mut visiting: HashSet<NodeId<T>> = HashSet::new();

        dfs(self, root, known, &mut result, &mut visiting, &combine);

        result
    }
}
