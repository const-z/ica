pub mod attributes;
pub mod edge;
pub mod node;

use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    hash::Hash,
};

pub use attributes::{AttributeKey, AttributeValue, Attributes};
pub use edge::Edge;
pub use node::Node;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId<T: Debug + Hash + Eq>(pub T);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EdgeId<T: Debug + Hash>(pub T);

pub trait HasId {
    type Id: Copy;

    fn id(&self) -> Self::Id;
}

impl<A, T> HasId for Node<A, T>
where
    T: Copy + Debug + Hash + Eq,
{
    type Id = NodeId<T>;

    fn id(&self) -> Self::Id {
        self.id
    }
}

impl<A, T> HasId for Edge<A, T>
where
    T: Copy + Debug + Hash + Eq,
{
    type Id = EdgeId<T>;

    fn id(&self) -> Self::Id {
        self.id
    }
}

#[derive(Debug, Default, Clone)]
pub struct Schema<NA, EA, T>
where
    T: Clone + Default + Debug + Hash + Eq,
{
    nodes: HashMap<NodeId<T>, Node<NA, T>>,
    edges: HashMap<EdgeId<T>, Edge<EA, T>>,
    edges_from: HashMap<NodeId<T>, Vec<EdgeId<T>>>,
    edges_to: HashMap<NodeId<T>, Vec<EdgeId<T>>>,
}

#[derive(Debug)]
pub struct ChildImpact<'a, NA, EA, T: Debug + Hash + Eq> {
    pub child: &'a Node<NA, T>,
    pub edge: &'a Edge<EA, T>,
    pub impact: f64,
}

#[derive(Debug)]
pub enum SchemaError {
    NodeExists(String),
    NodeNotFound(String),
    EdgeExists(String),
    EdgeNotFound(String),
    CycleDetected(String),
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaError::NodeExists(msg) => write!(f, "SchemaError: NodeExists: {}", msg),
            SchemaError::NodeNotFound(msg) => write!(f, "SchemaError: NodeNotFound: {}", msg),
            SchemaError::EdgeExists(msg) => write!(f, "SchemaError: EdgeExists: {}", msg),
            SchemaError::EdgeNotFound(msg) => write!(f, "SchemaError: EdgeNotFound: {}", msg),
            SchemaError::CycleDetected(msg) => write!(f, "SchemaError: CycleDetected: {}", msg),
        }
    }
}

impl<NA, EA, T> Schema<NA, EA, T>
where
    NA: Default,
    EA: Default,
    T: Clone + Default + Debug + Hash + Eq,
{
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

    pub fn insert_node(&mut self, id: NodeId<T>, attrs: NA) -> Result<(), SchemaError> {
        if self.nodes.contains_key(&id) {
            return Err(SchemaError::NodeExists(format!(
                "node with id {:?} already exists",
                id
            )));
        }

        self.nodes.insert(
            id.clone(),
            Node {
                id: id.clone(),
                attrs,
            },
        );

        self.edges_from.insert(id.clone(), Vec::new());
        self.edges_to.insert(id, Vec::new());

        Ok(())
    }

    pub fn remove_node(&mut self, id: &NodeId<T>) -> Result<(), SchemaError> {
        if !self.nodes.contains_key(id) {
            return Err(SchemaError::NodeNotFound(format!(
                "Node id {:?} not found",
                &id.0
            )));
        }

        self.nodes.remove(id);
        let edges_from = self.edges_from.remove(id);
        let edges_to = self.edges_to.remove(id);

        if let Some(edges_from) = edges_from {
            for edge in edges_from {
                let _ = self.remove_edge(&edge);
            }
        }
        if let Some(edges_to) = edges_to {
            for edge in edges_to {
                let _ = self.remove_edge(&edge);
            }
        }

        Ok(())
    }

    pub fn node(&self, id: &NodeId<T>) -> Result<&Node<NA, T>, SchemaError> {
        match self.nodes.get(id) {
            Some(node) => Ok(node),
            None => Err(SchemaError::NodeNotFound(format!(
                "Node id {:?} not found",
                id
            ))),
        }
    }

    pub fn insert_edge(
        &mut self,
        id: EdgeId<T>,
        from: NodeId<T>,
        to: NodeId<T>,
        attrs: EA,
    ) -> Result<(), SchemaError> {
        if !self.nodes.contains_key(&from) {
            return Err(SchemaError::NodeNotFound(format!(
                "attempt to add edge from unknown node id {:?}",
                from.0
            )));
        }

        if !self.nodes.contains_key(&to) {
            return Err(SchemaError::NodeNotFound(format!(
                "attempt to add edge to unknown node id {:?}",
                to.0
            )));
        }

        if self.edges.contains_key(&id) {
            return Err(SchemaError::EdgeExists(format!(
                "edge with id {:?} already exists",
                id
            )));
        }

        if let Some(edges_to) = &self.edges_from.get(&from)
            && edges_to.iter().any(|t| self.edge(t).unwrap().to == to)
        {
            return Err(SchemaError::EdgeExists(format!(
                "edge {:?} -> {:?} already exists",
                &from.0, &to.0
            )));
        }

        self.edges.insert(
            id.clone(),
            Edge {
                id: id.clone(),
                from: from.clone(),
                to: to.clone(),
                attrs,
            },
        );

        if let Some(value) = self.edges_from.get_mut(&from) {
            value.push(id.clone());
        }
        if let Some(value) = self.edges_to.get_mut(&to) {
            value.push(id);
        }

        Ok(())
    }

    pub fn remove_edge(&mut self, id: &EdgeId<T>) -> Result<Edge<EA, T>, SchemaError> {
        match self.edges.remove(id) {
            Some(edge) => {
                if let Some(nn) = self.edges_from.get_mut(&edge.from)
                    && let Some(i) = nn.iter().position(|e| e == id)
                {
                    nn.remove(i);
                }
                if let Some(nn) = self.edges_to.get_mut(&edge.to)
                    && let Some(i) = nn.iter().position(|e| e == id)
                {
                    nn.remove(i);
                }

                Ok(edge)
            }
            None => Err(SchemaError::EdgeNotFound(format!(
                "Edge id {:?} not found",
                id.0
            ))),
        }
    }

    pub fn edge(&self, id: &EdgeId<T>) -> Result<&Edge<EA, T>, SchemaError> {
        match self.edges.get(id) {
            Some(edge) => Ok(edge),
            None => Err(SchemaError::EdgeNotFound(format!(
                "Edge id {:?} not found",
                id
            ))),
        }
    }

    pub fn nodes(&self) -> impl Iterator<Item = &Node<NA, T>> {
        self.nodes.values()
    }

    pub fn edges(&self) -> impl Iterator<Item = &Edge<EA, T>> {
        self.edges.values()
    }

    pub fn outgoing_edges(&self, from: &NodeId<T>) -> impl Iterator<Item = &Edge<EA, T>> {
        self.edges_from
            .get(from)
            .into_iter()
            .flatten()
            .map(|edge_id| self.edges.get(edge_id).unwrap())
    }

    pub fn incoming_edges(&self, to: &NodeId<T>) -> impl Iterator<Item = &Edge<EA, T>> {
        self.edges_to
            .get(to)
            .into_iter()
            .flatten()
            .map(|edge_id| self.edges.get(edge_id).unwrap())
    }

    pub fn get_path_to_root(&self, root_id: NodeId<T>) -> Result<Vec<NodeId<T>>, SchemaError> {
        let mut result = Vec::new();
        let mut stack = Vec::new();
        let mut visited = HashSet::new();

        // Словарь для отслеживания количества необработанных входящих ребер для каждого узла
        let mut remaining_incoming = HashMap::new();

        // Инициализируем remaining_incoming для всех узлов
        for node in self.nodes() {
            let incoming_count = self.incoming_edges(&node.id).count();
            remaining_incoming.insert(node.id.clone(), incoming_count);
        }

        // Начинаем с корня
        stack.push(root_id.clone());

        while let Some(node_id) = stack.pop() {
            if visited.contains(&node_id) {
                continue;
            }

            // Проверяем, все ли входящие ребра (родители) обработаны
            let incoming_edges: Vec<_> = self.incoming_edges(&node_id).collect();
            let mut all_parents_processed = true;

            for edge in &incoming_edges {
                let parent_id = &edge.from;
                if !visited.contains(parent_id) {
                    all_parents_processed = false;
                    // Добавляем необработанного родителя в стек
                    if !stack.contains(parent_id) {
                        stack.push(parent_id.clone());
                    }
                }
            }

            if all_parents_processed {
                // Все родители обработаны, можно добавить текущий узел
                visited.insert(node_id.clone());
                result.push(node_id.clone());

                // После добавления узла, проверяем его детей
                for edge in self.outgoing_edges(&node_id) {
                    let child_id = &edge.to;
                    if !visited.contains(child_id) && !stack.contains(child_id) {
                        stack.push(child_id.clone());
                    }
                }
            } else {
                // Возвращаем узел в стек для повторной проверки позже
                // Но чтобы избежать бесконечного цикла, добавляем его в конец
                if !stack.contains(&node_id) {
                    stack.insert(0, node_id);
                }
            }
        }

        // Проверяем, что все узлы были посещены
        if visited.len() != self.node_count() {
            // Некоторые узлы не достижимы от корня или есть цикл
            // Добавляем оставшиеся узлы в порядке, который сохраняет зависимости
            let mut remaining_nodes: Vec<_> = self
                .nodes()
                .map(|n| n.id.clone())
                .filter(|id| !visited.contains(id))
                .collect();

            // Сортируем оставшиеся узлы так, чтобы родители были перед детьми
            remaining_nodes.sort_by(|a, b| {
                let a_has_edge_to_b = self.outgoing_edges(a).any(|e| e.to == *b);
                let b_has_edge_to_a = self.outgoing_edges(b).any(|e| e.to == *a);

                if a_has_edge_to_b {
                    std::cmp::Ordering::Less
                } else if b_has_edge_to_a {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Equal
                }
            });

            result.extend(remaining_nodes);
        }

        Ok(result)
    }

    pub fn get_full_path(&self) -> Result<Vec<NodeId<T>>, SchemaError> {
        let mut result = Vec::new();

        // Создаем копию графа для подсчета входящих ребер
        let mut in_degree = HashMap::new();

        // Инициализируем in_degree для всех узлов
        for node in self.nodes() {
            in_degree.insert(node.id.clone(), 0);
        }

        // Подсчитываем входящие ребра для каждого узла
        for edge in self.edges() {
            *in_degree.entry(edge.to.clone()).or_insert(0) += 1;
        }

        // Очередь для узлов без входящих ребер (листья)
        let mut queue = Vec::new();
        for (node_id, degree) in &in_degree {
            if *degree == 0 {
                queue.push(node_id.clone());
            }
        }

        // Обрабатываем узлы
        while let Some(node_id) = queue.pop() {
            result.push(node_id.clone());

            // Уменьшаем счетчик входящих ребер для всех детей этого узла
            for edge in self.outgoing_edges(&node_id) {
                let child_id = &edge.to;
                if let Some(degree) = in_degree.get_mut(child_id) {
                    *degree -= 1;
                    if *degree == 0 && !result.contains(child_id) && !queue.contains(child_id) {
                        queue.push(child_id.clone());
                    }
                }
            }
        }

        // Проверяем, что все узлы обработаны
        if result.len() != self.node_count() {
            return Err(SchemaError::CycleDetected(
                "Graph contains a cycle".to_string(),
            ));
        }

        // Для получения порядка от листьев к корню, оставляем как есть
        // Так как алгоритм Кана естественным образом дает топологический порядок
        Ok(result)
    }

    /// combine - пользовательская функция, которая:
    /// - принимает ссылку на узел, состояние которого нужно вычислить, и входящие в него ребра.
    /// - возвращает расчитанное значение состояния (обычно в диапазоне \[0.0, 1.0\]).
    /// - возвращает признак необходимости дальнейшего вычисления.
    ///
    /// хранение состояния расчитанных значений, возлагается на замыкание этой функции
    /// схема ничего не знает о расчитанных состояних
    pub fn compute_with_root<F>(&self, root: NodeId<T>, mut combine: F)
    where
        F: for<'a> FnMut(&'a NodeId<T>, Vec<&'a Edge<EA, T>>),
    {
        let path = self.get_path_to_root(root).unwrap();

        for node_id in path.iter() {
            // у каждой ноды получить список нод влияющих на эту ноду
            let eges: Vec<&Edge<EA, T>> = self.incoming_edges(node_id).collect();
            // выполнить combine с этими данными
            combine(node_id, eges);
        }
    }

    pub fn compute<F>(&self, mut combine: F)
    where
        F: for<'a> FnMut(&'a NodeId<T>, Vec<&'a Edge<EA, T>>),
    {
        let path = self.get_full_path().unwrap();

        for node_id in path.iter() {
            // у каждой ноды получить список нод влияющих на эту ноду
            let eges: Vec<&Edge<EA, T>> = self.incoming_edges(node_id).collect();
            // выполнить combine с этими данными
            combine(node_id, eges);
        }
    }
}

#[cfg(test)]
mod tests_schema {
    use super::*;

    #[test]
    fn test_get_path_from_root_to_leaf() {
        let nodes = vec![
            "bea057ed-9517-4737-a61b-f0d22879273e",
            "ced3de21-afed-4375-8942-794033f10576",
            "cff6685b-8983-445b-abdf-0b31fb42437a",
            "b2216bd4-f863-46b6-92b9-5c220b342ab2",
            "aad80c80-1169-11f1-b4ac-0800200c9a66",
            "b118642c-b368-4e9f-bdc3-64e7c82ee684",
            "6a49d90b-aa88-410d-8a19-1b5a96903d92",
            "6a49d90b-aa88-410d-8a19-1b5a96903d93",
            "6a49d90b-aa88-410d-8a19-1b5a96903d94",
        ];

        let edges = vec![
            (
                NodeId("bea057ed-9517-4737-a61b-f0d22879273e".to_string()),
                NodeId("cff6685b-8983-445b-abdf-0b31fb42437a".to_string()),
            ),
            (
                NodeId("b2216bd4-f863-46b6-92b9-5c220b342ab2".to_string()),
                NodeId("cff6685b-8983-445b-abdf-0b31fb42437a".to_string()),
            ),
            (
                NodeId("ced3de21-afed-4375-8942-794033f10576".to_string()),
                NodeId("bea057ed-9517-4737-a61b-f0d22879273e".to_string()),
            ),
            (
                NodeId("ced3de21-afed-4375-8942-794033f10576".to_string()),
                NodeId("b2216bd4-f863-46b6-92b9-5c220b342ab2".to_string()),
            ),
            (
                NodeId("aad80c80-1169-11f1-b4ac-0800200c9a66".to_string()),
                NodeId("ced3de21-afed-4375-8942-794033f10576".to_string()),
            ),
            (
                NodeId("b118642c-b368-4e9f-bdc3-64e7c82ee684".to_string()),
                NodeId("ced3de21-afed-4375-8942-794033f10576".to_string()),
            ),
            (
                NodeId("6a49d90b-aa88-410d-8a19-1b5a96903d92".to_string()),
                NodeId("aad80c80-1169-11f1-b4ac-0800200c9a66".to_string()),
            ),
            (
                NodeId("6a49d90b-aa88-410d-8a19-1b5a96903d92".to_string()),
                NodeId("cff6685b-8983-445b-abdf-0b31fb42437a".to_string()),
            ),
            (
                NodeId("6a49d90b-aa88-410d-8a19-1b5a96903d93".to_string()),
                NodeId("6a49d90b-aa88-410d-8a19-1b5a96903d92".to_string()),
            ),
            (
                NodeId("6a49d90b-aa88-410d-8a19-1b5a96903d94".to_string()),
                NodeId("6a49d90b-aa88-410d-8a19-1b5a96903d92".to_string()),
            ),
            (
                NodeId("6a49d90b-aa88-410d-8a19-1b5a96903d94".to_string()),
                NodeId("aad80c80-1169-11f1-b4ac-0800200c9a66".to_string()),
            ),
        ];

        let mut schema = Schema::<Attributes, Attributes, String>::new();

        for node_id in nodes {
            let _ = schema.insert_node(NodeId(node_id.to_string()), Attributes::new());
        }

        for (idx, edge) in edges.into_iter().enumerate() {
            let _ = schema.insert_edge(
                EdgeId::<String>(idx.to_string()),
                edge.0,
                edge.1,
                Attributes::new(),
            );
        }

        let root = NodeId("cff6685b-8983-445b-abdf-0b31fb42437a".to_string());

        let path = schema.get_path_to_root(root).unwrap();

        assert_eq!(path.len(), 9);
        assert_eq!(
            path.last().unwrap().0,
            "cff6685b-8983-445b-abdf-0b31fb42437a".to_string(),
            "Root node must be first"
        );

        assert!(
            path.iter()
                .position(|i| "bea057ed-9517-4737-a61b-f0d22879273e" == i.0)
                .unwrap()
                < 8
        );
        assert!(
            path.iter()
                .position(|i| "b2216bd4-f863-46b6-92b9-5c220b342ab2" == i.0)
                .unwrap()
                < 8
        );
        assert!(
            path.iter()
                .position(|i| "ced3de21-afed-4375-8942-794033f10576" == i.0)
                .unwrap()
                < path
                    .iter()
                    .position(|i| "b2216bd4-f863-46b6-92b9-5c220b342ab2" == i.0)
                    .unwrap()
                && path
                    .iter()
                    .position(|i| "ced3de21-afed-4375-8942-794033f10576" == i.0)
                    .unwrap()
                    < path
                        .iter()
                        .position(|i| "bea057ed-9517-4737-a61b-f0d22879273e" == i.0)
                        .unwrap()
        );

        assert!(
            path.iter()
                .position(|i| "aad80c80-1169-11f1-b4ac-0800200c9a66" == i.0)
                .unwrap()
                < path
                    .iter()
                    .position(|i| "ced3de21-afed-4375-8942-794033f10576" == i.0)
                    .unwrap()
        );
        assert!(
            path.iter()
                .position(|i| "b118642c-b368-4e9f-bdc3-64e7c82ee684" == i.0)
                .unwrap()
                < path
                    .iter()
                    .position(|i| "ced3de21-afed-4375-8942-794033f10576" == i.0)
                    .unwrap()
        );

        assert!(
            path.iter()
                .position(|i| "6a49d90b-aa88-410d-8a19-1b5a96903d92" == i.0)
                .unwrap()
                < path
                    .iter()
                    .position(|i| "aad80c80-1169-11f1-b4ac-0800200c9a66" == i.0)
                    .unwrap()
        );
        assert!(
            path.iter()
                .position(|i| "6a49d90b-aa88-410d-8a19-1b5a96903d93" == i.0)
                .unwrap()
                < path
                    .iter()
                    .position(|i| "6a49d90b-aa88-410d-8a19-1b5a96903d92" == i.0)
                    .unwrap()
        );
        assert!(
            path.iter()
                .position(|i| "6a49d90b-aa88-410d-8a19-1b5a96903d94" == i.0)
                .unwrap()
                < path
                    .iter()
                    .position(|i| "6a49d90b-aa88-410d-8a19-1b5a96903d92" == i.0)
                    .unwrap()
        );
    }

    #[test]
    fn test_get_full_path() {
        let nodes = vec![
            "bea057ed-9517-4737-a61b-f0d22879273e",
            "ced3de21-afed-4375-8942-794033f10576",
            "cff6685b-8983-445b-abdf-0b31fb42437a",
        ];

        let edges = vec![
            (
                NodeId("bea057ed-9517-4737-a61b-f0d22879273e".to_string()),
                NodeId("cff6685b-8983-445b-abdf-0b31fb42437a".to_string()),
            ),
            (
                NodeId("ced3de21-afed-4375-8942-794033f10576".to_string()),
                NodeId("cff6685b-8983-445b-abdf-0b31fb42437a".to_string()),
            ),
        ];

        let mut schema = Schema::<Attributes, Attributes, String>::new();

        for node_id in nodes.clone() {
            let _ = schema.insert_node(NodeId(node_id.to_string()), Attributes::new());
        }

        for (idx, edge) in edges.into_iter().enumerate() {
            let _ = schema.insert_edge(
                EdgeId::<String>(idx.to_string()),
                edge.0,
                edge.1,
                Attributes::new(),
            );
        }

        let root = NodeId("cff6685b-8983-445b-abdf-0b31fb42437a".to_string());

        let path = schema.get_full_path().unwrap();

        assert_eq!(path.len(), 3);
        assert_eq!(path.last().unwrap(), &root, "Root node must be first");

        for id in nodes {
            assert!(
                path.contains(&NodeId(id.to_string())),
                "All nodes must be in the path"
            );
        }
    }
}
