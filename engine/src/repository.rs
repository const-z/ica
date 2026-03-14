use std::{
    fmt::{Debug, Display},
    hash::Hash,
    sync::Arc,
};

use ica_core::{EdgeId, NodeId, Schema};
use tokio::sync::RwLock;

// Определение базовых типов для примера (в реальном проекте могут быть импортированы)
#[derive(Debug)]
pub enum RepositoryError {
    NotFound(String),
    Conflict(String),
    Internal(String),
}

impl Display for RepositoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RepositoryError::NotFound(msg) => write!(f, "RepositoryError: NotFound: {}", msg),
            RepositoryError::Conflict(msg) => write!(f, "RepositoryError: Conflict: {}", msg),
            RepositoryError::Internal(msg) => write!(f, "RepositoryError: Internal: {}", msg),
        }
    }
}

pub struct Incident<NA, T: Debug + Clone + Hash + Eq> {
    pub id: NodeId<T>,
    pub attrs: NA,
    pub severity: f64,
}

pub struct IncidentEdge<EA, T: Debug + Clone + Hash + Eq> {
    pub id: EdgeId<T>,
    pub to_id: NodeId<String>,
    pub attrs: EA,
}

/// Трейт для взаимодействия с абстрактным хранилищем данных
pub trait SchemaRepository<SA, NA, EA, T>
where
    T: Clone + Default + Hash + Eq + Debug,
{
    /// Получить список схем
    async fn list_schemas(&self) -> Vec<(T, SA)>;

    /// Добавить схему
    async fn add_schema(
        &self,
        schema_id: impl Into<String>,
        attrs: SA,
    ) -> Result<(), RepositoryError>;

    /// Удалить схему
    async fn remove_schema(&self, schema_id: impl Into<String>) -> Result<(), RepositoryError>;

    /// Получить схему
    async fn get_schema(
        &self,
        schema_id: T,
    ) -> Result<Arc<RwLock<Schema<SA, NA, EA, T>>>, RepositoryError>;

    // Добавить узел на схему
    async fn add_node(
        &self,
        schema_id: T,
        node_id: NodeId<T>,
        attrs: NA,
    ) -> Result<(), RepositoryError>;

    /// Добавить узел-инцидент и связать его с другим узлом на схеме
    async fn add_incident(
        &self,
        schema_id: T,
        incident: Incident<NA, T>,
        edge: IncidentEdge<EA, T>,
    ) -> Result<(), RepositoryError>;

    /// Удалить узел
    async fn remove_node(&self, schema_id: T, id: NodeId<T>) -> Result<(), RepositoryError>;

    /// Добавить связь
    async fn add_edge(
        &self,
        schema_id: T,
        edge_id: EdgeId<T>,
        from: NodeId<T>,
        to: NodeId<T>,
        arrts: EA,
    ) -> Result<(), RepositoryError>;

    /// Удалить связь
    async fn remove_edge(&self, schema_id: T, edge_id: EdgeId<T>) -> Result<(), RepositoryError>;

    async fn compute<C>(&self, schema_id: T, f: C) -> Result<(), RepositoryError>
    where
        C: Fn(NodeId<T>, f64);
}
