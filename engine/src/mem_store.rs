use std::collections::HashMap;
use std::sync::Arc;

use ica_core::schema::SchemaError;
use ica_core::{AttributeValue, Attributes, EdgeId, NodeId, Schema};
use tokio::sync::RwLock;

use crate::compute_fn::compute;
use crate::repository::{Incident, IncidentEdge, RepositoryError, SchemaRepository};

pub type NodeIdString = NodeId<String>;
pub type EdgeIdString = EdgeId<String>;
pub type DomainSchema = Schema<Attributes, Attributes, Attributes, String>;
pub type StoreSchemas = Arc<RwLock<HashMap<String, Arc<RwLock<DomainSchema>>>>>;
pub type StoreSeeds = Arc<RwLock<HashMap<String, Arc<RwLock<HashMap<NodeIdString, f64>>>>>>;

pub struct MemorySchemaStore {
    // Схемы
    schemas: StoreSchemas,
    // Известные значения состояния узлов
    seeds: StoreSeeds,
}

impl MemorySchemaStore {
    fn new() -> Self {
        Self {
            schemas: Arc::new(RwLock::new(HashMap::new())),
            seeds: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get_schemas_store(&self) -> (StoreSchemas, StoreSeeds) {
        (self.schemas.clone(), self.seeds.clone())
    }
}

impl Default for MemorySchemaStore {
    fn default() -> Self {
        MemorySchemaStore::new()
    }
}

impl From<SchemaError> for RepositoryError {
    fn from(err: SchemaError) -> Self {
        match err {
            SchemaError::CycleDetected(msg) => RepositoryError::Internal(msg),
            SchemaError::EdgeExists(msg) => RepositoryError::Conflict(msg),
            SchemaError::NodeExists(msg) => RepositoryError::Conflict(msg),
            SchemaError::EdgeNotFound(msg) => RepositoryError::NotFound(msg),
            SchemaError::NodeNotFound(msg) => RepositoryError::NotFound(msg),
        }
    }
}

impl SchemaRepository<Attributes, Attributes, Attributes, String> for MemorySchemaStore {
    async fn list_schemas(&self) -> Vec<(String, Attributes)> {
        let schemas = self.schemas.read().await;
        let mut result = vec![];

        for (id, schema) in schemas.iter() {
            result.push((id.clone(), schema.read().await.attrs.clone()));
        }

        result
    }

    async fn add_schema(
        &self,
        schema_id: impl Into<String>,
        attrs: Attributes,
    ) -> Result<(), RepositoryError> {
        let schema_id = schema_id.into();
        let mut schemas = self.schemas.write().await;

        if schemas.get(&schema_id.clone()).is_some() {
            return Err(RepositoryError::Conflict(format!(
                "Schema with id {} already exists",
                &schema_id
            )));
        }

        let schema = DomainSchema::new(attrs);

        schemas.insert(schema_id.clone(), Arc::new(RwLock::new(schema)));
        let mut seeds = self.seeds.write().await;
        seeds.insert(schema_id, Arc::new(RwLock::new(HashMap::new())));

        Ok(())
    }

    async fn remove_schema(&self, schema_id: impl Into<String>) -> Result<(), RepositoryError> {
        let mut schemas = self.schemas.write().await;
        let schema_id = schema_id.into();
        if schemas.remove(&schema_id).is_some() {
            self.seeds.write().await.remove(&schema_id);
            Ok(())
        } else {
            Err(RepositoryError::NotFound(format!(
                "Schema with id {} not found",
                &schema_id
            )))
        }
    }

    async fn get_schema(
        &self,
        schema_id: String,
    ) -> Result<Arc<RwLock<DomainSchema>>, RepositoryError> {
        let schemas = self.schemas.read().await;
        if let Some(schema) = schemas.get(&schema_id) {
            Ok(schema.clone())
        } else {
            Err(RepositoryError::NotFound(format!(
                "Schema with id {} not found",
                &schema_id
            )))
        }
    }

    async fn add_node(
        &self,
        schema_id: String,
        node_id: NodeIdString,
        attrs: Attributes,
    ) -> Result<(), RepositoryError> {
        let schemas = self.schemas.read().await;

        let mut schema = match schemas.get(&schema_id) {
            Some(schema) => schema.write().await,
            None => {
                return Err(RepositoryError::NotFound(format!(
                    "Schema with id {} not found",
                    &schema_id
                )));
            }
        };

        if let Err(err) = schema.insert_node(node_id, attrs) {
            return Err(err.into());
        }

        Ok(())
    }

    async fn add_incident(
        &self,
        schema_id: String,
        incident: Incident<Attributes, String>,
        edge: IncidentEdge<Attributes, String>,
    ) -> Result<(), RepositoryError> {
        // Поверить существует ли node_id
        let schemas = self.schemas.read().await;

        let mut schema = match schemas.get(&schema_id) {
            Some(schema) => schema.write().await,
            None => {
                return Err(RepositoryError::NotFound(format!(
                    "Schema with id {} not found",
                    &schema_id
                )));
            }
        };

        if let Err(err) = schema.node(&edge.to_id) {
            return Err(err.into());
        }

        let mut incident_attrs = incident.attrs;
        incident_attrs.insert("type", AttributeValue::Text("INCIDENT".to_string()));
        incident_attrs.insert("severity", AttributeValue::Float(incident.severity));

        if let Err(err) = schema.insert_node(incident.id.clone(), incident_attrs) {
            return Err(err.into());
        }

        // Добавить связь между инцидентом и узлом на схеме
        if let Err(err) = schema.insert_edge(edge.id, incident.id.clone(), edge.to_id, edge.attrs) {
            // Если произошла ошибка, то откатываем предыдущие изменения
            let _ = schema.remove_node(&incident.id);
            return Err(err.into());
        }

        Ok(())
    }

    async fn remove_node(
        &self,
        schema_id: String,
        node_id: NodeIdString,
    ) -> Result<(), RepositoryError> {
        let schemas = self.schemas.read().await;

        let mut schema = match schemas.get(&schema_id) {
            Some(schema) => schema.write().await,
            None => {
                return Err(RepositoryError::NotFound(format!(
                    "Schema with id {} not found",
                    &schema_id
                )));
            }
        };

        let seeds = self.seeds.read().await;

        if let Err(err) = schema.remove_node(&node_id) {
            return Err(err.into());
        }

        let mut seeds = seeds.get(&schema_id).unwrap().write().await;
        seeds.remove(&node_id);

        Ok(())
    }

    async fn add_edge(
        &self,
        schema_id: String,
        edge_id: EdgeIdString,
        from: NodeIdString,
        to: NodeIdString,
        attrs: Attributes,
    ) -> Result<(), RepositoryError> {
        let schemas = self.schemas.read().await;

        let mut schema = match schemas.get(&schema_id) {
            Some(schema) => schema.write().await,
            None => {
                return Err(RepositoryError::NotFound(format!(
                    "Schema with id {} not found",
                    &schema_id
                )));
            }
        };

        if let Err(err) = schema.insert_edge(edge_id, from, to, attrs) {
            return Err(err.into());
        }

        Ok(())
    }

    async fn remove_edge(
        &self,
        schema_id: String,
        edge_id: EdgeIdString,
    ) -> Result<(), RepositoryError> {
        let schemas = self.schemas.write().await;

        let mut schema = match schemas.get(&schema_id) {
            Some(schema) => schema.write().await,
            None => {
                return Err(RepositoryError::NotFound(format!(
                    "Schema with id {} not found",
                    &schema_id
                )));
            }
        };

        if let Err(err) = schema.remove_edge(&edge_id) {
            return Err(err.into());
        }

        Ok(())
    }

    async fn compute<C>(&self, schema_id: String, f: C) -> Result<(), RepositoryError>
    where
        C: Fn(NodeIdString, f64),
    {
        let schemas = self.schemas.read().await;
        let schemas = schemas.get(&schema_id).unwrap();

        compute(schemas.clone(), f).await;

        Ok(())
    }
}
