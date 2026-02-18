use std::collections::HashMap;
use std::sync::Arc;

use ica_core::{AttributeValue, Attributes, EdgeId, Schema, NodeId};
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use crate::schema::schema_service_server::SchemaService;
use crate::schema::*;

/// In-memory storage for all schemas.
struct SchemaStore {
    schemas: RwLock<HashMap<String, Schema<Attributes, Attributes, u64>>>,
    // Predefined influence values (seeds) for nodes, keyed by schema_id -> node_id -> influence
    seeds: RwLock<HashMap<String, HashMap<NodeId<u64>, f64>>>,
}

impl Default for SchemaStore {
    fn default() -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
            seeds: RwLock::new(HashMap::new()),
        }
    }
}

#[derive(Clone, Default)]
pub struct SchemaServiceImpl {
    store: Arc<SchemaStore>,
}

impl SchemaServiceImpl {
    pub fn new() -> Self {
        Self {
            store: Arc::new(SchemaStore::default()),
        }
    }

    fn to_attributes(attrs: Vec<Attribute>) -> Attributes {
        let mut result = Attributes::new();
        for attr in attrs {
            let Some(value) = attr.value else {
                continue;
            };

            let value = match value {
                attribute::Value::Text(v) => AttributeValue::Text(v),
                attribute::Value::Integer(v) => AttributeValue::Integer(v),
                attribute::Value::Float(v) => AttributeValue::Float(v),
                attribute::Value::Boolean(v) => AttributeValue::Boolean(v),
            };

            result.insert(attr.key, value);
        }
        result
    }
}

#[tonic::async_trait]
impl SchemaService for SchemaServiceImpl {
    async fn create_schema(
        &self,
        request: Request<CreateSchemaRequest>,
    ) -> Result<Response<CreateSchemaResponse>, Status> {
        let req = request.into_inner();
        let id = req.schema_id.trim().to_owned();

        if id.is_empty() {
            return Err(Status::invalid_argument("schema_id must not be empty"));
        }

        let mut guard = self.store.schemas.write().await;
        if guard.contains_key(&id) {
            return Err(Status::already_exists("schema already exists"));
        }

        guard.insert(id.clone(), Schema::new());

        Ok(Response::new(CreateSchemaResponse { schema_id: id }))
    }

    async fn delete_schema(
        &self,
        request: Request<DeleteSchemaRequest>,
    ) -> Result<Response<DeleteSchemaResponse>, Status> {
        let req = request.into_inner();
        let id = req.schema_id.trim().to_owned();

        let mut guard = self.store.schemas.write().await;
        if guard.remove(&id).is_none() {
            return Err(Status::not_found("schema not found"));
        }

        // Also remove seeds for this schema
        let mut seeds_guard = self.store.seeds.write().await;
        seeds_guard.remove(&id);

        Ok(Response::new(DeleteSchemaResponse {}))
    }

    async fn add_node(
        &self,
        request: Request<AddNodeRequest>,
    ) -> Result<Response<AddNodeResponse>, Status> {
        let req = request.into_inner();
        let id = req.schema_id.trim().to_owned();

        let mut guard = self.store.schemas.write().await;
        let schema = guard
            .get_mut(&id)
            .ok_or_else(|| Status::not_found("schema not found"))?;

        let node_id = NodeId(req.node_id);
        if schema.node(node_id).is_some() {
            return Err(Status::already_exists("node already exists"));
        }

        let attrs = Self::to_attributes(req.attributes);
        schema.insert_node(node_id, attrs);

        Ok(Response::new(AddNodeResponse { node_id: node_id.0 }))
    }

    async fn remove_node(
        &self,
        request: Request<RemoveNodeRequest>,
    ) -> Result<Response<RemoveNodeResponse>, Status> {
        let req = request.into_inner();
        let id = req.schema_id.trim().to_owned();

        let mut guard = self.store.schemas.write().await;
        let schema = guard
            .get_mut(&id)
            .ok_or_else(|| Status::not_found("schema not found"))?;

        let node_id = NodeId(req.node_id);
        if schema.remove_node(node_id).is_none() {
            return Err(Status::not_found("node not found"));
        }

        Ok(Response::new(RemoveNodeResponse {}))
    }

    async fn add_edge(
        &self,
        request: Request<AddEdgeRequest>,
    ) -> Result<Response<AddEdgeResponse>, Status> {
        let req = request.into_inner();

        let id = req.schema_id.trim().to_owned();

        let mut guard = self.store.schemas.write().await;
        let schema = guard
            .get_mut(&id)
            .ok_or_else(|| Status::not_found("schema not found"))?;

        let from = NodeId(req.from);
        let to = NodeId(req.to);

        if schema.node(from).is_none() || schema.node(to).is_none() {
            return Err(Status::failed_precondition(
                "both `from` and `to` nodes must exist",
            ));
        }

        let edge_id = EdgeId(req.edge_id);
        if schema.edge(edge_id).is_some() {
            return Err(Status::already_exists("edge already exists"));
        }

        let attrs = Self::to_attributes(req.attributes);
        schema.insert_edge(edge_id, from, to, attrs);

        Ok(Response::new(AddEdgeResponse { edge_id: edge_id.0 }))
    }

    async fn remove_edge(
        &self,
        request: Request<RemoveEdgeRequest>,
    ) -> Result<Response<RemoveEdgeResponse>, Status> {
        let req = request.into_inner();
        let id = req.schema_id.trim().to_owned();

        let mut guard = self.store.schemas.write().await;
        let schema = guard
            .get_mut(&id)
            .ok_or_else(|| Status::not_found("schema not found"))?;

        if schema.remove_edge(ica_core::EdgeId(req.edge_id)).is_none() {
            return Err(Status::not_found("edge not found"));
        }

        Ok(Response::new(RemoveEdgeResponse {}))
    }

    async fn compute_state(
        &self,
        request: Request<ComputeStateRequest>,
    ) -> Result<Response<ComputeStateResponse>, Status> {
        let req = request.into_inner();
        let id = req.schema_id.trim().to_owned();

        let guard = self.store.schemas.read().await;
        let schema = guard
            .get(&id)
            .ok_or_else(|| Status::not_found("schema not found"))?;

        let root_id = NodeId(req.root_node_id);
        if schema.node(root_id).is_none() {
            return Err(Status::not_found("root node not found"));
        }

        // Collect seeds from request and from stored seeds for this schema
        let mut known = std::collections::HashMap::new();
        for seed in req.seeds {
            known.insert(NodeId(seed.node_id), seed.influence);
        }

        // Add stored seeds for this schema
        let seeds_guard = self.store.seeds.read().await;
        if let Some(schema_seeds) = seeds_guard.get(&id) {
            for (&node_id, &influence) in schema_seeds {
                known.insert(node_id, influence);
            }
        }

        // По умолчанию считаем, что влияние узла — это среднее значение влияния его детей.
        let result = schema.compute_influence(root_id, &known, |_, children| {
            if children.is_empty() {
                0.0
            } else {
                let sum: f64 = children.iter().map(|c| c.influence).sum();
                sum / (children.len() as f64)
            }
        });

        let states = result
            .into_iter()
            .map(|(node_id, influence)| NodeState {
                node_id: node_id.0,
                influence,
            })
            .collect();

        Ok(Response::new(ComputeStateResponse { states }))
    }

    async fn add_node_with_influence(
        &self,
        request: Request<AddNodeWithInfluenceRequest>,
    ) -> Result<Response<AddNodeWithInfluenceResponse>, Status> {
        let req = request.into_inner();
        let id = req.schema_id.trim().to_owned();

        let mut guard = self.store.schemas.write().await;
        let schema = guard
            .get_mut(&id)
            .ok_or_else(|| Status::not_found("schema not found"))?;

        let node_id = NodeId(req.node_id);
        if schema.node(node_id).is_some() {
            return Err(Status::already_exists("node already exists"));
        }

        let parent_id = NodeId(req.parent_node_id);
        if schema.node(parent_id).is_none() {
            return Err(Status::not_found("parent node not found"));
        }

        let edge_id = EdgeId(req.edge_id);
        if schema.edge(edge_id).is_some() {
            return Err(Status::already_exists("edge already exists"));
        }

        // Create the node
        let node_attrs = Self::to_attributes(req.attributes);
        schema.insert_node(node_id, node_attrs);

        // Create the edge from new node to parent (child -> parent)
        let edge_attrs = Self::to_attributes(req.edge_attributes);
        schema.insert_edge(edge_id, node_id, parent_id, edge_attrs);

        // Store the influence seed
        let mut seeds_guard = self.store.seeds.write().await;
        seeds_guard
            .entry(id)
            .or_insert_with(HashMap::new)
            .insert(node_id, req.influence);

        Ok(Response::new(AddNodeWithInfluenceResponse {
            node_id: node_id.0,
            edge_id: edge_id.0,
        }))
    }
}
