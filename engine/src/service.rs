use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use ica_core::{AttributeValue, Attributes, EdgeId, NodeId, Schema};
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tokio::time;
use tonic::{Request, Response, Status};

use crate::compute_fn::compute;
use crate::mem_store::MemorySchemaStore;
use crate::repository::SchemaRepository;
use crate::repository::{self, RepositoryError};
use crate::schema_contracts::schema_service_server::SchemaService;
use crate::schema_contracts::{
    AddEdgeRequest, AddEdgeResponse, AddIncidentRequest, AddIncidentResponse, AddNodeRequest,
    AddNodeResponse, Attribute, ComputeStateRequest, ComputeStateResponse, CreateSchemaRequest,
    CreateSchemaResponse, DeleteSchemaRequest, DeleteSchemaResponse, Edge, GetSchemaRequest,
    GetSchemaResponse, Node, RemoveEdgeRequest, RemoveEdgeResponse, RemoveNodeRequest,
    RemoveNodeResponse, attribute, get_schema_response,
};

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

fn to_vec_attributes(attrs: Attributes) -> Vec<Attribute> {
    attrs
        .iter()
        .map(|(key, value)| {
            let value = match value {
                AttributeValue::Text(v) => attribute::Value::Text(v.to_string()),
                AttributeValue::Integer(v) => attribute::Value::Integer(*v),
                AttributeValue::Float(v) => attribute::Value::Float(*v),
                AttributeValue::Boolean(v) => attribute::Value::Boolean(*v),
            };
            Attribute {
                key: key.0.clone(),
                value: Some(value),
            }
        })
        .collect()
}

impl From<RepositoryError> for Status {
    fn from(err: RepositoryError) -> Self {
        match err {
            RepositoryError::NotFound(err) => Status::not_found(err.to_string()),
            RepositoryError::Conflict(err) => Status::already_exists(err.to_string()),
            RepositoryError::Internal(err) => Status::internal(err.to_string()),
        }
    }
}

#[derive(Default)]
pub struct SchemaServiceImpl {
    store: MemorySchemaStore,
    durty_schemas: Arc<RwLock<HashSet<String>>>,
}

impl SchemaServiceImpl {
    pub fn new() -> Self {
        let result = Self {
            store: MemorySchemaStore::default(),
            durty_schemas: Arc::new(RwLock::new(HashSet::new())),
        };

        result.start_worker(1);

        result
    }

    pub fn start_worker(&self, interval_secs: u64) {
        let durty_schemas = self.durty_schemas.clone();
        let (schemas, seeds) = self.store.get_schemas_store();

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(interval_secs));

            loop {
                interval.tick().await;
                let mut durty_schemas = durty_schemas.write().await;

                if durty_schemas.is_empty() {
                    println!("durty_schemas is empty, skip");
                    continue;
                }

                for schema_id in durty_schemas.clone() {
                    let schema = schemas.read().await;
                    let schema = schema.get(&schema_id).unwrap();
                    let seeds = seeds.read().await;
                    let seeds = seeds.get(&schema_id).unwrap();

                    println!("Processing schema {}", schema_id);

                    compute(schema.clone(), seeds.clone(), |node_id, state| {
                        println!("Node {} state: {}", node_id.0, state);
                    })
                    .await;

                    durty_schemas.remove(&schema_id);
                }
            }
        });
    }
}

#[tonic::async_trait]
impl SchemaService for SchemaServiceImpl {
    type GetSchemaStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<GetSchemaResponse, Status>> + Send + 'static>,
    >;
    type ComputeStateStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<ComputeStateResponse, Status>> + Send + 'static>,
    >;

    async fn get_schema(
        &self,
        request: Request<GetSchemaRequest>,
    ) -> Result<Response<Self::GetSchemaStream>, Status> {
        let req = request.into_inner();
        let schema_id = req.schema_id.clone();

        let schema = match self.store.get_schema(schema_id).await {
            Err(err) => return Err(err.into()),
            Ok(schema) => schema,
        };

        let (tx, rx) = mpsc::channel(100);
        let tx_nodes = tx.clone();
        let tx_edges = tx.clone();

        let schema_nodes_ref = schema.clone();
        tokio::spawn(async move {
            let schema = schema_nodes_ref.read().await;

            for node in schema.nodes() {
                let node = Node {
                    node_id: node.id.clone().0,
                    state: 0.0,
                    attributes: to_vec_attributes(node.attrs.clone()),
                };

                let item = get_schema_response::Item::Node(node.clone());
                let response = GetSchemaResponse { item: Some(item) };

                if tx_nodes.send(Ok(response)).await.is_err() {
                    break;
                }
            }
        });

        let schema_edges_ref = schema.clone();
        tokio::spawn(async move {
            let schema = schema_edges_ref.read().await;
            for edge in schema.edges() {
                let edge = Edge {
                    edge_id: edge.id.clone().0,
                    from_id: edge.from.clone().0,
                    to_id: edge.to.clone().0,
                    weight: 1.0,
                };

                let item = get_schema_response::Item::Edge(edge.clone());
                let response = GetSchemaResponse { item: Some(item) };

                if tx_edges.send(Ok(response)).await.is_err() {
                    break;
                }
            }
        });

        let output_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

        Ok(Response::new(
            Box::pin(output_stream) as Self::GetSchemaStream
        ))
    }

    async fn create_schema(
        &self,
        request: Request<CreateSchemaRequest>,
    ) -> Result<Response<CreateSchemaResponse>, Status> {
        let req = request.into_inner();
        let schema_id = req.schema_id.trim().to_owned();

        if let Err(err) = self
            .store
            .add_schema(schema_id.clone(), Schema::new())
            .await
        {
            return Err(err.into());
        }

        Ok(Response::new(CreateSchemaResponse { schema_id }))
    }

    async fn delete_schema(
        &self,
        request: Request<DeleteSchemaRequest>,
    ) -> Result<Response<DeleteSchemaResponse>, Status> {
        let req = request.into_inner();
        let schema_id = req.schema_id.trim().to_owned();

        if let Err(err) = self.store.remove_schema(schema_id).await {
            return Err(err.into());
        }

        Ok(Response::new(DeleteSchemaResponse {}))
    }

    async fn add_node(
        &self,
        request: Request<AddNodeRequest>,
    ) -> Result<Response<AddNodeResponse>, Status> {
        let req = request.into_inner();
        let schema_id = req.schema_id.trim().to_owned();
        let node_id = NodeId(req.node_id.trim().to_owned());

        if let Err(err) = self
            .store
            .add_node(schema_id, node_id.clone(), to_attributes(req.attributes))
            .await
        {
            return Err(err.into());
        }

        Ok(Response::new(AddNodeResponse { node_id: node_id.0 }))
    }

    async fn remove_node(
        &self,
        request: Request<RemoveNodeRequest>,
    ) -> Result<Response<RemoveNodeResponse>, Status> {
        let req = request.into_inner();
        let schema_id = req.schema_id.trim().to_owned();
        let node_id = req.node_id.trim().to_owned();

        if let Err(err) = self.store.remove_node(schema_id, NodeId(node_id)).await {
            return Err(err.into());
        }

        Ok(Response::new(RemoveNodeResponse {}))
    }

    async fn add_edge(
        &self,
        request: Request<AddEdgeRequest>,
    ) -> Result<Response<AddEdgeResponse>, Status> {
        let req = request.into_inner();
        let schema_id = req.schema_id.trim().to_owned();
        let edge_id = req.edge_id.trim().to_owned();
        let from_id = req.from_id.trim().to_owned();
        let to_id = req.to_id.trim().to_owned();

        if let Err(err) = self
            .store
            .add_edge(
                schema_id,
                EdgeId(edge_id.clone()),
                NodeId(from_id),
                NodeId(to_id),
                to_attributes(req.attributes),
            )
            .await
        {
            return Err(err.into());
        };

        Ok(Response::new(AddEdgeResponse { edge_id }))
    }

    async fn remove_edge(
        &self,
        request: Request<RemoveEdgeRequest>,
    ) -> Result<Response<RemoveEdgeResponse>, Status> {
        let req = request.into_inner();
        let id = req.schema_id.trim().to_owned();

        if let Err(err) = self.store.remove_edge(id, EdgeId(req.edge_id)).await {
            return Err(err.into());
        }

        Ok(Response::new(RemoveEdgeResponse {}))
    }

    async fn add_incident(
        &self,
        request: Request<AddIncidentRequest>,
    ) -> Result<Response<AddIncidentResponse>, Status> {
        let req = request.into_inner();
        let schema_id = req.schema_id.trim().to_owned();

        if schema_id.is_empty() {
            return Err(Status::invalid_argument("schema_id is empty"));
        }

        let incident = match req.incident {
            Some(incident) => incident,
            None => return Err(Status::invalid_argument("incident is empty")),
        };

        let edge = match req.edge {
            Some(edge) => edge,
            None => return Err(Status::invalid_argument("edge is empty")),
        };

        if let Err(err) = self
            .store
            .add_incident(
                schema_id.clone(),
                repository::Incident {
                    id: NodeId(incident.node_id.clone()),
                    attrs: to_attributes(incident.attributes),
                    severity: incident.severity,
                },
                repository::IncidentEdge {
                    id: EdgeId(edge.edge_id.clone()),
                    to_id: NodeId(edge.to_id),
                    attrs: to_attributes(edge.attributes),
                },
            )
            .await
        {
            return Err(err.into());
        }

        let mut durty_schemas = self.durty_schemas.write().await;
        durty_schemas.insert(schema_id);

        Ok(Response::new(AddIncidentResponse {
            node_id: incident.node_id,
            edge_id: edge.edge_id,
        }))
    }

    async fn compute_state(
        &self,
        request: Request<ComputeStateRequest>,
    ) -> Result<Response<Self::ComputeStateStream>, Status> {
        let req = request.into_inner();
        let schema_id = req.schema_id.trim().to_owned();
        let (tx, rx) = mpsc::channel(100);

        match self
            .store
            .compute(schema_id, |node_id, state| {
                let tx = tx.clone();
                println!("Node {} = {}", node_id.clone().0, state);
                tokio::spawn(async move {
                    tx.send(Ok(ComputeStateResponse {
                        node_id: node_id.0,
                        state,
                    }))
                    .await
                });
            })
            .await
        {
            Ok(()) => {}
            Err(err) => return Err(err.into()),
        }

        let output_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::ComputeStateStream
        ))
    }
}

#[cfg(test)]
mod tests_engine_service {
    use crate::schema_contracts::attribute::Value;

    use super::*;

    #[test]
    fn test_to_vec_attribute() {
        let mut attrs = Attributes::new();
        attrs.insert("attr-1", AttributeValue::Text("some text".to_string()));
        attrs.insert("attr-2", AttributeValue::Integer(123));
        attrs.insert("attr-3", AttributeValue::Boolean(true));
        assert_eq!(attrs.len(), 3);

        let vec = to_vec_attributes(attrs);
        assert_eq!(vec.len(), 3);

        for attr in vec {
            let (key, value) = (attr.key.as_str(), attr.value.unwrap());
            match key {
                "attr-1" => assert_eq!(value, Value::Text("some text".to_string())),
                "attr-2" => assert_eq!(value, Value::Integer(123)),
                "attr-3" => assert_eq!(value, Value::Boolean(true)),
                _ => panic!("Unexpected key: {key}"),
            }
        }
    }

    #[test]
    fn test_to_attributes() {
        let vec = vec![
            Attribute {
                key: "attr-1".to_string(),
                value: Some(Value::Text("some text".to_string())),
            },
            Attribute {
                key: "attr-2".to_string(),
                value: Some(Value::Integer(123)),
            },
            Attribute {
                key: "attr-3".to_string(),
                value: Some(Value::Boolean(true)),
            },
        ];

        let attrs = to_attributes(vec);
        assert!(!attrs.is_empty());
        assert_eq!(attrs.len(), 3);

        if let AttributeValue::Text(text) = attrs.get("attr-1").unwrap() {
            assert_eq!(text, "some text");
        } else {
            panic!("Unexpected attribute value");
        }
    }
}
