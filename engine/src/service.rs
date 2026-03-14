use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use ica_core::{AttributeValue, Attributes, EdgeId, NodeId};
use ica_layout::Layout;
use ica_layout::LayoutSettings;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tokio::time;
use tonic::{Request, Response, Status};

use crate::compute_fn::compute;
use crate::mem_store::MemorySchemaStore;
use crate::repository::{self, RepositoryError, SchemaRepository};

use crate::schema_contracts::{
    AddEdgeRequest, AddEdgeResponse, AddIncidentRequest, AddIncidentResponse, AddNodeRequest,
    AddNodeResponse, Attribute, ComputeStateRequest, ComputeStateResponse, CreateSchemaRequest,
    CreateSchemaResponse, DeleteSchemaRequest, DeleteSchemaResponse, Edge, ExportSchemaRequest,
    ExportSchemaResponse, GetSchemaRequest, GetSchemaResponse, GetStateRequest, GetStateResponse,
    ImportSchemaRequest, ImportSchemaResponse, LayoutRequest, LayoutResponse, ListSchemasRequest,
    ListSchemasResponse, Node, RemoveEdgeRequest, RemoveEdgeResponse, RemoveNodeRequest,
    RemoveNodeResponse, attribute, get_schema_response, list_schemas_response,
    schema_service_server::SchemaService,
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
                    continue;
                }

                for schema_id in durty_schemas.clone() {
                    let schema = schemas.read().await;
                    let schema = schema.get(&schema_id).unwrap();
                    let seeds = seeds.read().await;
                    let mut seeds = seeds.get(&schema_id).unwrap().write().await;

                    println!("Processing schema {}", schema_id);

                    compute(schema.clone(), |node_id, state| {
                        seeds.insert(node_id, state);
                    })
                    .await;

                    durty_schemas.remove(&schema_id);
                }
            }
        });
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaHeader {
    pub schema_id: String,
    pub attrs: Attributes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchemaElement {
    Header(SchemaHeader),
    Node(ica_core::schema::Node<Attributes, String>),
    Edge(ica_core::schema::Edge<Attributes, String>),
}

#[tonic::async_trait]
impl SchemaService for SchemaServiceImpl {
    type GetSchemaStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<GetSchemaResponse, Status>> + Send + 'static>,
    >;
    type ComputeStateStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<ComputeStateResponse, Status>> + Send + 'static>,
    >;
    type ExportSchemaStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<ExportSchemaResponse, Status>> + Send + 'static>,
    >;

    async fn get_schema(
        &self,
        request: Request<GetSchemaRequest>,
    ) -> Result<Response<Self::GetSchemaStream>, Status> {
        let req = request.into_inner();
        println!("get_schema {:?}", req);

        let schema_id = req.schema_id.clone();
        let include_incidents = req.include_incidents;

        let schema = self.store.get_schema(schema_id).await?;
        let (tx, rx) = mpsc::channel(100);
        let tx_nodes = tx.clone();
        let tx_edges = tx.clone();

        let schema = schema.clone();
        tokio::spawn(async move {
            let schema = schema.read().await;
            let mut nodes_ignore = vec![];

            for node in schema.nodes() {
                if !include_incidents
                    && let Some(node_type) = node.attrs.get_text("type")
                    && node_type == "INCIDENT"
                {
                    nodes_ignore.push(node.id.0.clone());
                    continue;
                }

                let node = Node {
                    node_id: node.id.clone().0,
                    attributes: to_vec_attributes(node.attrs.clone()),
                };

                let item = get_schema_response::Item::Node(node.clone());
                let response = GetSchemaResponse { item: Some(item) };

                if tx_nodes.send(Ok(response)).await.is_err() {
                    break;
                }
            }

            for edge in schema.edges() {
                if nodes_ignore.contains(&edge.from.0) {
                    continue;
                }

                let edge = Edge {
                    edge_id: edge.id.clone().0,
                    from_id: edge.from.clone().0,
                    to_id: edge.to.clone().0,
                };

                let item = get_schema_response::Item::Edge(edge.clone());
                let response = GetSchemaResponse { item: Some(item) };

                if tx_edges.send(Ok(response)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(
            Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)) as Self::GetSchemaStream,
        ))
    }

    async fn create_schema(
        &self,
        request: Request<CreateSchemaRequest>,
    ) -> Result<Response<CreateSchemaResponse>, Status> {
        let req = request.into_inner();
        let schema_id = req.schema_id.trim().to_owned();

        self.store
            .add_schema(schema_id.clone(), to_attributes(req.attributes))
            .await?;

        Ok(Response::new(CreateSchemaResponse { schema_id }))
    }

    async fn delete_schema(
        &self,
        request: Request<DeleteSchemaRequest>,
    ) -> Result<Response<DeleteSchemaResponse>, Status> {
        let req = request.into_inner();
        let schema_id = req.schema_id.trim().to_owned();

        self.store.remove_schema(schema_id).await?;

        Ok(Response::new(DeleteSchemaResponse {}))
    }

    async fn add_node(
        &self,
        request: Request<AddNodeRequest>,
    ) -> Result<Response<AddNodeResponse>, Status> {
        let req = request.into_inner();
        let schema_id = req.schema_id.trim().to_owned();
        let node_id = NodeId(req.node_id.trim().to_owned());

        self.store
            .add_node(schema_id, node_id.clone(), to_attributes(req.attributes))
            .await?;

        Ok(Response::new(AddNodeResponse { node_id: node_id.0 }))
    }

    async fn remove_node(
        &self,
        request: Request<RemoveNodeRequest>,
    ) -> Result<Response<RemoveNodeResponse>, Status> {
        let req = request.into_inner();
        let schema_id = req.schema_id.trim().to_owned();
        let node_id = req.node_id.trim().to_owned();

        self.store
            .remove_node(schema_id.clone(), NodeId(node_id))
            .await?;
        let mut durty_schemas = self.durty_schemas.write().await;
        durty_schemas.insert(schema_id);

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

        self.store
            .add_edge(
                schema_id,
                EdgeId(edge_id.clone()),
                NodeId(from_id),
                NodeId(to_id),
                to_attributes(req.attributes),
            )
            .await?;

        Ok(Response::new(AddEdgeResponse { edge_id }))
    }

    async fn remove_edge(
        &self,
        request: Request<RemoveEdgeRequest>,
    ) -> Result<Response<RemoveEdgeResponse>, Status> {
        let req = request.into_inner();
        let id = req.schema_id.trim().to_owned();

        self.store.remove_edge(id, EdgeId(req.edge_id)).await?;

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

        self.store
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
            .await?;

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

        self.store
            .compute(schema_id, |node_id, state| {
                let tx = tx.clone();
                tokio::spawn(async move {
                    tx.send(Ok(ComputeStateResponse {
                        node_id: node_id.0,
                        state,
                    }))
                    .await
                });
            })
            .await?;

        let output_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::ComputeStateStream
        ))
    }

    async fn export_schema(
        &self,
        request: Request<ExportSchemaRequest>,
    ) -> Result<Response<Self::ExportSchemaStream>, Status> {
        let req = request.into_inner();
        let schema_id = req.schema_id.trim().to_owned();

        let schema = self.store.get_schema(schema_id.clone()).await?;

        let (tx, rx) = mpsc::channel(100);
        let tx_schema = tx.clone();

        let attrs = {
            let s = schema.read().await;
            s.attrs.clone()
        };

        let chunk = match serde_json::to_string(&SchemaElement::Header(SchemaHeader {
            schema_id,
            attrs,
        })) {
            Ok(chunk) => chunk,
            Err(err) => {
                return Err(Status::internal(err.to_string()));
            }
        };

        if let Err(err) = tx_schema.send(Ok(ExportSchemaResponse { chunk })).await {
            return Err(Status::internal(err.to_string()));
        }

        let schema_ref = schema.clone();
        tokio::spawn(async move {
            let schema = schema_ref.read().await;

            for node in schema.nodes() {
                let chunk = match serde_json::to_string(&SchemaElement::Node(node.clone())) {
                    Ok(chunk) => chunk,
                    Err(err) => {
                        return Err(Status::internal(err.to_string()));
                    }
                };

                let response = ExportSchemaResponse { chunk };

                if tx_schema.send(Ok(response)).await.is_err() {
                    break;
                }
            }

            for edge in schema.edges() {
                let chunk = match serde_json::to_string(&SchemaElement::Edge(edge.clone())) {
                    Ok(chunk) => chunk,
                    Err(err) => {
                        return Err(Status::internal(err.to_string()));
                    }
                };

                let response = ExportSchemaResponse { chunk };

                if tx_schema.send(Ok(response)).await.is_err() {
                    break;
                }
            }

            Ok(())
        });

        let output_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::ExportSchemaStream
        ))
    }

    async fn import_schema(
        &self,
        request: Request<tonic::Streaming<ImportSchemaRequest>>,
    ) -> Result<Response<ImportSchemaResponse>, Status> {
        let mut stream = request.into_inner();
        let mut schema_id = String::new();

        while let Some(message) = stream.message().await.unwrap() {
            let item = match serde_json::from_str::<SchemaElement>(&message.chunk) {
                Ok(item) => item,
                Err(err) => {
                    return Err(Status::internal(err.to_string()));
                }
            };
            match item {
                SchemaElement::Header(header) => {
                    self.store
                        .add_schema(header.schema_id.clone(), header.attrs)
                        .await?;
                    schema_id = header.schema_id;
                }
                SchemaElement::Node(node) => {
                    self.store
                        .add_node(schema_id.clone(), node.id, node.attrs)
                        .await?;
                }
                SchemaElement::Edge(edge) => {
                    self.store
                        .add_edge(schema_id.clone(), edge.id, edge.from, edge.to, edge.attrs)
                        .await?;
                }
            }
        }

        Ok(Response::new(ImportSchemaResponse {}))
    }

    async fn layout(
        &self,
        request: Request<LayoutRequest>,
    ) -> Result<Response<LayoutResponse>, Status> {
        let req = request.into_inner();
        let schema_id = req.schema_id.trim().to_owned();
        let schema = self.store.get_schema(schema_id).await?;

        tokio::spawn(async move {
            let positions = {
                let schema = schema.read().await;
                schema.layout(LayoutSettings {
                    space_between_nodes: 60.0,
                    node_width: 80.0,
                    node_height: 40.0,
                })
            };

            let mut schema = schema.write().await;
            for (node_id, position) in &positions {
                let n = schema.node_mut(node_id).unwrap();
                n.attrs.insert("x", AttributeValue::Float(position.x));
                n.attrs.insert("y", AttributeValue::Float(position.y));
            }
        });

        Ok(Response::new(LayoutResponse {}))
    }

    async fn list_schemas(
        &self,
        request: Request<ListSchemasRequest>,
    ) -> Result<Response<ListSchemasResponse>, Status> {
        println!("list_schemas {:?}", request.into_inner());

        let schemas = self.store.list_schemas().await;
        let schemas = schemas
            .iter()
            .map(|s| list_schemas_response::Schema {
                schema_id: s.0.clone(),
                attributes: to_vec_attributes(s.1.clone()),
            })
            .collect();

        Ok(Response::new(ListSchemasResponse { schemas }))
    }

    async fn get_state(
        &self,
        _request: Request<GetStateRequest>,
    ) -> Result<Response<GetStateResponse>, Status> {
        let (_, seeds) = self.store.get_schemas_store();
        let seeds = seeds.read().await;
        let mut states = vec![];
        for s in seeds.values() {
            for (node_id, state) in s.read().await.iter() {
                states.push(ComputeStateResponse {
                    node_id: node_id.0.clone(),
                    state: *state,
                });
            }
        }

        Ok(Response::new(GetStateResponse { states }))
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
