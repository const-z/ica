pub mod schema_contracts {
    tonic::include_proto!("ica.schema.v1");
}

const ENGINE_GRPS_SERVER: &str = "http://127.0.0.1:50051";

use std::collections::HashMap;

use tokio_stream::StreamExt;
use tonic::Status;
use tonic_web_wasm_client::{Client, options::FetchOptions};

use crate::client::schema_contracts::{
    Attribute, Edge, GetSchemaRequest, GetStateRequest, ListSchemasRequest, Node, attribute::Value,
    get_schema_response, schema_service_client::SchemaServiceClient,
};

// use crate::client::schema_contracts::{Edge, GetSchemaRequest, Node};

// pub fn get_attr_value() {

// }

pub trait GetAttrValue {
    fn get_text(&self, key: &str) -> Option<String>;
    fn get_float(&self, key: &str) -> Option<f64>;
}

impl GetAttrValue for Vec<Attribute> {
    fn get_text(&self, key: &str) -> Option<String> {
        self.iter()
            .find(|e| e.key == key)
            .and_then(|e| e.value.as_ref())
            .and_then(|v| match v {
                Value::Text(text) => Some(text.clone()),
                _ => None,
            })
    }

    fn get_float(&self, key: &str) -> Option<f64> {
        self.iter()
            .find(|e| e.key == key)
            .and_then(|e| e.value.as_ref())
            .and_then(|v| match v {
                Value::Float(v) => Some(*v),
                _ => None,
            })
    }
}

pub async fn list_schemas() -> Result<Vec<(String, String)>, Status> {
    let client = Client::new_with_options(
        ENGINE_GRPS_SERVER.to_string(),
        FetchOptions {
            timeout: Some(std::time::Duration::from_secs(2)),
            ..Default::default()
        },
    );
    let mut schema_service = SchemaServiceClient::new(client);

    let request = tonic::Request::new(ListSchemasRequest {});
    let response = match schema_service.list_schemas(request).await {
        Ok(response) => response,
        Err(e) => {
            return Err(e);
        }
    };

    let r = response
        .into_inner()
        .schemas
        .iter()
        .map(|item| {
            let name = item
                .attributes
                .get_text("name")
                .unwrap_or("Noname".to_string());
            (item.schema_id.clone(), name)
        })
        .collect();

    Ok(r)
}

pub async fn get_states() -> Result<Vec<(String, f64)>, Status> {
    let client = Client::new_with_options(
        ENGINE_GRPS_SERVER.to_string(),
        FetchOptions {
            timeout: Some(std::time::Duration::from_secs(2)),
            ..Default::default()
        },
    );
    let mut schema_service = SchemaServiceClient::new(client);

    let request = tonic::Request::new(GetStateRequest {});
    let response = match schema_service.get_state(request).await {
        Ok(response) => response,
        Err(e) => {
            return Err(e);
        }
    };

    let r = response
        .into_inner()
        .states
        .iter()
        .map(|item| (item.node_id.clone(), item.state))
        .collect();

    Ok(r)
}

pub async fn get_schema(
    schema_id: String,
) -> Result<(HashMap<String, Node>, HashMap<String, Edge>), Status> {
    let client = Client::new_with_options(
        ENGINE_GRPS_SERVER.to_string(),
        FetchOptions {
            timeout: Some(std::time::Duration::from_secs(2)),
            ..Default::default()
        },
    );
    let mut schema_service = SchemaServiceClient::new(client);

    let request = tonic::Request::new(GetSchemaRequest {
        schema_id,
        include_incidents: false,
    });
    let mut stream = match schema_service.get_schema(request).await {
        Ok(response) => response.into_inner(),
        Err(e) => {
            return Err(e);
        }
    };

    let mut nodes: HashMap<String, Node> = HashMap::new();
    let mut edges: HashMap<String, Edge> = HashMap::new();

    while let Some(response) = stream.next().await {
        match response {
            Ok(response) => {
                if let Some(item) = response.item {
                    match item {
                        get_schema_response::Item::Node(node) => {
                            nodes.insert(node.node_id.clone(), node);
                        }
                        get_schema_response::Item::Edge(edge) => {
                            edges.insert(edge.edge_id.clone(), edge);
                        }
                    }
                }
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    Ok((nodes, edges))
}
