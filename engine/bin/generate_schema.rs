use ica_core::{Attributes, EdgeId, NodeId, Schema};
use rand::distr::Alphanumeric;
use rand::{RngExt, random_range};

use ica_engine::SchemaElement;
use ica_engine::schema_contracts::schema_service_client::SchemaServiceClient;
use ica_engine::{SchemaHeader, schema_contracts::*};

fn generate_schema_test_data(
    nodes_count: usize,
) -> (
    NodeId<String>,
    Schema<Attributes, Attributes, Attributes, String>,
) {
    let mut g: Schema<Attributes, Attributes, Attributes, String> = Schema::new(Attributes::new());

    let next_node_id = || -> NodeId<String> {
        let rng = rand::rng();
        NodeId(
            rng.sample_iter(Alphanumeric)
                .take(10)
                .map(char::from)
                .collect::<String>(),
        )
    };

    let next_edge_id = || -> EdgeId<String> {
        let rng = rand::rng();
        EdgeId(
            rng.sample_iter(Alphanumeric)
                .take(10)
                .map(char::from)
                .collect::<String>(),
        )
    };

    let root = next_node_id();
    g.insert_node(root.clone(), Attributes::new()).unwrap();
    let mut prev_level = vec![root.clone()];

    while g.node_count() < nodes_count {
        let mut current_level = Vec::new();
        while current_level.is_empty() {
            for parent in &prev_level {
                for _ in 0..random_range(0..3) {
                    let child_id = next_node_id();
                    g.insert_node(child_id.clone(), Attributes::new()).unwrap();
                    let _ = g.insert_edge(
                        next_edge_id(),
                        child_id.clone(),
                        parent.clone(),
                        Attributes::new(),
                    );

                    let rand_parent = random_range(0..=prev_level.len());
                    if rand_parent > 0 && prev_level[rand_parent - 1].0 != parent.0 {
                        let _ = g.insert_edge(
                            next_edge_id(),
                            child_id.clone(),
                            prev_level[rand_parent - 1].clone(),
                            Attributes::new(),
                        );
                    }

                    current_level.push(child_id);
                }
            }
        }
        prev_level = current_level;
    }

    (root, g.clone())
}

#[tokio::main]
async fn main() {
    let count_nodes: usize = std::env::args()
        .nth(1)
        .expect("count of nodes required")
        .parse()
        .expect("arg must be a number");

    let (root, mut schema) = generate_schema_test_data(count_nodes);
    let schema_id = root.0.clone();

    schema.attrs.insert(
        "name",
        ica_core::AttributeValue::Text(format!("Test Schema #{}", root.0)),
    );

    let mut client = SchemaServiceClient::connect("http://localhost:50051")
        .await
        .expect("failed to connect to server http://localhost:50051");

    let (tx, rx) = tokio::sync::mpsc::channel(10);

    tokio::spawn(async move {
        let chunk = match serde_json::to_string(&SchemaElement::Header(SchemaHeader {
            schema_id: root.0.clone(),
            attrs: schema.attrs.clone(),
        })) {
            Ok(chunk) => chunk,
            Err(err) => {
                return Err(err.to_string());
            }
        };

        if let Err(err) = tx.send(ImportSchemaRequest { chunk }).await {
            return Err(err.to_string());
        }

        for node in schema.nodes() {
            let chunk = match serde_json::to_string(&SchemaElement::Node(node.clone())) {
                Ok(chunk) => chunk,
                Err(err) => {
                    return Err(err.to_string());
                }
            };

            let request = ImportSchemaRequest { chunk };

            if let Err(err) = tx.send(request).await {
                eprintln!("{err}");
                break;
            }
        }

        for edge in schema.edges() {
            let chunk = match serde_json::to_string(&SchemaElement::Edge(edge.clone())) {
                Ok(chunk) => chunk,
                Err(err) => {
                    return Err(err.to_string());
                }
            };

            let request = ImportSchemaRequest { chunk };

            if let Err(err) = tx.send(request).await {
                return Err(err.to_string());
            }
        }

        Ok(())
    });

    let request_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let request = tonic::Request::new(request_stream);

    let _ = client
        .import_schema(request)
        .await
        .expect("Error on import schema");

    println!("Schema {} imported successfully", &schema_id);

    let _ = client
        .layout(LayoutRequest {
            schema_id: schema_id.clone(),
        })
        .await
        .expect("Layout");

    println!("Schema {} laying is complete", schema_id);
}
