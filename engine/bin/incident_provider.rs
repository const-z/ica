use std::{sync::Arc, time::Duration};

use ica_core::NodeId;
use ica_engine::schema_contracts::{schema_service_client::SchemaServiceClient, *};
use rand::{RngExt, distr::Alphanumeric, random_range};
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use tonic::Request;

fn next_node_id() -> NodeId<String> {
    let rng = rand::rng();
    NodeId(
        rng.sample_iter(Alphanumeric)
            .take(10)
            .map(char::from)
            .collect::<String>(),
    )
}

#[tokio::main]
async fn main() {
    let schema_id = std::env::args().nth(1).expect("schema_id required");
    let interval: u64 = std::env::args()
        .nth(2)
        .expect("interval required")
        .parse()
        .expect("interval must be i32");
    let probability: u64 = std::env::args()
        .nth(3)
        .expect("probability required")
        .parse()
        .expect("probability must be i32");

    let mut client = SchemaServiceClient::connect("http://localhost:50051")
        .await
        .expect("failed to connect to server http://localhost:50051");

    // получить узлы схемы
    let mut stream = match client
        .get_schema(Request::new(GetSchemaRequest {
            schema_id: schema_id.clone(),
            include_incidents: true,
        }))
        .await
    {
        Ok(response) => response.into_inner(),
        Err(e) => {
            eprintln!("Error: {:?}", e);
            return;
        }
    };

    let nodes = Arc::new(Mutex::new(vec![]));
    let incidents = Arc::new(Mutex::new(vec![]));

    while let Some(response) = stream.next().await {
        match response {
            Ok(response) => {
                if let Some(item) = response.item
                    && let get_schema_response::Item::Node(node) = item
                {
                    if node.attributes.iter().any(|i| {
                        i.key == "type"
                            && match &i.value {
                                Some(attribute::Value::Text(v)) => v == "INCIDENT",
                                Some(_) => false,
                                None => false,
                            }
                    }) {
                        println!("Incident: {:#?}", node);
                        incidents.lock().await.push(node.node_id.clone());
                    };
                    nodes.lock().await.push(node);
                }
            }
            Err(e) => {
                eprintln!("Error: {:?}", e);
                return;
            }
        }
    }

    println!("Incidents: {:#?}", incidents.lock().await);

    let nodes_ref = nodes.clone();
    let _ = tokio::spawn(async move {
        loop {
            let del_or_ins = random_range(0..=probability);

            if del_or_ins == 0 {
                let node = {
                    let nodes = nodes_ref.lock().await;
                    nodes[random_range(0..nodes.len())].clone()
                };

                let incident_id: NodeId<String> = next_node_id();

                let request = AddIncidentRequest {
                    edge: Some(IncidentEdge {
                        edge_id: next_node_id().0,
                        to_id: node.node_id,
                        attributes: vec![],
                    }),
                    schema_id: schema_id.clone(),
                    incident: Some(Incident {
                        attributes: vec![],
                        node_id: incident_id.0.clone(),
                        severity: random_range(0.0..50.0) / 100.0,
                    }),
                };

                if let Err(err) = client.add_incident(Request::new(request)).await {
                    eprintln!("Error: {:?}", err);
                    continue;
                }

                incidents.lock().await.push(incident_id.0);
            } else {
                let mut incidents = incidents.lock().await;
                if incidents.is_empty() {
                    continue;
                }
                let inc_idx = random_range(0..incidents.len());
                let request = RemoveNodeRequest {
                    node_id: incidents[inc_idx].clone(),
                    schema_id: schema_id.clone(),
                };

                if let Err(err) = client.remove_node(Request::new(request)).await {
                    eprintln!("Error: {:?}", err);
                    continue;
                }

                incidents.remove(inc_idx);
            }

            tokio::time::sleep(Duration::from_secs(interval)).await;
        }
    })
    .await;
}
