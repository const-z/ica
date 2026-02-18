#[cfg(test)]
pub mod grpc_tests {
    use std::time::Duration;
    use tokio::time::sleep;
    use tonic::Request;
    use tonic::transport::{Channel, Server};

    use ica_grpc::SchemaServiceImpl;
    use ica_grpc::schema::schema_service_client::SchemaServiceClient;
    use ica_grpc::schema::schema_service_server::SchemaServiceServer;
    use ica_grpc::schema::*;

    /// Helper function to create a test server and client
    async fn setup_test_server() -> SchemaServiceClient<Channel> {
        let service = SchemaServiceImpl::new();

        // Bind to a random port
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            Server::builder()
                .add_service(SchemaServiceServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        // Give server time to start
        sleep(Duration::from_millis(100)).await;

        SchemaServiceClient::connect(format!("http://{}", addr))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_create_and_delete_schema() {
        let mut client = setup_test_server().await;

        // Create schema
        let create_req = CreateSchemaRequest {
            schema_id: "test-schema-1".to_string(),
        };
        let response = client
            .create_schema(Request::new(create_req))
            .await
            .unwrap();
        assert_eq!(response.get_ref().schema_id, "test-schema-1");

        // Try to create duplicate - should fail
        let create_req2 = CreateSchemaRequest {
            schema_id: "test-schema-1".to_string(),
        };
        let result = client.create_schema(Request::new(create_req2)).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::AlreadyExists);

        // Delete schema
        let delete_req = DeleteSchemaRequest {
            schema_id: "test-schema-1".to_string(),
        };
        let response = client
            .delete_schema(Request::new(delete_req))
            .await
            .unwrap();
        assert!(response.get_ref() == &DeleteSchemaResponse {});

        // Try to delete again - should fail
        let delete_req2 = DeleteSchemaRequest {
            schema_id: "test-schema-1".to_string(),
        };
        let result = client.delete_schema(Request::new(delete_req2)).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_add_and_remove_nodes() {
        let mut client = setup_test_server().await;

        // Create schema
        let create_req = CreateSchemaRequest {
            schema_id: "test-schema-2".to_string(),
        };
        client
            .create_schema(Request::new(create_req))
            .await
            .unwrap();

        // Add node
        let add_node_req = AddNodeRequest {
            schema_id: "test-schema-2".to_string(),
            node_id: 1,
            attributes: vec![Attribute {
                key: "name".to_string(),
                value: Some(attribute::Value::Text("node-1".to_string())),
            }],
        };
        let response = client.add_node(Request::new(add_node_req)).await.unwrap();
        assert_eq!(response.get_ref().node_id, 1);

        // Try to add duplicate node - should fail
        let add_node_req2 = AddNodeRequest {
            schema_id: "test-schema-2".to_string(),
            node_id: 1,
            attributes: vec![],
        };
        let result = client.add_node(Request::new(add_node_req2)).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::AlreadyExists);

        // Remove node
        let remove_node_req = RemoveNodeRequest {
            schema_id: "test-schema-2".to_string(),
            node_id: 1,
        };
        client
            .remove_node(Request::new(remove_node_req))
            .await
            .unwrap();

        // Try to remove again - should fail
        let remove_node_req2 = RemoveNodeRequest {
            schema_id: "test-schema-2".to_string(),
            node_id: 1,
        };
        let result = client.remove_node(Request::new(remove_node_req2)).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_add_and_remove_edges() {
        let mut client = setup_test_server().await;

        // Create schema
        let create_req = CreateSchemaRequest {
            schema_id: "test-schema-3".to_string(),
        };
        client
            .create_schema(Request::new(create_req))
            .await
            .unwrap();

        // Add nodes
        client
            .add_node(Request::new(AddNodeRequest {
                schema_id: "test-schema-3".to_string(),
                node_id: 1,
                attributes: vec![],
            }))
            .await
            .unwrap();

        client
            .add_node(Request::new(AddNodeRequest {
                schema_id: "test-schema-3".to_string(),
                node_id: 2,
                attributes: vec![],
            }))
            .await
            .unwrap();

        // Add edge
        let add_edge_req = AddEdgeRequest {
            schema_id: "test-schema-3".to_string(),
            from: 1,
            to: 2,
            edge_id: 10,
            attributes: vec![Attribute {
                key: "weight".to_string(),
                value: Some(attribute::Value::Float(0.5)),
            }],
        };
        let response = client.add_edge(Request::new(add_edge_req)).await.unwrap();
        assert_eq!(response.get_ref().edge_id, 10);

        // Try to add duplicate edge - should fail
        let add_edge_req2 = AddEdgeRequest {
            schema_id: "test-schema-3".to_string(),
            from: 1,
            to: 2,
            edge_id: 10,
            attributes: vec![],
        };
        let result = client.add_edge(Request::new(add_edge_req2)).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::AlreadyExists);

        // Remove edge
        let remove_edge_req = RemoveEdgeRequest {
            schema_id: "test-schema-3".to_string(),
            edge_id: 10,
        };
        client
            .remove_edge(Request::new(remove_edge_req))
            .await
            .unwrap();

        // Try to remove again - should fail
        let remove_edge_req2 = RemoveEdgeRequest {
            schema_id: "test-schema-3".to_string(),
            edge_id: 10,
        };
        let result = client.remove_edge(Request::new(remove_edge_req2)).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_add_node_with_influence() {
        let mut client = setup_test_server().await;

        // Create schema
        let create_req = CreateSchemaRequest {
            schema_id: "test-schema-4".to_string(),
        };
        client
            .create_schema(Request::new(create_req))
            .await
            .unwrap();

        // Add parent node
        client
            .add_node(Request::new(AddNodeRequest {
                schema_id: "test-schema-4".to_string(),
                node_id: 100,
                attributes: vec![],
            }))
            .await
            .unwrap();

        // Add node with influence
        let add_node_req = AddNodeWithInfluenceRequest {
            schema_id: "test-schema-4".to_string(),
            node_id: 200,
            attributes: vec![Attribute {
                key: "name".to_string(),
                value: Some(attribute::Value::Text("leaf-node".to_string())),
            }],
            influence: 0.8,
            parent_node_id: 100,
            edge_id: 300,
            edge_attributes: vec![],
        };
        let response = client
            .add_node_with_influence(Request::new(add_node_req))
            .await
            .unwrap();
        assert_eq!(response.get_ref().node_id, 200);
        assert_eq!(response.get_ref().edge_id, 300);
    }

    #[tokio::test]
    async fn test_compute_state() {
        let mut client = setup_test_server().await;

        // Create schema
        let create_req = CreateSchemaRequest {
            schema_id: "test-schema-5".to_string(),
        };
        client
            .create_schema(Request::new(create_req))
            .await
            .unwrap();

        // Build a simple graph: leaf1 -> mid -> root, leaf2 -> mid
        // root = 3
        let root_id = 3u64;
        let mid_id = 2u64;
        let leaf1_id = 1u64;
        let leaf2_id = 4u64;

        // Add nodes
        client
            .add_node(Request::new(AddNodeRequest {
                schema_id: "test-schema-5".to_string(),
                node_id: root_id,
                attributes: vec![],
            }))
            .await
            .unwrap();

        client
            .add_node(Request::new(AddNodeRequest {
                schema_id: "test-schema-5".to_string(),
                node_id: mid_id,
                attributes: vec![],
            }))
            .await
            .unwrap();

        // Add leaves with influence
        client
            .add_node_with_influence(Request::new(AddNodeWithInfluenceRequest {
                schema_id: "test-schema-5".to_string(),
                node_id: leaf1_id,
                attributes: vec![],
                influence: 1.0,
                parent_node_id: mid_id,
                edge_id: 10,
                edge_attributes: vec![],
            }))
            .await
            .unwrap();

        client
            .add_node_with_influence(Request::new(AddNodeWithInfluenceRequest {
                schema_id: "test-schema-5".to_string(),
                node_id: leaf2_id,
                attributes: vec![],
                influence: 0.5,
                parent_node_id: mid_id,
                edge_id: 11,
                edge_attributes: vec![],
            }))
            .await
            .unwrap();

        // Add edge from mid to root
        client
            .add_edge(Request::new(AddEdgeRequest {
                schema_id: "test-schema-5".to_string(),
                from: mid_id,
                to: root_id,
                edge_id: 12,
                attributes: vec![],
            }))
            .await
            .unwrap();

        // Compute state
        let compute_req = ComputeStateRequest {
            schema_id: "test-schema-5".to_string(),
            root_node_id: root_id,
            seeds: vec![], // Seeds are already stored via add_node_with_influence
        };
        let response = client
            .compute_state(Request::new(compute_req))
            .await
            .unwrap();

        let states = &response.get_ref().states;
        assert_eq!(states.len(), 4); // root, mid, leaf1, leaf2

        // Find states by node_id
        let root_state = states.iter().find(|s| s.node_id == root_id).unwrap();
        let mid_state = states.iter().find(|s| s.node_id == mid_id).unwrap();
        let leaf1_state = states.iter().find(|s| s.node_id == leaf1_id).unwrap();
        let leaf2_state = states.iter().find(|s| s.node_id == leaf2_id).unwrap();

        // Leaves keep their predefined values
        assert!((leaf1_state.influence - 1.0).abs() < 1e-9);
        assert!((leaf2_state.influence - 0.5).abs() < 1e-9);

        // mid = avg(1.0, 0.5) = 0.75
        assert!((mid_state.influence - 0.75).abs() < 1e-9);

        // root = avg(0.75) = 0.75
        assert!((root_state.influence - 0.75).abs() < 1e-9);
    }

    #[tokio::test]
    async fn test_error_cases() {
        let mut client = setup_test_server().await;

        // Try to add node to non-existent schema
        let add_node_req = AddNodeRequest {
            schema_id: "non-existent".to_string(),
            node_id: 1,
            attributes: vec![],
        };
        let result = client.add_node(Request::new(add_node_req)).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);

        // Try to add edge with non-existent nodes
        let create_req = CreateSchemaRequest {
            schema_id: "test-schema-6".to_string(),
        };
        client
            .create_schema(Request::new(create_req))
            .await
            .unwrap();

        let add_edge_req = AddEdgeRequest {
            schema_id: "test-schema-6".to_string(),
            from: 999,
            to: 1000,
            edge_id: 1,
            attributes: vec![],
        };
        let result = client.add_edge(Request::new(add_edge_req)).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::FailedPrecondition);
    }
}
