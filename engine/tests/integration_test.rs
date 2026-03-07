#[cfg(test)]
pub mod grpc_tests {

    use std::collections::HashMap;
    use std::time::Duration;
    use tokio::time::sleep;
    use tokio_stream::StreamExt;
    use tonic::Request;
    use tonic::transport::{Channel, Server};

    use ica_engine::SchemaServiceImpl;
    use ica_engine::schema_contracts::schema_service_client::SchemaServiceClient;
    use ica_engine::schema_contracts::schema_service_server::SchemaServiceServer;
    use ica_engine::schema_contracts::*;

    async fn setup_test_server() -> SchemaServiceClient<Channel> {
        let service = SchemaServiceImpl::new();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            Server::builder()
                .add_service(SchemaServiceServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        sleep(Duration::from_millis(100)).await;

        SchemaServiceClient::connect(format!("http://{}", addr))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_create_and_delete_schema() {
        let mut client = setup_test_server().await;

        let create_req = CreateSchemaRequest {
            schema_id: "test-schema-1".to_string(),
            attributes: vec![],
        };
        let response = client
            .create_schema(Request::new(create_req))
            .await
            .unwrap();
        assert_eq!(response.get_ref().schema_id, "test-schema-1");

        let create_req2 = CreateSchemaRequest {
            schema_id: "test-schema-1".to_string(),
            attributes: vec![],
        };
        let result = client.create_schema(Request::new(create_req2)).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::AlreadyExists);

        let delete_req = DeleteSchemaRequest {
            schema_id: "test-schema-1".to_string(),
        };
        let response = client
            .delete_schema(Request::new(delete_req))
            .await
            .unwrap();
        assert!(response.get_ref() == &DeleteSchemaResponse {});

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

        let create_req = CreateSchemaRequest {
            schema_id: "test-schema-2".to_string(),
            attributes: vec![],
        };
        client
            .create_schema(Request::new(create_req))
            .await
            .unwrap();

        let add_node_req = AddNodeRequest {
            schema_id: "test-schema-2".to_string(),
            node_id: "1".to_string(),
            attributes: vec![Attribute {
                key: "name".to_string(),
                value: Some(attribute::Value::Text("node-1".to_string())),
            }],
        };
        let response = client.add_node(Request::new(add_node_req)).await.unwrap();
        assert_eq!(response.get_ref().node_id, "1");

        let add_node_req2 = AddNodeRequest {
            schema_id: "test-schema-2".to_string(),
            node_id: "1".to_string(),
            attributes: vec![],
        };
        let result = client.add_node(Request::new(add_node_req2)).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::AlreadyExists);

        let remove_node_req = RemoveNodeRequest {
            schema_id: "test-schema-2".to_string(),
            node_id: "1".to_string(),
        };
        client
            .remove_node(Request::new(remove_node_req))
            .await
            .unwrap();

        let remove_node_req2 = RemoveNodeRequest {
            schema_id: "test-schema-2".to_string(),
            node_id: "1".to_string(),
        };
        let result = client.remove_node(Request::new(remove_node_req2)).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_add_and_remove_edges() {
        let mut client = setup_test_server().await;

        let create_req = CreateSchemaRequest {
            schema_id: "test-schema-3".to_string(),
            attributes: vec![],
        };
        client
            .create_schema(Request::new(create_req))
            .await
            .unwrap();

        client
            .add_node(Request::new(AddNodeRequest {
                schema_id: "test-schema-3".to_string(),
                node_id: "1".to_string(),
                attributes: vec![],
            }))
            .await
            .unwrap();

        client
            .add_node(Request::new(AddNodeRequest {
                schema_id: "test-schema-3".to_string(),
                node_id: "2".to_string(),
                attributes: vec![],
            }))
            .await
            .unwrap();

        let add_edge_req = AddEdgeRequest {
            schema_id: "test-schema-3".to_string(),
            from_id: "1".to_string(),
            to_id: "2".to_string(),
            edge_id: "10".to_string(),
            attributes: vec![Attribute {
                key: "weight".to_string(),
                value: Some(attribute::Value::Float(0.5)),
            }],
        };
        let response = client.add_edge(Request::new(add_edge_req)).await.unwrap();
        assert_eq!(response.get_ref().edge_id, "10");

        let add_edge_req2 = AddEdgeRequest {
            schema_id: "test-schema-3".to_string(),
            from_id: "1".to_string(),
            to_id: "2".to_string(),
            edge_id: "10".to_string(),
            attributes: vec![],
        };
        let result = client.add_edge(Request::new(add_edge_req2)).await;
        assert!(result.is_err());
        let errr = result.unwrap_err();
        assert_eq!(errr.code(), tonic::Code::AlreadyExists);

        let remove_edge_req = RemoveEdgeRequest {
            schema_id: "test-schema-3".to_string(),
            edge_id: "10".to_string(),
        };
        client
            .remove_edge(Request::new(remove_edge_req))
            .await
            .unwrap();

        let remove_edge_req2 = RemoveEdgeRequest {
            schema_id: "test-schema-3".to_string(),
            edge_id: "10".to_string(),
        };
        let result = client.remove_edge(Request::new(remove_edge_req2)).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_add_incident() {
        let mut client = setup_test_server().await;

        let create_req = CreateSchemaRequest {
            schema_id: "test-schema-4".to_string(),
            attributes: vec![],
        };
        client
            .create_schema(Request::new(create_req))
            .await
            .unwrap();

        client
            .add_node(Request::new(AddNodeRequest {
                schema_id: "test-schema-4".to_string(),
                node_id: "100".to_string(),
                attributes: vec![],
            }))
            .await
            .unwrap();

        let add_node_req = AddIncidentRequest {
            schema_id: "test-schema-4".to_string(),
            incident: Some(Incident {
                node_id: "200".to_string(),
                attributes: vec![Attribute {
                    key: "name".to_string(),
                    value: Some(attribute::Value::Text("leaf-node".to_string())),
                }],
                severity: 0.8,
            }),
            edge: Some(IncidentEdge {
                edge_id: "300".to_string(),
                to_id: "100".to_string(),
                attributes: vec![],
            }),
        };
        let response = client
            .add_incident(Request::new(add_node_req))
            .await
            .unwrap();
        assert_eq!(response.get_ref().node_id, "200");
        assert_eq!(response.get_ref().edge_id, "300");
    }

    #[tokio::test]
    async fn test_compute_state() {
        let mut client = setup_test_server().await;

        let create_req = CreateSchemaRequest {
            schema_id: "test-schema-5".to_string(),
            attributes: vec![],
        };
        client
            .create_schema(Request::new(create_req))
            .await
            .unwrap();

        let root_id = "root".to_string();
        let mid_id = "mid-1".to_string();
        let leaf1_id = "leaf-1".to_string();
        let leaf2_id = "leaf-2".to_string();

        client
            .add_node(Request::new(AddNodeRequest {
                schema_id: "test-schema-5".to_string(),
                node_id: root_id.clone(),
                attributes: vec![],
            }))
            .await
            .unwrap();

        client
            .add_node(Request::new(AddNodeRequest {
                schema_id: "test-schema-5".to_string(),
                node_id: mid_id.clone(),
                attributes: vec![],
            }))
            .await
            .unwrap();

        client
            .add_incident(Request::new(AddIncidentRequest {
                schema_id: "test-schema-5".to_string(),
                incident: Some(Incident {
                    node_id: leaf1_id.clone(),
                    attributes: vec![],
                    severity: 1.0,
                }),
                edge: Some(IncidentEdge {
                    edge_id: "10".to_string(),
                    to_id: mid_id.clone(),
                    attributes: vec![],
                }),
            }))
            .await
            .unwrap();

        client
            .add_incident(Request::new(AddIncidentRequest {
                schema_id: "test-schema-5".to_string(),
                incident: Some(Incident {
                    node_id: leaf2_id.clone(),
                    attributes: vec![],
                    severity: 0.5,
                }),
                edge: Some(IncidentEdge {
                    edge_id: "11".to_string(),
                    to_id: mid_id.clone(),
                    attributes: vec![],
                }),
            }))
            .await
            .unwrap();

        client
            .add_edge(Request::new(AddEdgeRequest {
                schema_id: "test-schema-5".to_string(),
                from_id: mid_id.clone(),
                to_id: root_id.clone(),
                edge_id: "12".to_string(),
                attributes: vec![],
            }))
            .await
            .unwrap();

        let compute_req = ComputeStateRequest {
            schema_id: "test-schema-5".to_string(),
            root_node_id: root_id.clone(),
        };

        let mut stream = client
            .compute_state(Request::new(compute_req))
            .await
            .unwrap()
            .into_inner();

        let mut states = HashMap::new();
        while let Some(response) = stream.next().await {
            match response {
                Ok(data) => {
                    states.insert(data.node_id, data.state);
                }
                Err(err) => panic!("Error: {err}"),
            }
        }

        assert_eq!(states.len(), 4);

        let root_state = states.get(&root_id).unwrap();
        let mid_state = states.get(&mid_id).unwrap();
        let leaf1_state = states.get(&leaf1_id).unwrap();
        let leaf2_state = states.get(&leaf2_id).unwrap();

        assert!((leaf1_state - 1.0).abs() < 1e-9);
        assert!((leaf2_state - 0.5).abs() < 1e-9);
        assert!((mid_state - 0.75).abs() < 1e-9);
        assert!((root_state - 0.75).abs() < 1e-9);
    }

    #[tokio::test]
    async fn test_error_cases() {
        let mut client = setup_test_server().await;

        let add_node_req = AddNodeRequest {
            schema_id: "non-existent".to_string(),
            node_id: "1".to_string(),
            attributes: vec![],
        };
        let result = client.add_node(Request::new(add_node_req)).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);

        let create_req = CreateSchemaRequest {
            schema_id: "test-schema-6".to_string(),
            attributes: vec![],
        };
        client
            .create_schema(Request::new(create_req))
            .await
            .unwrap();

        let add_edge_req = AddEdgeRequest {
            schema_id: "test-schema-6".to_string(),
            from_id: "999".to_string(),
            to_id: "1000".to_string(),
            edge_id: "1".to_string(),
            attributes: vec![],
        };
        let result = client.add_edge(Request::new(add_edge_req)).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_export_schema() {
        let mut client = setup_test_server().await;

        let create_req = CreateSchemaRequest {
            schema_id: "test-schema-5".to_string(),
            attributes: vec![],
        };
        client
            .create_schema(Request::new(create_req))
            .await
            .unwrap();

        let root_id = "root".to_string();
        let mid_id = "mid-1".to_string();
        let leaf1_id = "leaf-1".to_string();
        let leaf2_id = "leaf-2".to_string();

        client
            .add_node(Request::new(AddNodeRequest {
                schema_id: "test-schema-5".to_string(),
                node_id: root_id.clone(),
                attributes: vec![Attribute {
                    key: "attr_key".to_string(),
                    value: Some(attribute::Value::Text("This is text".to_string())),
                }],
            }))
            .await
            .unwrap();

        client
            .add_node(Request::new(AddNodeRequest {
                schema_id: "test-schema-5".to_string(),
                node_id: mid_id.clone(),
                attributes: vec![],
            }))
            .await
            .unwrap();

        client
            .add_incident(Request::new(AddIncidentRequest {
                schema_id: "test-schema-5".to_string(),
                incident: Some(Incident {
                    node_id: leaf1_id.clone(),
                    attributes: vec![],
                    severity: 1.0,
                }),
                edge: Some(IncidentEdge {
                    edge_id: "10".to_string(),
                    to_id: mid_id.clone(),
                    attributes: vec![Attribute {
                        key: "attr_key".to_string(),
                        value: Some(attribute::Value::Text("Edge attribute".to_string())),
                    }],
                }),
            }))
            .await
            .unwrap();

        client
            .add_incident(Request::new(AddIncidentRequest {
                schema_id: "test-schema-5".to_string(),
                incident: Some(Incident {
                    node_id: leaf2_id.clone(),
                    attributes: vec![],
                    severity: 0.5,
                }),
                edge: Some(IncidentEdge {
                    edge_id: "11".to_string(),
                    to_id: mid_id.clone(),
                    attributes: vec![],
                }),
            }))
            .await
            .unwrap();

        client
            .add_edge(Request::new(AddEdgeRequest {
                schema_id: "test-schema-5".to_string(),
                from_id: mid_id.clone(),
                to_id: root_id.clone(),
                edge_id: "12".to_string(),
                attributes: vec![],
            }))
            .await
            .unwrap();

        let export_req = ExportSchemaRequest {
            schema_id: "test-schema-5".to_string(),
        };

        let mut stream = client
            .export_schema(Request::new(export_req))
            .await
            .unwrap()
            .into_inner();

        let mut chunks = vec![];
        while let Some(response) = stream.next().await {
            match response {
                Ok(data) => {
                    print!("{}", data.chunk);
                    chunks.push(data.chunk);
                }
                Err(err) => panic!("Error: {err}"),
            }
        }
    }

    #[tokio::test]
    async fn test_import_schema() {
        let mut client = setup_test_server().await;
        let jsonl_data = vec![
            r#"{"Header":{"schema_id":"fg1KHvCamu","attrs":{"inner":{}}}}"#,
            r#"{"Node":{"id":"3o9ePvAi8N","attrs":{"inner":{}}}}"#,
            r#"{"Node":{"id":"fg1KHvCamu","attrs":{"inner":{}}}}"#,
            r#"{"Node":{"id":"TFFYUslmgi","attrs":{"inner":{}}}}"#,
            r#"{"Node":{"id":"AeTVFd3nPi","attrs":{"inner":{}}}}"#,
            r#"{"Node":{"id":"FMEX1yuIil","attrs":{"inner":{}}}}"#,
            r#"{"Node":{"id":"YvR1tFyftJ","attrs":{"inner":{}}}}"#,
            r#"{"Node":{"id":"Mb6bsADsG5","attrs":{"inner":{}}}}"#,
            r#"{"Node":{"id":"2kII0dfTpV","attrs":{"inner":{}}}}"#,
            r#"{"Node":{"id":"UO0QRttntR","attrs":{"inner":{}}}}"#,
            r#"{"Node":{"id":"8Amd4c72PV","attrs":{"inner":{}}}}"#,
            r#"{"Edge":{"id":"qx4H8SpEvh","from":"8Amd4c72PV","to":"UO0QRttntR","attrs":{"inner":{}}}}"#,
            r#"{"Edge":{"id":"hhrYTyOjuk","from":"UO0QRttntR","to":"Mb6bsADsG5","attrs":{"inner":{}}}}"#,
            r#"{"Edge":{"id":"ZiVRcC3COW","from":"FMEX1yuIil","to":"3o9ePvAi8N","attrs":{"inner":{}}}}"#,
            r#"{"Edge":{"id":"sFVcPhbZfl","from":"YvR1tFyftJ","to":"8Amd4c72PV","attrs":{"inner":{}}}}"#,
            r#"{"Edge":{"id":"mgHDcmq5gG","from":"AeTVFd3nPi","to":"fg1KHvCamu","attrs":{"inner":{}}}}"#,
            r#"{"Edge":{"id":"VMY2uVmjXt","from":"Mb6bsADsG5","to":"AeTVFd3nPi","attrs":{"inner":{}}}}"#,
            r#"{"Edge":{"id":"9SCdaGCcLq","from":"UO0QRttntR","to":"TFFYUslmgi","attrs":{"inner":{}}}}"#,
            r#"{"Edge":{"id":"1hcazWNjF6","from":"2kII0dfTpV","to":"8Amd4c72PV","attrs":{"inner":{}}}}"#,
            r#"{"Edge":{"id":"wnbWg2TsgM","from":"3o9ePvAi8N","to":"UO0QRttntR","attrs":{"inner":{}}}}"#,
            r#"{"Edge":{"id":"4xwG4ZUvz9","from":"YvR1tFyftJ","to":"3o9ePvAi8N","attrs":{"inner":{}}}}"#,
            r#"{"Edge":{"id":"HkKNpVGkzY","from":"TFFYUslmgi","to":"AeTVFd3nPi","attrs":{"inner":{}}}}"#,
        ];

        let (tx, rx) = tokio::sync::mpsc::channel(10);

        tokio::spawn(async move {
            for chunk in jsonl_data.iter() {
                if let Err(err) = tx
                    .send(ImportSchemaRequest {
                        chunk: chunk.to_string(),
                    })
                    .await
                {
                    panic!("{err}");
                }
            }
        });

        let request_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let request = tonic::Request::new(request_stream);

        let response = client.import_schema(request).await;

        assert!(response.is_ok())
    }

    #[tokio::test]
    async fn test_list_schemas() {
        let mut client = setup_test_server().await;
        let _ = client
            .create_schema(Request::new(CreateSchemaRequest {
                schema_id: "111".to_string(),
                attributes: vec![],
            }))
            .await;
        let _ = client
            .create_schema(Request::new(CreateSchemaRequest {
                schema_id: "222".to_string(),
                attributes: vec![],
            }))
            .await;
        let _ = client
            .create_schema(Request::new(CreateSchemaRequest {
                schema_id: "333".to_string(),
                attributes: vec![],
            }))
            .await;

        let schemas = client
            .list_schemas(Request::new(ListSchemasRequest {}))
            .await;

        assert!(schemas.is_ok());

        let schemas = schemas.unwrap().into_inner().schemas;
        assert_eq!(schemas.len(), 3);

        for id in ["111", "222", "333"] {
            assert!(
                schemas.iter().any(|s| s.schema_id == id),
                "{} not found in schemas",
                id
            );
        }
    }
}
