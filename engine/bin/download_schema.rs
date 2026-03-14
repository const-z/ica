use ica_engine::schema_contracts::{schema_service_client::SchemaServiceClient, *};
use tokio_stream::StreamExt;
use tonic::Request;

#[tokio::main]
async fn main() {
    let schema_id = std::env::args().nth(1).expect("schema_id required");

    let mut client = SchemaServiceClient::connect("http://localhost:50051")
        .await
        .expect("failed to connect to server http://localhost:50051");

    let export_req = ExportSchemaRequest { schema_id };

    let mut stream = client
        .export_schema(Request::new(export_req))
        .await
        .unwrap()
        .into_inner();

    let mut chunks = vec![];
    while let Some(response) = stream.next().await {
        match response {
            Ok(data) => {
                println!("{}", data.chunk);
                chunks.push(data.chunk);
            }
            Err(err) => panic!("Error: {err}"),
        }
    }
}

//  cargo run --bin download_schema iQo9qIsZoc
