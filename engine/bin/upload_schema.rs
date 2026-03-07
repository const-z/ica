use ica_engine::schema_contracts::{schema_service_client::SchemaServiceClient, *};
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, BufReader},
};
use tonic::Request;

#[tokio::main]
async fn main() {
    let file_path = std::env::args().nth(1).expect("file_path required");

    let mut client = SchemaServiceClient::connect("http://localhost:50051")
        .await
        .expect("failed to connect to server http://localhost:50051");

    let file = File::open(file_path).await.expect("Cannot open file");
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    let (tx, rx) = tokio::sync::mpsc::channel(4);

    tokio::spawn(async move {
        while let Ok(Some(line)) = lines.next_line().await {
            let file_line = ImportSchemaRequest {
                chunk: line.to_string(),
            };

            if tx.send(file_line).await.is_err() {
                eprintln!("Failed to send line, server might have disconnected");
                break;
            }
        }
    });

    let request_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

    let response = client.import_schema(Request::new(request_stream)).await;

    println!("Response from server: {:?}", response.is_ok());
}

// cargo run --bin upload_schema .etc/schema-2.jsonl
