use ica_engine::SchemaServiceImpl;
use ica_engine::schema_contracts::schema_service_server::SchemaServiceServer;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:50051".parse()?;
    let service = SchemaServiceImpl::new();

    println!("ica-grpc listening on {}", addr);

    Server::builder()
        .add_service(SchemaServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
