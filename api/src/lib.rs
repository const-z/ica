pub mod schema {
    tonic::include_proto!("ica.schema");
}

mod service;

pub use service::SchemaServiceImpl;
