pub mod schema_contracts {
    tonic::include_proto!("ica.schema.v1");
}

mod compute_fn;
mod mem_store;
mod repository;
mod service;

pub use service::SchemaServiceImpl;
