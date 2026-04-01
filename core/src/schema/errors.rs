#[derive(Debug)]
pub enum SchemaError {
    NodeExists(String),
    NodeNotFound(String),
    EdgeExists(String),
    EdgeNotFound(String),
    CycleDetected(String),
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaError::NodeExists(msg) => {
                write!(f, "SchemaError: NodeExists: node {} already exists", msg)
            }
            SchemaError::NodeNotFound(msg) => {
                write!(f, "SchemaError: NodeNotFound: node {} not found", msg)
            }
            SchemaError::EdgeExists(msg) => {
                write!(f, "SchemaError: EdgeExists: edge {} already exists", msg)
            }
            SchemaError::EdgeNotFound(msg) => {
                write!(f, "SchemaError: EdgeNotFound: edge {} not found", msg)
            }
            SchemaError::CycleDetected(msg) => write!(f, "SchemaError: CycleDetected: {}", msg),
        }
    }
}
