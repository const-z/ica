fn main() {
    let proto_files = &["../proto/schema.proto"];
    let proto_includes = &["../proto"];

    tonic_prost_build::configure()
        .compile_protos(proto_files, proto_includes)
        .expect("failed to compile proto files");
}

