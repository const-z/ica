use walkdir::WalkDir;

fn main() {
    let proto_dir = "../proto";
    let mut proto_files: Vec<String> = Vec::new();
    for entry in WalkDir::new(proto_dir).into_iter().filter_map(|e| {
        if let Ok(e) = e {
            if e.file_type().is_file()
                && e.path().extension().is_some()
                && e.path().extension().unwrap() == "proto"
            {
                Some(e)
            } else {
                None
            }
        } else {
            None
        }
    }) {
        proto_files.push(entry.path().to_str().unwrap().to_string());
    }

    tonic_prost_build::configure()
        .build_server(false)
        .build_transport(false)
        .compile_protos(&proto_files, &[proto_dir.to_string()])
        .expect("failed to compile proto files");
}
