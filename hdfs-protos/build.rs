use prost_build::Config;

use std;
use std::path::PathBuf;

fn main() {
    // initialize config
    let mut config = Config::new();

    // create output directory
    let output_directory = PathBuf::from(std::env::var("OUT_DIR")
        .expect("OUT_DIR environment variable not set"));
    std::fs::create_dir_all(&output_directory)
        .expect("failed to create prefix directory");
    config.out_dir(&output_directory);

    // configure extern paths in protobuf
    config.extern_path(".hadoop.hdfs", "crate::hdfs");

    // compile protos
    config.compile_protos(
	&["src/acl.proto",
            "src/ClientDatanodeProtocol.proto",
            "src/ClientNamenodeProtocol.proto",
            "src/DatanodeProtocol.proto",
            "src/datatransfer.proto",
            "src/encryption.proto",
            "src/HAServiceProtocol.proto",
            "src/hdfs.proto",
            "src/HdfsServer.proto",
            "src/inotify.proto",
            "src/IpcConnectionContext.proto",
            "src/NamenodeProtocol.proto",
            "src/ProtobufRpcEngine.proto",
            "src/RpcHeader.proto",
            "src/Security.proto",
            "src/xattr.proto"],
        &["src/"]).unwrap();
}
