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

    // compile protos
    config.compile_protos(
        &["acl.proto",
            "ClientDatanodeProtocol.proto",
            "ClientNamenodeProtocol.proto",
            "DatanodeProtocol.proto",
            "datatransfer.proto",
            "encryption.proto",
            "HAServiceProtocol.proto",
            "hdfs.proto",
            "HdfsServer.proto",
            "inotify.proto",
            "IpcConnectionContext.proto",
            "NamenodeProtocol.proto",
            "ProtobufRpcEngine.proto",
            "RpcHeader.proto",
            "Security.proto",
            "xattr.proto"],
        &["protos/"]).unwrap();

    /*config.compile_protos(
	&["protos/HAServiceProtocol.proto",
            "protos/IpcConnectionContext.proto",
            "protos/ProtobufRpcEngine.proto",
            "protos/RpcHeader.proto",
            "protos/Security.proto"],
        &["protos/"]).unwrap();

    config.compile_protos(
	&["protos/acl.proto",
            "protos/ClientDatanodeProtocol.proto",
            "protos/ClientNamenodeProtocol.proto",
            "protos/datatransfer.proto",
            "protos/encryption.proto",
            "protos/hdfs.proto",
            "protos/HdfsServer.proto",
            "protos/inotify.proto",
            "protos/xattr.proto"],
        &["protos/"]).unwrap();

    config.compile_protos(&["protos/DatanodeProtocol.proto"],
        &["protos/"]).unwrap();

    config.compile_protos(&["protos/NamenodeProtocol.proto"],
        &["protos/"]).unwrap();*/
}
