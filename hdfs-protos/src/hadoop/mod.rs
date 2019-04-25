pub mod common {
    include!(concat!(env!("OUT_DIR"), "/hadoop.common.rs"));
}

pub mod datanode {
    include!(concat!(env!("OUT_DIR"), "/hadoop.hdfs.datanode.rs"));
}

pub mod hdfs {
    include!(concat!(env!("OUT_DIR"), "/hadoop.hdfs.rs"));
}

use hdfs::*;

pub mod namenode {
    include!(concat!(env!("OUT_DIR"), "/hadoop.hdfs.namenode.rs"));
}
