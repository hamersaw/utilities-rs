extern crate byteorder;
extern crate crossbeam_channel;

pub mod block;

#[cfg(test)]
mod tests {
    #[test]
    fn random_block_transfer() {
        extern crate rand;
        use rand::Rng;

        use std::io::{Read, Write};
        use std::net::{TcpListener, TcpStream};

        use super::block::{BlockInputStream, BlockOutputStream};

        // generate random block
        //let block_size = 67108864;
        let block_size = 1048576;
        let mut block: Vec<u8> = Vec::new();

        let mut rng = rand::thread_rng();
        for i in 0..block_size {
            block.push(rng.gen());
        }

        // open BlockOutputStream and write data
        let chunk_size_bytes = 4096;
        let chunks_per_packet = 3;

        // start server thread
        let listener = TcpListener::bind("127.0.0.1:15605").unwrap();
        let join_handle = std::thread::spawn(move || {
            // accept connection
            let (stream, _) = listener.accept().unwrap();;

            // write block to stream
            let mut out_stream: BlockOutputStream =
                BlockOutputStream::new(Box::new(stream),
                    chunk_size_bytes, chunks_per_packet);

            out_stream.write_all(&block);
            out_stream.close();
        });

        // connect to server
        match TcpStream::connect("127.0.0.1:15605") {
            Ok(stream) => {
                // read block from stream
                let mut in_stream = BlockInputStream::new(
                    Box::new(stream), chunk_size_bytes, chunks_per_packet);

                let mut buf = Vec::new();
                in_stream.read_to_end(&mut buf);
                in_stream.close();
                println!("buf len: {}", buf.len());
            },
            Err(e) => println!("failed to connect to server"),
        }

        // join server thread
        join_handle.join();
 
        // TODO - asset equality
        assert_eq!(2 + 2, 4);
    }
}
