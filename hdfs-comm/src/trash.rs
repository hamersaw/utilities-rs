
    // should call BlockOutputStream.write_all(...);

        /*let mut bytes_written = 0;
        while bytes_written < buf.len() {
            // write buffer if full
            if self.index == self.buffer.len() {
                self.flush_buffer();
            }

            // copy into self.buffer
            let copy_length = std::cmp::min(self.buffer.len()
                - self.index, buf.len() - bytes_written);
            let src_slice = &buf[bytes_written..bytes_written + copy_length];
            self.buffer[self.index..self.index + copy_length]
                .copy_from_slice(src_slice);

            // increase buffer indices
            self.index += bytes_written;
            bytes_written += copy_length;
        }

        Ok(buf.len())*/

/*use std::io::{Read, Write};

pub struct BlockInputStream {
}

impl BlockInputStream {
    pub fn new(_stream: impl Read + Write) -> BlockInputStream {
        unimplemented!();
    }
}

impl Read for BlockInputStream {
    fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
        unimplemented!();
    }
}*/


        /*match receiver.recv() {
            Ok(chunk_buffer) => {
                if chunk_buffer.len() == 0 {
                    break;
                }
                // TODO - send down stream
            }
            Err(_e) => break,
        }*/


    let (chunk_sender, chunk_receiver) = unbounded();
    let mut chunk_writer = ChunkWriter::new(chunk_receiver);
    let mut block_output_stream = BlockOutputStream::new(chunk_sender);

    thread::spawn(move || {
        for packet in receiver.iter() {

        }
    });
