use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use crossbeam_channel::{self, Receiver, Sender};

use std;
use std::io::{ErrorKind, Read, Write};
use std::thread::JoinHandle;

pub struct BlockOutputStream {
    sender: Sender<Vec<u8>>,
    join_handle: Option<JoinHandle<()>>,
    buffer: Vec<u8>,
    index: usize,
}

impl BlockOutputStream {
    pub fn new(stream: Box<Write + Send>, chunk_size_bytes: u32,
            chunks_per_packet: u8) -> BlockOutputStream {
        // initialize chunk channel
        let (sender, receiver): (Sender<Vec<u8>>, Receiver<Vec<u8>>)
            = crossbeam_channel::unbounded();

        let join_handle = std::thread::spawn(move || {
                send_chunks(stream, receiver, chunk_size_bytes);
            });

        // calculate buffer length
        let buffer_length =
            chunk_size_bytes as usize * chunks_per_packet as usize;

        BlockOutputStream {
            sender: sender,
            join_handle: Some(join_handle),
            buffer: vec![0u8; buffer_length],
            index: 0,
        }
    }

    pub fn close(mut self) {
        if let None = self.join_handle {
            return;
        }

        // flush data, send empty packet, and drop sender
        if self.index != 0 {
            let _ = self.flush();
        }
        let _ = self.flush();
        drop(self.sender);

        // join sender thread
        let _ = self.join_handle.unwrap().join();
        self.join_handle = None;
    }
}

impl Write for BlockOutputStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // copy buf into self.buffer
        let copy_length =
            std::cmp::min(self.buffer.len() - self.index, buf.len());
        self.buffer[self.index..self.index + copy_length]
            .copy_from_slice(&buf[0..copy_length]);

        // increase buffer index and flush if necessary
        self.index += copy_length;
        if self.index == self.buffer.len() {
            self.flush()?;
        }

        Ok(copy_length)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // flush buffer
        let mut buf = vec![0u8; self.index];
        buf[0..self.index].copy_from_slice(&self.buffer[0..self.index]);
        let _ = self.sender.send(buf); // TODO - handle result

        // reset index
        self.index = 0;
        Ok(())
    }
}

fn send_chunks(mut stream: impl Write + Send,
        receiver: Receiver<Vec<u8>>, _chunk_size_bytes: u32) {
    for buf in receiver.iter() {
        // compute packet length
        //let checksum_count =
        //    (buf.len() as f64 / chunk_size_bytes as f64).ceil() as u32;
        //let packet_length = 4 + buf.len() as u32 + (checksum_count * 4);
        //stream.write_int(packet_length);

        // TODO - write packet header proto

        // TODO - write checksums
        
        // TODO - write buf

        // TODO - tmp
        stream.write_u32::<BigEndian>(buf.len() as u32).unwrap();
        stream.write_all(&buf).unwrap();
        if buf.len() == 0 {
            stream.write_u8(1).unwrap();
        } else {
            stream.write_u8(0).unwrap();
        }

        // if last packet -> break from loop
        if buf.len() == 0 {
            break;
        }
    }
}

pub struct BlockInputStream {
    receiver: Receiver<Vec<u8>>,
    join_handle: Option<JoinHandle<()>>,
    buffer: Vec<u8>,
    start_index: usize,
    end_index: usize,
}

impl BlockInputStream {
    pub fn new(stream: Box<Read + Send>, chunk_size_bytes: u32,
            chunks_per_packet: u8) -> BlockInputStream {
        // initialize chunk channel
        let (sender, receiver): (Sender<Vec<u8>>, Receiver<Vec<u8>>)
            = crossbeam_channel::unbounded();

        // start chunk sending thread
        let join_handle = std::thread::spawn(move || {
                recv_chunks(stream, sender);
            });

        // calculate buffer length
        let buffer_length =
            chunk_size_bytes as usize * chunks_per_packet as usize;

        // calculate buffer length
        BlockInputStream {
            receiver: receiver,
            join_handle: Some(join_handle),
            buffer: vec![0u8; buffer_length],
            start_index: 0,
            end_index: 0,
        }
    }

    pub fn close(mut self) {
        if let None = self.join_handle {
            return;
        }

        // join sender thread
        let _ = self.join_handle.unwrap().join();
        self.join_handle = None;
    }

    fn fill(&mut self) -> std::io::Result<()> {
        // retrieve next buffer
        let result = self.receiver.recv();
        if let Err(e) = result {
            return Err(std::io::Error::new(ErrorKind::UnexpectedEof, e));
        }

        let buf = result.unwrap();

        // copy data
        self.buffer[0..buf.len()].copy_from_slice(&buf);

        // set indices
        self.start_index = 0;
        self.end_index = buf.len();

        Ok(())
    }
}

impl Read for BlockInputStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // if no data in buffer -> fill buffer
        if self.start_index == self.end_index {
            self.fill()?;
        }

        // copy data
        let copy_length =
            std::cmp::min(self.end_index - self.start_index, buf.len());
        let src_slice =
            &self.buffer[self.start_index..self.start_index + copy_length];
        buf[0..copy_length].copy_from_slice(src_slice);

        // increase indices
        self.start_index += copy_length;
 
        Ok(copy_length)
    }
}

fn recv_chunks(mut stream: impl Read, sender: Sender<Vec<u8>>) {
    loop {
        // TODO - tmp
        let packet_length = stream.read_u32::<BigEndian>().unwrap() as usize;
        let mut buf = vec![0u8; packet_length];
        stream.read_exact(&mut buf[0..packet_length]).unwrap();
        let last_packet = stream.read_u8().unwrap();

        if last_packet == 1 {
            break;
        }

        let _ = sender.send(buf); // TODO - handle error
    }

    drop(sender);
}
