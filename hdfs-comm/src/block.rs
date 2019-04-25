use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use crossbeam_channel::{self, Receiver, Sender};
use hdfs_protos::hadoop::hdfs::PacketHeaderProto;
use prost::Message;

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
        receiver: Receiver<Vec<u8>>, chunk_size_bytes: u32) {
    let mut sequence_number = 0;
    let mut offset_in_block = 0;
    for buf in receiver.iter() {
        // TODO - error handling everywhere
        // compute packet length
        let checksum_count =
            (buf.len() as f64 / chunk_size_bytes as f64).ceil() as i32;
        let packet_length = 4 + buf.len() as i32 + (checksum_count * 4);
        stream.write_i32::<BigEndian>(packet_length);

        // write packet header proto
        let packet_header_proto = PacketHeaderProto {
                offset_in_block: offset_in_block,
                seqno: sequence_number,
                last_packet_in_block: buf.len() == 0,
                data_len: buf.len() as i32,
                sync_block: Some(false),
            };
        
        stream.write_i16::<BigEndian>(packet_header_proto
            .encoded_len() as i16);

        let mut packet_header_buf = Vec::new();
        packet_header_proto.encode(&mut packet_header_buf);
        stream.write_all(&packet_header_buf);

        // write checksums
        for i in 0..checksum_count {
            let start_index = (i * chunk_size_bytes as i32) as usize;
            let end_index = std::cmp::min((i + 1)
                * chunk_size_bytes as i32, buf.len() as i32) as usize;
            let checksum = crc::crc32::checksum_castagnoli(
                &buf[start_index..end_index]);

            stream.write_u32::<BigEndian>(checksum);
        }
        
        // write buf
        stream.write_all(&buf);
        stream.flush();

        // if last packet -> break from loop
        if buf.len() == 0 {
            break;
        }

        sequence_number += 1;
        offset_in_block += buf.len() as i64;
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
        // TODO - handle errors on all of this
        // read packet length and packet header
        let packet_length = stream.read_i32::<BigEndian>().unwrap();

        let packet_header_length =
            stream.read_i16::<BigEndian>().unwrap() as usize;
        let mut packet_header_buffer = vec![0u8; packet_header_length];
        stream.read_exact(&mut packet_header_buffer).unwrap();
        let packet_header_proto =
            PacketHeaderProto::decode(packet_header_buffer).unwrap();

        // read checksums
        let checksums_count = (packet_length - 4
            - packet_header_proto.data_len) / 4;
        for _ in 0..checksums_count {
            let _ = stream.read_u32::<BigEndian>().unwrap();
        }

        // read data
        let mut buf = vec![0u8; packet_header_proto.data_len as usize];
        stream.read_exact(&mut buf).unwrap();

        // TODO - validate checksums

        if packet_header_proto.last_packet_in_block {
            break;
        }

        let _ = sender.send(buf); // TODO - handle error
    }

    drop(sender);
}
