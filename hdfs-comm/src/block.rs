use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use crossbeam_channel::{self, Receiver, RecvError, Sender};
use hdfs_protos::hadoop::hdfs::{PacketHeaderProto, PipelineAckProto};
use prost::Message;

use std;
use std::io::{BufWriter, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::thread::JoinHandle;
use std::time::SystemTime;

static FIRST_BIT_U8: u8 = 128;
static MASK_U8: u8 = 127;

pub struct BlockOutputStream {
    sender: Sender<Vec<u8>>,
    recv_acks_handle: Option<JoinHandle<()>>,
    send_chunks_handle: Option<JoinHandle<()>>,
    buffer: Vec<u8>,
    index: usize,
}

impl BlockOutputStream {
    pub fn new(stream: TcpStream, offset_in_block: i64,
            chunk_size_bytes: u32, chunks_per_packet: u8)
            -> BlockOutputStream {
        // initialize chunk channel
        let (sender, receiver): (Sender<Vec<u8>>, Receiver<Vec<u8>>)
            = crossbeam_channel::unbounded();

        // start recv_acks send_chunks threads
        let sequence_num = Arc::new(AtomicI64::new(0));
        let ack_sequence_num = Arc::new(AtomicI64::new(0));
        let last_packet = Arc::new(AtomicBool::new(false));

        let ra_stream = stream.try_clone().unwrap();
        let (ra_sequence_num, ra_ack_sequence_num) =
            (sequence_num.clone(), ack_sequence_num.clone());
        let ra_last_packet = last_packet.clone();

        let recv_acks_handle = std::thread::spawn(move || {
                if let Err(e) = recv_acks(ra_stream, ra_sequence_num,
                        ra_ack_sequence_num, ra_last_packet) {
                    warn!("failure in recv acks thread: {}", e);
                }
            });

        let send_chunks_handle = std::thread::spawn(move || {
                if let Err(e) = send_chunks(stream, receiver,
                        sequence_num, last_packet,
                        offset_in_block, chunk_size_bytes) {
                    warn!("failure in send chunks thread: {}", e);
                }
            });

        // calculate buffer length
        let buffer_length =
            chunk_size_bytes as usize * chunks_per_packet as usize;

        BlockOutputStream {
            sender: sender,
            recv_acks_handle: Some(recv_acks_handle),
            send_chunks_handle: Some(send_chunks_handle),
            buffer: vec![0u8; buffer_length],
            index: 0,
        }
    }

    pub fn close(mut self) {
        let close_start = SystemTime::now();

        if let None = self.recv_acks_handle {
            return;
        }

        // flush data, send empty packet, and drop sender
        let flush_start = SystemTime::now();
        if self.index != 0 {
            let _ = self.flush();
        }
        let _ = self.flush();
        let flush_duration = SystemTime::now()
            .duration_since(flush_start);

        let drop_start = SystemTime::now();
        drop(self.sender);
        let drop_duration = SystemTime::now()
            .duration_since(drop_start);

        // join recv acks and send chunks thread
        let chunk_start = SystemTime::now();
        let _ = self.send_chunks_handle.unwrap().join();
        self.send_chunks_handle = None;
        let chunk_duration = SystemTime::now()
            .duration_since(chunk_start);

        let ack_start = SystemTime::now();
        let _ = self.recv_acks_handle.unwrap().join();
        self.recv_acks_handle = None;
        let ack_duration = SystemTime::now()
            .duration_since(ack_start);

        let close_duration = SystemTime::now()
            .duration_since(close_start);
        println!("closed stream in {:?} flush:{:?} drop:{:?} ack:{:?} chunk:{:?}",
            close_duration, flush_duration, drop_duration, ack_duration, chunk_duration);
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

fn recv_acks(mut stream: TcpStream, sequence_num: Arc<AtomicI64>,
        ack_sequence_num: Arc<AtomicI64>, last_packet: Arc<AtomicBool>)
        -> std::io::Result<()> {
    // TODO - fix -> not working for some reason
    /*while !last_packet.load(Ordering::SeqCst) || 
            ack_sequence_num.load(Ordering::SeqCst) 
                < sequence_num.load(Ordering::SeqCst) {

        // calculate leb128 encoded op proto length
        let mut length = 0;
        for i in 0.. {
            println!("\tread byte {}", i);
            let byte = stream.read_u8()?;
            length += ((byte & MASK_U8) as u64) << (i * 7);

            if byte & FIRST_BIT_U8 != FIRST_BIT_U8 {
                break;
            }
        }
        println!("RECVing ACK: length {}", length);

        // read ack proto into buffer
        let mut buf = vec![0u8; length as usize];
        stream.read_exact(&mut buf)?;
        println!("RECV ACK with length {}", length);

        // decode PipelineAckProto
        let pipeline_ack_proto = PipelineAckProto::decode(buf)?;
        if pipeline_ack_proto.seqno > ack_sequence_num.load(Ordering::SeqCst) {
            ack_sequence_num.store(pipeline_ack_proto.seqno, Ordering::SeqCst);
        }
        println!("updated ACK {}", pipeline_ack_proto.seqno);
    }*/

    Ok(())    
}

fn send_chunks(mut stream: TcpStream, receiver: Receiver<Vec<u8>>,
        sequence_num: Arc<AtomicI64>, last_packet: Arc<AtomicBool>,
        mut offset_in_block: i64, chunk_size_bytes: u32)
        -> std::io::Result<()> {
    let mut stream = BufWriter::new(stream); // TODO - test

    for buf in receiver.iter() {
        let packet_start = SystemTime::now();

        // compute packet length
        let checksum_count =
            (buf.len() as f64 / chunk_size_bytes as f64).ceil() as i32;
        let packet_length = 4 + buf.len() as i32 + (checksum_count * 4);
        stream.write_i32::<BigEndian>(packet_length)?;

        // write packet header proto
        let header_start = SystemTime::now();
        let packet_header_proto = PacketHeaderProto {
                offset_in_block: offset_in_block,
                seqno: sequence_num.load(Ordering::SeqCst),
                last_packet_in_block: buf.len() == 0,
                data_len: buf.len() as i32,
                sync_block: Some(false),
            };

        stream.write_i16::<BigEndian>(packet_header_proto
            .encoded_len() as i16)?;

        let mut packet_header_buf = Vec::new();
        packet_header_proto.encode(&mut packet_header_buf)?;
        stream.write_all(&packet_header_buf)?;
        let header_duration = SystemTime::now()
            .duration_since(header_start);

        // write checksums
        let checksum_start = SystemTime::now();
        for i in 0..checksum_count {
            let start_index = (i * chunk_size_bytes as i32) as usize;
            let end_index = std::cmp::min((i + 1)
                * chunk_size_bytes as i32, buf.len() as i32) as usize;
            let checksum = crc::crc32::checksum_castagnoli(
                &buf[start_index..end_index]);

            stream.write_u32::<BigEndian>(checksum)?;
        }
        let checksum_duration = SystemTime::now()
            .duration_since(checksum_start);
        
        // write buf
        stream.write_all(&buf)?;
        stream.flush()?;

        let packet_duration = SystemTime::now()
            .duration_since(packet_start);
        println!("SEND chunk {} in {:?} header:{:?} checksum:{:?}",
            sequence_num.load(Ordering::SeqCst), packet_duration,
            header_duration, checksum_duration);

        // if last packet -> break from loop
        if buf.len() == 0 {
            last_packet.store(true, Ordering::SeqCst);
            break;
        }

        sequence_num.fetch_add(1, Ordering::SeqCst);
        offset_in_block += buf.len() as i64;
    }

    Ok(())
}

pub struct BlockInputStream {
    receiver: Receiver<Vec<u8>>,
    join_handle: Option<JoinHandle<()>>,
    buffer: Vec<u8>,
    start_index: usize,
    end_index: usize,
}

impl BlockInputStream {
    pub fn new(stream: TcpStream, chunk_size_bytes: u32,
            chunks_per_packet: u8) -> BlockInputStream {
        // initialize chunk channel
        let (sender, receiver): (Sender<Vec<u8>>, Receiver<Vec<u8>>)
            = crossbeam_channel::unbounded();

        // start chunk sending thread
        let join_handle = std::thread::spawn(move || {
                if let Err(e) = recv_chunks(stream, sender) {
                    warn!("recv chunks thread failed: {}", e);
                }
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

    fn fill(&mut self) -> std::io::Result<usize> {
        // retrieve next buffer
        let result = self.receiver.recv();
        if let Err(RecvError) = result {
            return Ok(0);
        }

        let buf = result.unwrap();

        // copy data
        self.buffer[0..buf.len()].copy_from_slice(&buf);

        // set indices
        self.start_index = 0;
        self.end_index = buf.len();

        Ok(buf.len())
    }
}

impl Read for BlockInputStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // if no data in buffer -> fill buffer
        if self.start_index == self.end_index {
            if self.fill()? == 0 {
                return Ok(0);
            }
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

fn recv_chunks(mut stream: TcpStream, sender: Sender<Vec<u8>>) -> std::io::Result<()> {
    let mut pa_proto = PipelineAckProto::default();
    let mut resp_buf = Vec::new();
    loop {
        // read packet length and packet header
        let packet_length = stream.read_i32::<BigEndian>()?;

        let packet_header_length = stream.read_i16::<BigEndian>()? as usize;
        let mut packet_header_buffer = vec![0u8; packet_header_length];
        stream.read_exact(&mut packet_header_buffer)?;
        let packet_header_proto =
            PacketHeaderProto::decode(packet_header_buffer)?;

        // read checksums
        let checksums_count = (packet_length - 4
            - packet_header_proto.data_len) / 4;
        for _ in 0..checksums_count {
            let _ = stream.read_u32::<BigEndian>()?;
        }

        // read data
        let mut buf = vec![0u8; packet_header_proto.data_len as usize];
        stream.read_exact(&mut buf)?;

        // TODO - validate checksums

        // send pipeline ack
        resp_buf.clear();
        pa_proto.seqno = packet_header_proto.seqno;
        pa_proto.encode_length_delimited(&mut resp_buf)?;
        stream.write_all(&resp_buf)?;

        if packet_header_proto.last_packet_in_block {
            break;
        }

        let _ = sender.send(buf); // TODO - handle error
    }

    drop(sender);
    Ok(())
}
