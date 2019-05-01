use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use hdfs_protos::hadoop::common::{IpcConnectionContextProto, RequestHeaderProto,
    RpcRequestHeaderProto, RpcResponseHeaderProto, RpcSaslProto};
use prost::{self, Message};

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::thread::JoinHandle;

pub trait Protocol: Send + Sync {
    fn process(&self, method: &str, req_buf: &[u8], resp_buf: &mut Vec<u8>);
}

pub struct Server {
    listener: TcpListener,
    thread_count: u8,
    sleep_ms: u64,
    shutdown: Arc<AtomicBool>,
    protocols: Arc<RwLock<HashMap<String, Box<Protocol>>>>,
    join_handles: Vec<JoinHandle<()>>,
}

impl Server {
    pub fn new(listener: TcpListener, thread_count: u8, sleep_ms: u64) -> Server {
        Server {
            listener: listener,
            thread_count: thread_count,
            sleep_ms: sleep_ms,
            shutdown: Arc::new(AtomicBool::new(true)),
            protocols: Arc::new(RwLock::new(HashMap::new())),
            join_handles: Vec::new(),
        }
    }

    pub fn register(&mut self, protocol_name: &str, protocol: Box<Protocol>) {
        let mut protocols = self.protocols.write().unwrap();
        protocols.insert(protocol_name.to_owned(), protocol);
        info!("registered protocol: {}", protocol_name);
    }

    pub fn start(&mut self) -> std::io::Result<()> {
        // set shutdown
        self.shutdown.store(false, Ordering::Relaxed);

        // start worker threads
        for _ in 0..self.thread_count {
            // clone variables
            let listener_clone = self.listener.try_clone()?;
            listener_clone.set_nonblocking(true)?;
            let sleep_duration = Duration::from_millis(self.sleep_ms);
            let shutdown_clone = self.shutdown.clone();
            let protocols_clone = self.protocols.clone();

            let join_handle = std::thread::spawn(move || {
                for result in listener_clone.incoming() {
                    match result {
                        Ok(mut stream) => {
                            // process stream
                            let protocols = protocols_clone.read().unwrap();
                            match process_stream(&mut stream, &protocols) {
                                Err(ref e) if e.kind() != std::io
                                        ::ErrorKind::UnexpectedEof => {
                                    error!("failed to process stream {}", e);
                                },
                                _ => {},
                            }
                        },
                        Err(ref e) if e.kind() ==
                                std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(sleep_duration);
                        },
                        Err(ref e) if e.kind() !=
                                std::io::ErrorKind::WouldBlock => {
                            error!("failed to connect client: {}", e);
                        },
                        _ => {},
                    }

                    // check if shutdown
                    if shutdown_clone.load(Ordering::Relaxed) {
                        break;
                    }
                }
            });

            self.join_handles.push(join_handle);
        }

        Ok(())
    }

    pub fn stop(mut self) -> std::thread::Result<()> {
        if self.shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }

        // shutdown
        self.shutdown.store(true, Ordering::Relaxed);

        // join threads
        while self.join_handles.len() != 0 {
            let join_handle = self.join_handles.pop().unwrap();
            join_handle.join()?;
        }

        Ok(())
    }
}

fn process_stream(stream: &mut TcpStream,
        protocols: &HashMap<String, Box<Protocol>>) -> std::io::Result<()> {
    // iterate over rpc requests
    let mut connection_header = vec![0u8; 7];
 
    // read in connection header - TODO validate
    stream.read_exact(&mut connection_header)?;

    loop {
        // read packet
        let packet_length = stream.read_u32::<BigEndian>()? as usize;
        let mut req_buf = vec![0u8; packet_length];
        stream.read_exact(&mut req_buf)?;
        let mut req_buf_index = 0;

        // read RpcRequestHeaderProto
        debug!("parsing RpcRequestHeaderProto: {}", req_buf_index);
        let rpc_header_request = RpcRequestHeaderProto
            ::decode_length_delimited(&req_buf[req_buf_index..])?;
        req_buf_index += calculate_length(rpc_header_request.encoded_len());

        // create RpcResponseHeaderProto
        let mut rpc_header_response = RpcResponseHeaderProto::default();
        rpc_header_response.call_id = rpc_header_request.call_id as u32;
        rpc_header_response.status = 0; // set to success
        rpc_header_response.client_id = Some(rpc_header_request.client_id);

        let mut resp_buf = Vec::new();
        rpc_header_response.encode_length_delimited(&mut resp_buf)?;

        // match call id of request
        match rpc_header_request.call_id {
            -33 => {
                debug!("RpcSaslProto: {}", req_buf_index);
                // parse RpcSaslProto
                let rpc_sasl = RpcSaslProto
                    ::decode_length_delimited(&req_buf)?;
                req_buf_index += calculate_length(rpc_sasl.encoded_len());

                match rpc_sasl.state {
                    1 =>  {
                        // handle negotiate
                        let mut resp = RpcSaslProto::default();
                        resp.state = 0;

                        resp.encode_length_delimited(&mut resp_buf)?;
                    },
                    _ => unimplemented!(),
                }
            },
            -3 => {
                debug!("IpcConnectionContextProto: {}", req_buf_index);
                // parse IpcConnectionContextProto
                let ipc_connection_context = IpcConnectionContextProto
                    ::decode_length_delimited(&req_buf[req_buf_index..])?;
                req_buf_index += calculate_length(
                    ipc_connection_context.encoded_len());

                // TODO - process IpcConnectionContextProto
                //println!("{:?} {:?}", ipc_connection_context.user_info,
                //    ipc_connection_context.protocol);
                continue; // don't send response here
            },
            call_id if call_id >= 0 => {
                debug!("RequestHeaderProto: {}", req_buf_index);
                // parse RequestHeaderProto
                let request_header = RequestHeaderProto
                    ::decode_length_delimited(&req_buf[req_buf_index..])?;
                req_buf_index += calculate_length(request_header.encoded_len());

                // get protocol
                let protocol_result = protocols.get(&request_header
                    .declaring_class_protocol_name);
                if let None = protocol_result {
                    error!("protocol '{}' does not exist",
                        &request_header.declaring_class_protocol_name);
                }

                let protocol = protocol_result.unwrap();

                // execute method
                protocol.process(&request_header.method_name,
                    &req_buf[req_buf_index..], &mut resp_buf);
                // TODO - increment req_buf_index?
                //  will need to figure out how much data is read
            },
            _ => unimplemented!(),
        }

        // write response buffer
        debug!("writing resp {}", resp_buf.len());
        stream.write_i32::<BigEndian>(resp_buf.len() as i32)?;
        stream.write_all(&resp_buf)?;
        stream.flush();

        // check rpc_header_request.rpc_op (2 -> close)
        if rpc_header_request.rpc_op.is_none() || 
                rpc_header_request.rpc_op.unwrap() == 2 {
            break;
        }
    }

    Ok(())
}

fn calculate_length(length: usize) -> usize {
    length + prost::encoding::encoded_len_varint(length as u64)
}
