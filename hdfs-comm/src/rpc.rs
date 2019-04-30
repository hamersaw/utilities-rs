use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use hdfs_protos::hadoop::common::{IpcConnectionContextProto,
    RequestHeaderProto, RpcRequestHeaderProto,
    RpcResponseHeaderProto, RpcSaslProto};
use prost::{self, Message};

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::thread::JoinHandle;

type RpcFn = Box<Fn(&Vec<u8>, &mut Vec<u8>) + Send + Sync + 'static>;

pub struct Server {
    shutdown: Arc<AtomicBool>,
    listener: TcpListener,
    protocols: Arc<RwLock<HashMap<String, HashMap<String, RpcFn>>>>,
    thread_count: u8,
    join_handles: Vec<JoinHandle<()>>,
}

impl Server {
    pub fn new(listener: TcpListener, thread_count: u8) -> Server {
        Server {
            shutdown: Arc::new(AtomicBool::new(true)),
            listener: listener,
            protocols: Arc::new(RwLock::new(HashMap::new())),
            thread_count: thread_count,
            join_handles: Vec::new(),
        }
    }

    pub fn register(&mut self, protocol: &str, method: &str, function: RpcFn) {
        let mut protocols = self.protocols.write().unwrap();
        let functions = protocols.entry(protocol.to_owned())
            .or_insert(HashMap::new());
        functions.insert(method.to_owned(), function);
    }

    pub fn start(&mut self) -> std::io::Result<()> {
        // set shutdown
        self.shutdown.store(false, Ordering::Relaxed);

        // start worker threads
        for _ in 0..self.thread_count {
            // clone variables
            let listener_clone = self.listener.try_clone()?;
            listener_clone.set_nonblocking(true)?;
            let protocols_clone = self.protocols.clone();
            let shutdown_clone = self.shutdown.clone();

            let join_handle = std::thread::spawn(move || {
                for result in listener_clone.incoming() {
                    match result {
                        Ok(mut stream) => {
                            // process socket
                            let protocols = protocols_clone.read().unwrap();
                            if let Err(e) = process_stream(&mut stream, &protocols) {
                                println!("{}", e); // TODO - log
                            }
                        },
                        Err(ref e) if e.kind() !=
                                std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(50));
                        },
                        Err(ref e) if e.kind() !=
                                std::io::ErrorKind::WouldBlock => {
                            println!("{}", e); // TODO - log
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
        protocols: &HashMap<String, HashMap<String, RpcFn>>) -> std::io::Result<()> {
    // iterate over rpc requests
    let mut connection_header = vec![0u8; 7];
    loop {
        // read in connection header - TODO validate
        stream.read_exact(&mut connection_header)?;

        // read packet
        let packet_length = stream.read_u32::<BigEndian>()? as usize;
        let mut req_buf = vec![0u8; packet_length];
        stream.read_exact(&mut req_buf)?;

        // read RpcRequestHeaderProto
        let rpc_header_request = RpcRequestHeaderProto
            ::decode_length_delimited(&req_buf)?;

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
                // parse RpcSaslProto
                let rpc_sasl = RpcSaslProto
                    ::decode_length_delimited(&req_buf)?;

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
                // parse IpcConnectionContextProto
                let _ipc_connection_context = IpcConnectionContextProto
                    ::decode_length_delimited(&req_buf)?;

                // TODO - process IpcConnectionContextProto
            },
            call_id if call_id >= 0 => {
                // parse RequestHeaderProto
                let request_header = RequestHeaderProto
                    ::decode_length_delimited(&req_buf)?;

                // get function
                let functions_result = protocols.get(&request_header
                    .declaring_class_protocol_name);
                if let None = functions_result {
                    // TODO - protocol does not exist
                }

                let functions = functions_result.unwrap();
                let function_result =
                    functions.get(&request_header.method_name);
                if let None = function_result {
                    // TODO - method does not exist
                }

                let function = function_result.unwrap();

                // execute function
                function(&req_buf, &mut resp_buf);
            },
            _ => unimplemented!(),
        }

        // write response buffer
        stream.write_i32::<BigEndian>(resp_buf.len() as i32)?;
        stream.write_all(&resp_buf)?;

        // check rpc_header_request.rpc_op (1 -> continuation)
        if rpc_header_request.rpc_op.is_none() || 
                rpc_header_request.rpc_op.unwrap() != 1 {
            break;
        }
    }

    Ok(())
}
