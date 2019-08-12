#[macro_use]
extern crate log;

use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

pub trait StreamHandler where Self: Send + Sync {
    fn process(&self, stream: &mut TcpStream) -> std::io::Result<()>;
}

pub struct Server {
    listener: TcpListener,
    sleep_ms: u64,
    shutdown: Arc<AtomicBool>,
    join_handles: Vec<JoinHandle<()>>,
}

impl Server {
    pub fn new(listener: TcpListener, sleep_ms: u64) -> Server {
        Server {
            listener: listener,
            sleep_ms: sleep_ms,
            shutdown: Arc::new(AtomicBool::new(true)),
            join_handles: Vec::new(),
        }
    }

    pub fn start(&mut self, handler: 
            Arc<RwLock<Box<StreamHandler>>>) -> std::io::Result<()> {
        // set shutdown
        self.shutdown.store(false, Ordering::Relaxed);

        // clone variables
        let listener_clone = self.listener.try_clone()?;
        listener_clone.set_nonblocking(true)?;
        let sleep_duration = Duration::from_millis(self.sleep_ms);
        let shutdown_clone = self.shutdown.clone();

        let join_handle = std::thread::spawn(move || {
            for result in listener_clone.incoming() {

                let handler_clone = handler.clone();
                std::thread::spawn(move || {
                    process_stream_result(result,
                        handler_clone, sleep_duration);
                });

                // check if shutdown
                if shutdown_clone.load(Ordering::Relaxed) {
                    break;
                }
            }
        });

        self.join_handles.push(join_handle);
        Ok(())
    }

    pub fn start_threadpool(&mut self, thread_count: u8, handler:
            Arc<RwLock<Box<StreamHandler>>>) -> std::io::Result<()> {
        // set shutdown
        self.shutdown.store(false, Ordering::Relaxed);

        // start worker threads
        for _ in 0..thread_count {
            // clone variables
            let listener_clone = self.listener.try_clone()?;
            listener_clone.set_nonblocking(true)?;
            let sleep_duration = Duration::from_millis(self.sleep_ms);
            let shutdown_clone = self.shutdown.clone();
            let handler_clone = handler.clone();

            let join_handle = std::thread::spawn(move || {
                for result in listener_clone.incoming() {
                    process_stream_result(result,
                        handler_clone.clone(), sleep_duration);

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

fn process_stream_result(result: std::io::Result<TcpStream>,
        handler: Arc<RwLock<Box<StreamHandler>>>, 
        sleep_duration: Duration) {
    match result {
        Ok(mut stream) => {
            // process stream
            let handler = handler.read().unwrap();
            match handler.process(&mut stream) {
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
}

#[cfg(test)]
mod tests {
    #[test]
    fn cycle_server() {
        use std::net::{TcpListener, TcpStream};
        use std::sync::{Arc, RwLock};
        use super::{Server, StreamHandler};

        struct NullHandler { }
        impl StreamHandler for NullHandler {
            fn process(&self, _: &mut TcpStream) -> std::io::Result<()> {
                Ok(())
            }
        }

        // open server
        let listener = TcpListener::bind("127.0.0.1:15605")
            .expect("TcpListener bind");
        let mut server = Server::new(listener, 4);

        // start server
        let handler = NullHandler {};
        server.start(Arc::new(RwLock::new(Box::new(handler))))
            .expect("server start");
        //server.start_threadpool(8, Arc::new(RwLock::new(
        //    Box::new(handler)))).expect("server start");

        // stop server
        server.stop().expect("server stop");
    }
}
