/*
 * server.rs
 *
 * Implementation of EasyDB database server
 *
 * University of Toronto
 * 2019
 */

use std::net::TcpListener;
use std::net::TcpStream;
use std::io::Write;
use std::io;
use std::io::{Error, ErrorKind};
use packet::Command;
use packet::Response;
use packet::Network;
use schema::Table;
use database;
use database::Database;
use std::collections::HashMap;
use std::thread;
use std::sync::{Arc, Mutex};

fn single_threaded(listener: TcpListener, table_schema: Vec<Table>, verbose: bool)
{
    // /* 
    //  * you probably need to use table_schema somewhere here or in
    //  * Database::new 
    //  */

    // //Start by calling parse on the table_schema...
    // let size = table_schema.len();
    // let mut db = Database { schema : table_schema, tables : vec![HashMap::new(); size], r_ids : vec![0; size]};

    // for stream in listener.incoming() {
    //     let stream = stream.unwrap();
        
    //     if verbose {
    //         println!("Connected to {}", stream.peer_addr().unwrap());
    //     }
        
    //     match handle_connection(stream, &mut db) {
    //         Ok(()) => {
    //             if verbose {
    //                 println!("Disconnected.");
    //             }
    //         },
    //         Err(e) => eprintln!("Connection error: {:?}", e),
    //     };
    // }
}

// Harry + Shayshu
fn multi_threaded(listener: TcpListener, table_schema: Vec<Table>, verbose: bool)
{
    //Start by calling parse on the table_schema...
    let size = table_schema.len();
    let db = Arc::new(Mutex::new(Database { 
        schema : table_schema, 
        tables : vec![HashMap::new(); size], 
        r_ids : vec![0; size]
    }));
    let available_threads: Arc<Mutex<i32>> = Arc::new(Mutex::new(4));

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        
        if verbose {
            println!("Connected to {}", stream.peer_addr().unwrap());
        }
        
        let counter = available_threads.clone();
        let shared = db.clone();
        std::thread::spawn(move || {
            match handle_connection(stream, shared, counter) {
                Ok(()) => {
                    if verbose {
                        println!("Disconnected.");
                    }
                },
                Err(e) => eprintln!("Connection error: {:?}", e),
            };
        });
    } 
}

/* Sets up the TCP connection between the database client and server */
pub fn run_server(table_schema: Vec<Table>, ip_address: String, verbose: bool)
{
    let listener = match TcpListener::bind(ip_address) {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!("Could not start server: {}", e);
            return;
        },
    };
    
    println!("Listening: {:?}", listener);
    
    multi_threaded(listener, table_schema, verbose);
}

impl Network for TcpStream {}

/* Receive the request packet from ORM and send a response back */
fn handle_connection(mut stream: TcpStream, db: Arc<Mutex<Database>>, limit: Arc<Mutex<i32>>) 
    -> io::Result<()> 
{
    /* 
     * Tells the client that the connction to server is successful.
     * TODO: respond with SERVER_BUSY when attempting to accept more than
     *       4 simultaneous clients.
     */
    let mut available_threads = limit.lock().unwrap();
    let run_thread = *available_threads > 0;    
    println!("Available threads {}", *available_threads);
    if run_thread {
        *available_threads -= 1;
    }
    drop(available_threads);

    if run_thread {
            stream.respond(&Response::Connected)?;
        
            loop {
                let request = match stream.receive() {
                    Ok(request) => request,
                    Err(e) => {
                        /* respond error */
                        stream.respond(&Response::Error(Response::BAD_REQUEST))?;
                        return Err(e);
                    },
                };
                
                /* we disconnect with client upon receiving Exit */
                if let Command::Exit = request.command {
                    break;
                }
                
                /* Send back a response */
                let mut locked_db = &mut *db.lock().unwrap();
                let response = database::handle_request(request, &mut locked_db);
                
                
                stream.respond(&response)?;
                drop(locked_db);
                stream.flush()?;
            }
            
            let mut available_threads = limit.lock().unwrap();
            println!("Available threads {}", *available_threads);
            *available_threads += 1;
            drop(available_threads);

            Ok(())
    }
    else{
        stream.respond(&Response::Error(Response::SERVER_BUSY));
        return Err(Error::new(ErrorKind::Other, "oh no!"));
    }
}

