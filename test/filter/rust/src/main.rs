use serde::{Deserialize, Serialize};
use std::collections::LinkedList;
use std::env;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

mod tree;
use tree::{Customer, TreeNode};

#[derive(Debug, Serialize, Deserialize)]
struct Event {
    id: i32,
    date: i32,
    #[serde(rename = "type")]
    event_type: String,
}

fn main() -> io::Result<()> {
    // Environment variables with defaults
    let router_host = env::var("ROUTER_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let router_port: u16 = env::var("ROUTER_PORT")
        .unwrap_or_else(|_| "8000".to_string())
        .parse()
        .unwrap_or(8000);
    let num_customers: i32 = env::var("NUM_CUSTOMERS")
        .unwrap_or_else(|_| "100000".to_string())
        .parse()
        .unwrap_or(100000);
    let max_date: i32 = env::var("MAX_DATE")
        .unwrap_or_else(|_| "100000".to_string())
        .parse()
        .unwrap_or(100000);
    let max_span: i32 = env::var("MAX_SPAN")
        .unwrap_or_else(|_| "10".to_string())
        .parse()
        .unwrap_or(10);

    // Create interval tree
    let mut tree = TreeNode::new(1, max_date);

    // Generate customers
    let customers = generate_customers(num_customers, max_date, max_span);
    for customer in customers {
        tree.insert(customer);
    }

    let tree = Arc::new(tree);

    // Connect to router
    let router_addr = format!("{}:{}", router_host, router_port);
    let router_conn = TcpStream::connect(&router_addr)?;
    let router_conn = Arc::new(std::sync::Mutex::new(router_conn));

    // Start TCP server
    let listener = TcpListener::bind("0.0.0.0:8080")?;
    println!("Listening on 0.0.0.0:8080");

    // Accept connections
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let tree_clone = Arc::clone(&tree);
                let router_clone = Arc::clone(&router_conn);
                
                thread::spawn(move || {
                    if let Err(e) = handle_connection(stream, tree_clone, router_clone) {
                        eprintln!("Error handling connection: {}", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("Failed to accept connection: {}", e);
            }
        }
    }

    Ok(())
}

fn handle_connection(
    mut conn: TcpStream,
    tree: Arc<TreeNode>,
    router_conn: Arc<std::sync::Mutex<TcpStream>>,
) -> io::Result<()> {
    let start = Instant::now();
    let mut messages = 0;
    let mut buffer = LinkedList::new();

    let mut chunk = [0u8; 10240];
    
    loop {
        match conn.read(&mut chunk) {
            Ok(0) => break, // EOF
            Ok(n) => {
                // Add bytes to buffer list
                for i in 0..n {
                    buffer.push_back(chunk[i]);
                }
                
                let new_messages = process_buffer(&mut buffer, &tree, &router_conn)?;
                messages += new_messages;
            }
            Err(e) => {
                eprintln!("Read error: {}", e);
                break;
            }
        }
    }

    let duration = start.elapsed();
    println!("Duration (ms): {}", duration.as_millis());
    println!("Handled messages: {}", messages);

    Ok(())
}

fn process_buffer(
    buffer: &mut LinkedList<u8>,
    tree: &TreeNode,
    router_conn: &Arc<std::sync::Mutex<TcpStream>>,
) -> io::Result<usize> {
    let mut messages = 0;

    while buffer.len() >= 4 {
        // Read message length from first 4 bytes
        let mut msg_len_bytes = [0u8; 4];
        for i in 0..4 {
            if let Some(byte) = buffer.pop_front() {
                msg_len_bytes[i] = byte;
            } else {
                // Put bytes back if we don't have enough
                for j in (0..i).rev() {
                    buffer.push_front(msg_len_bytes[j]);
                }
                return Ok(messages);
            }
        }

        let msg_len = u32::from_be_bytes(msg_len_bytes) as usize;

        // Check if we have the complete message
        if buffer.len() < msg_len {
            // Put length bytes back
            for i in (0..4).rev() {
                buffer.push_front(msg_len_bytes[i]);
            }
            break;
        }

        // Extract message data
        let mut msg_data = Vec::with_capacity(msg_len);
        for _ in 0..msg_len {
            if let Some(byte) = buffer.pop_front() {
                msg_data.push(byte);
            }
        }

        // Process message
        match serde_json::from_slice::<Event>(&msg_data) {
            Ok(event) => {
                // Dispatch to interval tree
                tree.dispatch(event.date, &mut |customer_id| {
                    // Send to router
                    let data = (customer_id as u32).to_be_bytes();
                    if let Ok(mut router) = router_conn.lock() {
                        let _ = router.write_all(&data);
                    }
                });
                messages += 1;
            }
            Err(e) => {
                eprintln!("Failed to decode JSON: {}", e);
            }
        }
    }

    Ok(messages)
}

fn generate_customers(n: i32, max_date: i32, max_span: i32) -> Vec<Customer> {
    // Simple PRNG for consistent results
    let mut seed = 42u64;
    let mut customers = Vec::with_capacity(n as usize);

    for i in 0..n {
        // Simple linear congruential generator
        seed = seed.wrapping_mul(1103515245).wrapping_add(12345);
        let start = ((seed % (max_date - max_span) as u64) + 1) as i32;
        
        seed = seed.wrapping_mul(1103515245).wrapping_add(12345);
        let span = ((seed % max_span as u64) + 1) as i32;
        
        customers.push(Customer {
            id: i + 1,
            start,
            end: start + span,
        });
    }

    customers
}
