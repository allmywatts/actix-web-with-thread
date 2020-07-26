use actix_rt::System;
use actix_web::{dev::Server, middleware, web, App, HttpResponse, HttpServer, Responder};
use crossbeam::crossbeam_channel::bounded;
use crossbeam::crossbeam_channel::{Receiver, Sender};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::{thread, time::Duration};

// Accept payload on some route and
// send it to the channel if it's not full.
async fn index(tx: web::Data<Sender<String>>) -> impl Responder {
    match tx.try_send(String::from("Some message")) {
        Ok(_) => HttpResponse::Accepted().body(format!("Payload accepted.")),
        Err(_) => HttpResponse::ServiceUnavailable().body("Server busy."),
    }
}

// Run actix-web server in a thread
// https://github.com/actix/examples/tree/master/run-in-thread
fn run_app(tx: Sender<Server>, s: Sender<String>) -> std::io::Result<()> {
    let mut sys = System::new("test");

    let srv = HttpServer::new(move || {
        App::new()
            .data(s.clone())
            .wrap(middleware::Logger::default())
            .service(web::resource("/").to(index))
    })
    .disable_signals() // We handle signals on the main thread
    .workers(1)
    .bind("127.0.0.1:8080")?
    .run();

    let _ = tx.send(srv.clone());
    sys.block_on(srv)
}

// Process payloads
// The payload is in a compact binary format which
// blows up in size when converted to json
fn long_running_memory_intensive_part(msg: String) {
    thread::sleep(Duration::from_secs(5));
    println!("Processed: {}", msg);
}

fn main() {
    std::env::set_var("RUST_LOG", "actix_web=info,actix_server=info");
    env_logger::init();

    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    let (tx, rx) = bounded(1);
    // On average we process payloads faster than they arrive but
    // there might be occasional spikes. We want to process them
    // one at a time to avoid OoM
    let (s, r): (Sender<String>, Receiver<String>) = bounded(5);

    thread::spawn(move || {
        let _ = run_app(tx, s);
    });

    let srv = rx.recv().unwrap();

    while running.load(Ordering::SeqCst) {
        // Receive with timeout so we can still accept signals
        if let Ok(msg) = r.recv_timeout(Duration::from_secs(1)) {
            long_running_memory_intensive_part(msg);
        };
    }

    System::new("").block_on(srv.stop(true));
}
