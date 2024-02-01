use connection::Connection;
use dotenv::dotenv;
use log::info;
use tokio::net::TcpListener;

mod connection;
mod stream_manager;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    dotenv().ok();
    env_logger::init();
    let manager_sender = stream_manager::start();
    let connection = TcpListener::bind("0.0.0.0:1935").await?;
    info!("Listening on port 1935");
    let mut counter = 0;

    loop {
        let (stream, connection_info) = connection.accept().await?;

        let connection = Connection::new(counter, manager_sender.clone());

        info!(
            "Connection {}: New connection from {}",
            counter, connection_info
        );

        tokio::spawn(async move {
            connection.start_handshake(stream).await;
        });

        counter += 1;
    }
}
