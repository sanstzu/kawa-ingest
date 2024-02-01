use std::collections::VecDeque;

use futures::future::FutureExt;
use log::{info, trace};
use rml_rtmp::chunk_io::Packet;
use tokio::io::{AsyncWriteExt, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::BoxError;

pub async fn connection_writer(
    connection_id: i32,
    mut stream: WriteHalf<TcpStream>,
    mut packets_to_send: mpsc::UnboundedReceiver<Packet>,
) -> Result<(), BoxError> {
    trace!("Connection {}: Connection writer starting", connection_id);
    const BACKLOG_THRESHOLD: usize = 100;
    let mut send_queue = VecDeque::new();

    loop {
        let packet = packets_to_send.recv().await;
        if packet.is_none() {
            trace!(
                "Connection {}: Connection writer received None, shutting down",
                connection_id
            );
            break;
        }

        send_queue.push_back(packet.unwrap());
        while let Some(Some(packet)) = packets_to_send.recv().now_or_never() {
            send_queue.push_back(packet);
        }

        let mut send_optional_packets = true;

        if send_queue.len() > BACKLOG_THRESHOLD {
            trace!(
                "Connection {}: Too many packets in backlog ({}), not sending optional packets",
                connection_id,
                send_queue.len()
            );
            send_optional_packets = false;
        }

        for packet in send_queue.drain(..) {
            if send_optional_packets || !packet.can_be_dropped {
                stream.write_all(packet.bytes.as_ref()).await?;
            }
        }
    }

    info!(
        "Connection {}: Connection writer shutting down",
        connection_id
    );

    Ok(())
}
