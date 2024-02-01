use bytes::{Bytes, BytesMut};
use log::{info, trace};
use tokio::{
    io::{AsyncReadExt, ReadHalf},
    net::TcpStream,
    sync::mpsc,
};

use crate::BoxError;

pub async fn connection_reader(
    connection_id: i32,
    mut stream: ReadHalf<TcpStream>,
    manager: mpsc::UnboundedSender<Bytes>,
) -> Result<(), BoxError> {
    trace!("Connection {}: Connection reader starting", connection_id);
    let mut buffer = BytesMut::with_capacity(4096);

    loop {
        let read_result = stream.read_buf(&mut buffer).await?;
        if read_result == 0 {
            trace!(
                "Connection {}: Connection closed by remote host (read returned 0 bytes)",
                connection_id
            );
            break;
        }

        let bytes = buffer.split_off(read_result);
        if manager.send(buffer.freeze()).is_err() {
            trace!(
                "Connection {}: Manager receiver dropped, shutting down",
                connection_id
            );
            break;
        }

        buffer = bytes;
    }

    info!(
        "Connection {}: Connection reader shutting down",
        connection_id,
    );

    Ok(())
}
