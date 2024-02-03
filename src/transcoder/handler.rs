use std::collections::VecDeque;

use crate::service::{transcoder_client::TranscoderClient, StreamSessionData};
use log::{error, info};
use std::sync::mpsc::channel;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_stream::{wrappers::UnboundedReceiverStream, StreamExt};
use tonic::transport::Channel;

use crate::BoxError;

use super::message::Message;

pub async fn stream_handler(
    mut client: TranscoderClient<Channel>,

    session_id: u64,
    stream_rx: UnboundedReceiver<Message>,
) -> Result<(), BoxError> {
    let receiver_stream = UnboundedReceiverStream::new(stream_rx).map(move |message| {
        match message {
            Message::Audio(bytes) => {
                let payload = StreamSessionData {
                    session_id: session_id.clone(),
                    data: bytes.to_vec(),
                    r#type: 1, // Audio = 1
                };
                payload
            }
            Message::Video(bytes) => {
                let payload = StreamSessionData {
                    session_id: session_id.clone(),
                    data: bytes.to_vec(),
                    r#type: 2, // Video = 2
                };
                payload
            }
        }
    });

    match client
        .stream_session(receiver_stream)
        .await?
        .into_inner()
        .status
    {
        1 => info!("Stream session closed successfully"),
        15 => error!("Stream session closed with error"),
        _ => error!("Stream session closed with unknown status"),
    }

    Ok(())
}
