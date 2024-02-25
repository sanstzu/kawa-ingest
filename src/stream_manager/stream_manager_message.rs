use bytes::Bytes;
use rml_rtmp::time::RtmpTimestamp;
use tokio::sync::mpsc;

use super::connection_message::ConnectionMessage;

pub enum StreamManagerMessage {
    NewConnection {
        connection_id: i32,
        sender: mpsc::UnboundedSender<ConnectionMessage>,
        disconnection: mpsc::UnboundedReceiver<()>,
    },
    PublishRequest {
        connection_id: i32,
        stream_key: String,
        rtmp_app: String,
        request_id: u32,
    },

    NewVideoData {
        sending_connection_id: i32,
        timestamp: RtmpTimestamp,
        data: Bytes,
    },

    NewAudioData {
        sending_connection_id: i32,
        timestamp: RtmpTimestamp,
        data: Bytes,
    },

    PublishFinished {
        connection_id: i32,
    },
}
