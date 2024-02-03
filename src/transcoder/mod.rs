use crate::service::{transcoder_client::TranscoderClient, InitializeSessionRequest};

use crate::BoxError;
use bytes::Bytes;
use log::error;
use tokio::sync::mpsc::{self, UnboundedSender};

mod handler;
mod message;
mod state;

use message::Message;
use state::State;

#[derive(Clone)]
struct Session {
    id: u64,
    stream_tx: UnboundedSender<Message>,
}

struct Transcoder {
    state: State,
    stream_key: String,
    connection_id: i32,
    session: Option<Session>,
}

impl Transcoder {
    fn new(stream_key: String, connection_id: i32) -> Self {
        Self {
            state: State::Uninitialized,
            stream_key,
            connection_id,
            session: None,
        }
    }

    pub async fn initialize(&mut self, publish_url: String) -> Result<(), BoxError> {
        const TRANSCODER_SERVICE_URL: &str = "http://[::1]:50051"; // TODO: Use env file to set service URL
        let mut client = TranscoderClient::connect(TRANSCODER_SERVICE_URL).await?;

        // Initialize session

        let (tx, rx) = mpsc::unbounded_channel();
        let res = client
            .initialize_session(InitializeSessionRequest {
                id: self.connection_id.clone(),
                publish_url,
            })
            .await?
            .into_inner();

        match res.status {
            1 => {
                self.session = Some(Session {
                    id: res.session_id,
                    stream_tx: tx,
                });
                self.state = State::Waiting;
            }
            15 => {
                error!("Error initializing session");
                return Ok(());
            }
            _ => {
                error!("Unknown error initializing session");
                return Ok(());
            }
        }

        let session_id = self.session.clone().unwrap().id;
        tokio::spawn(async move {
            if handler::stream_handler(client, session_id, rx)
                .await
                .is_err()
            {
                error!("Error handling stream");
            }
        });

        self.state = State::Streaming;

        Ok(())
    }
}
