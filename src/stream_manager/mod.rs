pub mod connection_message;
mod publish_details;
pub mod stream_manager_message;

use bytes::Bytes;
use futures::future::select_all;
use futures::future::BoxFuture;
use log::error;
use log::info;

use log::warn;
use rml_rtmp::time::RtmpTimestamp;
use std::collections::hash_map::HashMap;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub use connection_message::ConnectionMessage;
use futures::FutureExt;
pub use publish_details::PublishDetails;
pub use stream_manager_message::StreamManagerMessage;

use crate::extractor::extract_audio;
use crate::extractor::extract_video_frame;
use crate::extractor::extract_video_header;
use crate::session_manager;
use crate::transcoder::{Message as TranscoderMessage, TranscoderManager};

pub fn start() -> mpsc::UnboundedSender<StreamManagerMessage> {
    let (sender, receiver) = mpsc::unbounded_channel();

    let manager = StreamManager::new();
    tokio::spawn(async move { manager.run(receiver).await });

    sender
}

enum FutureResult {
    Disconnection {
        connection_id: i32,
    },
    MessageReceived {
        receiver: UnboundedReceiver<StreamManagerMessage>,
        message: Option<StreamManagerMessage>,
    },
}

struct StreamManager<'a> {
    publish_details: HashMap<String, PublishDetails>,
    sender_by_connection_id: HashMap<i32, mpsc::UnboundedSender<ConnectionMessage>>,
    key_by_connection_id: HashMap<i32, String>,
    transcoder_by_connection_id: HashMap<i32, TranscoderManager>,
    new_disconnect_futures: Vec<BoxFuture<'a, FutureResult>>,
}

impl<'a> StreamManager<'a> {
    fn new() -> Self {
        StreamManager {
            publish_details: HashMap::new(),
            sender_by_connection_id: HashMap::new(),
            key_by_connection_id: HashMap::new(),
            transcoder_by_connection_id: HashMap::new(),
            new_disconnect_futures: Vec::new(),
        }
    }

    fn cleanup_connection(&mut self, connection_id: i32) {
        info!("Stream manager is removing connection {}", connection_id);

        self.sender_by_connection_id.remove(&connection_id);
        if let Some(key) = self.key_by_connection_id.remove(&connection_id) {
            if let Some(details) = self.publish_details.get_mut(&key) {
                if details.connection_id == connection_id {
                    self.publish_details.remove(&key);
                }
            }
        }

        self.transcoder_by_connection_id.remove(&connection_id);
    }

    async fn run(mut self, receiver: UnboundedReceiver<StreamManagerMessage>) {
        async fn new_receiver_future(
            mut receiver: UnboundedReceiver<StreamManagerMessage>,
        ) -> FutureResult {
            let result = receiver.recv().await;
            FutureResult::MessageReceived {
                receiver,
                message: result,
            }
        }

        let mut futures = select_all(vec![new_receiver_future(receiver).boxed()]);

        loop {
            let (result, _index, remaining_futures) = futures.await;
            let mut new_futures = Vec::from(remaining_futures);

            match result {
                FutureResult::MessageReceived { receiver, message } => {
                    match message {
                        Some(message) => self.handle_message(message).await,
                        None => return, // receiver has no more senders
                    }

                    new_futures.push(new_receiver_future(receiver).boxed());
                }

                FutureResult::Disconnection { connection_id } => {
                    self.cleanup_connection(connection_id);
                }
            }

            for future in self.new_disconnect_futures.drain(..) {
                new_futures.push(future);
            }

            futures = select_all(new_futures);
        }
    }

    async fn handle_message(&mut self, message: StreamManagerMessage) {
        match message {
            StreamManagerMessage::NewConnection {
                connection_id,
                sender,
                disconnection,
            } => {
                self.handle_new_connection(connection_id, sender, disconnection);
            }

            StreamManagerMessage::PublishRequest {
                connection_id,
                request_id,
                rtmp_app,
                stream_key,
            } => {
                self.handle_publish_request(connection_id, request_id, rtmp_app, stream_key)
                    .await;
            }

            StreamManagerMessage::PublishFinished { connection_id } => {
                self.handle_publish_finished(connection_id);
            }

            StreamManagerMessage::NewAudioData {
                sending_connection_id,
                timestamp,
                data,
            } => {
                self.handle_new_audio_data(sending_connection_id, timestamp, data);
            }

            StreamManagerMessage::NewVideoData {
                sending_connection_id,
                timestamp,
                data,
            } => {
                self.handle_new_video_data(sending_connection_id, timestamp, data);
            }
        }
    }

    fn handle_new_connection(
        &mut self,
        connection_id: i32,
        sender: UnboundedSender<ConnectionMessage>,
        disconnection: UnboundedReceiver<()>,
    ) {
        self.sender_by_connection_id.insert(connection_id, sender);
        self.new_disconnect_futures
            .push(wait_for_client_disconnection(connection_id, disconnection).boxed());
    }

    async fn handle_publish_request(
        &mut self,
        connection_id: i32,
        request_id: u32,
        rtmp_app: String,
        stream_key: String,
    ) {
        let sender = match self.sender_by_connection_id.get(&connection_id) {
            None => {
                info!(
                    "Connection {} is requesting to publish but not registered",
                    connection_id
                );
                return;
            }

            Some(x) => x,
        };

        if self.key_by_connection_id.contains_key(&connection_id) {
            warn!(
                "Connection {} is requesting to publish but is already publishing",
                connection_id
            );
            if sender
                .send(ConnectionMessage::RequestDenied { request_id })
                .is_err()
            {
                self.cleanup_connection(connection_id);
            }

            return;
        }

        let key = format!("{}/{}", rtmp_app, stream_key);
        match self.publish_details.get(&key) {
            None => (),
            Some(details) => {
                warn!("Publish request by connection {} for stream '{}' rejected as it's already being published by connection {}",
                         connection_id, key, details.connection_id);

                if sender
                    .send(ConnectionMessage::RequestDenied { request_id })
                    .is_err()
                {
                    self.cleanup_connection(connection_id);
                }

                return;
            }
        }

        self.key_by_connection_id.insert(connection_id, key.clone());
        self.publish_details.insert(
            key.clone(),
            PublishDetails {
                video_sequence_header: None,
                audio_sequence_header: None,
                metadata: None,
                connection_id,
            },
        );

        let publish_path = match session_manager::get_publish_url(&stream_key).await {
            Err(x) => {
                error!("Error getting publish url: {}", x);
                return;
            }
            Ok(None) => {
                info!(
                    "Connection {} is requesting to publish but stream key is not found",
                    connection_id
                );
                if sender
                    .send(ConnectionMessage::RequestDenied { request_id })
                    .is_err()
                {
                    self.cleanup_connection(connection_id);
                }
                return;
            }
            Ok(Some(x)) => match x.is_empty() {
                true => {
                    info!(
                        "Connection {} is requesting to publish but stream key is not found",
                        connection_id
                    );

                    if sender
                        .send(ConnectionMessage::RequestDenied { request_id })
                        .is_err()
                    {
                        self.cleanup_connection(connection_id);
                    }
                    return;
                }
                false => x,
            },
        };

        // Initialize Transcoder

        let mut transcoder = TranscoderManager::new(stream_key.clone(), connection_id.clone());

        if transcoder.initialize(publish_path).await.is_err() {
            error!(
                "Failed to initialize transcoder for connection {}",
                connection_id
            );
            if sender
                .send(ConnectionMessage::RequestDenied { request_id })
                .is_err()
            {
                self.cleanup_connection(connection_id);
            }
            return;
        }

        info!("Connection {} is requesting to publish 5", connection_id);

        self.transcoder_by_connection_id
            .insert(connection_id, transcoder);

        if sender
            .send(ConnectionMessage::RequestAccepted { request_id })
            .is_err()
        {
            self.cleanup_connection(connection_id);
        }
    }

    fn handle_publish_finished(&mut self, connection_id: i32) {
        self.cleanup_connection(connection_id);
    }

    fn handle_new_audio_data(
        &mut self,
        sending_connection_id: i32,
        _timestamp: RtmpTimestamp,
        data: Bytes,
    ) {
        let key = match self.key_by_connection_id.get(&sending_connection_id) {
            Some(x) => x,
            None => return,
        };

        let details = match self.publish_details.get_mut(key) {
            Some(x) => x,
            None => return,
        };

        if is_audio_sequence_header(&data) {
            // TODO: Use the header information
            details.audio_sequence_header = Some(data.clone());
        } else {
            let transcoder = match self.transcoder_by_connection_id.get(&sending_connection_id) {
                Some(x) => x,
                None => return,
            };

            let extracted_data = extract_audio(data.clone());

            let message = TranscoderMessage::Audio(extracted_data);

            if transcoder.handle_message(message).is_err() {
                self.cleanup_connection(sending_connection_id);
            }
        }
    }

    fn handle_new_video_data(
        &mut self,
        sending_connection_id: i32,
        _timestamp: RtmpTimestamp,
        data: Bytes,
    ) {
        let key = match self.key_by_connection_id.get(&sending_connection_id) {
            Some(x) => x,
            None => return,
        };

        let details = match self.publish_details.get_mut(key) {
            Some(x) => x,
            None => return,
        };

        let mut _can_be_dropped = true;
        let mut _is_key_frame = false;
        if is_video_sequence_header(&data) {
            details.video_sequence_header = Some(data.clone());
            _can_be_dropped = false;
        } else if is_video_keyframe(&data) {
            _can_be_dropped = false;
            _is_key_frame = true;
        }

        let transcoder = match self.transcoder_by_connection_id.get(&sending_connection_id) {
            Some(x) => x,
            None => return,
        };

        let extracted_data = match is_video_sequence_header(&data) {
            true => extract_video_header(data.clone()),
            false => extract_video_frame(data.clone()),
        };

        let message = TranscoderMessage::Video(extracted_data);

        if transcoder.handle_message(message).is_err() {
            self.cleanup_connection(sending_connection_id);
        }
    }
}

fn is_video_sequence_header(data: &Bytes) -> bool {
    // This is assuming h264.
    return data.len() >= 2 && data[0] == 0x17 && data[1] == 0x00;
}

fn is_audio_sequence_header(data: &Bytes) -> bool {
    // This is assuming aac
    return data.len() >= 2 && data[0] == 0xaf && data[1] == 0x00;
}

fn is_video_keyframe(data: &Bytes) -> bool {
    // assumings h264
    return data.len() >= 2 && data[0] == 0x17 && data[1] != 0x00; // 0x00 is the sequence header, don't count that for now
}

async fn wait_for_client_disconnection(
    connection_id: i32,
    mut receiver: UnboundedReceiver<()>,
) -> FutureResult {
    // The channel should only be closed when the client has disconnected
    while let Some(()) = receiver.recv().await {}

    FutureResult::Disconnection { connection_id }
}
