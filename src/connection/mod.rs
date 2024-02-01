mod connection_reader;
mod connection_writer;
mod state;

use crate::{
    connection::state::State,
    stream_manager::{
        connection_message::ConnectionMessage, stream_manager_message::StreamManagerMessage,
    },
    BoxError,
};
use log::{error, info, trace, warn};
use rml_rtmp::{
    chunk_io::Packet,
    handshake::{Handshake, HandshakeProcessResult, PeerType},
    sessions::{
        PublishMode, ServerSession, ServerSessionConfig, ServerSessionEvent, ServerSessionResult,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc::{self, UnboundedSender},
};

#[derive(PartialEq)]
enum ConnectionAction {
    None,
    Disconnect,
}

use self::{connection_reader::connection_reader, connection_writer::connection_writer};

pub struct Connection {
    connection_id: i32,
    session: Option<ServerSession>,
    stream_manager_sender: mpsc::UnboundedSender<StreamManagerMessage>,
    state: State,
}

impl Connection {
    pub fn new(
        connection_id: i32,
        stream_manager_sender: mpsc::UnboundedSender<StreamManagerMessage>,
    ) -> Connection {
        Connection {
            connection_id,
            session: None,
            stream_manager_sender,
            state: State::Waiting,
        }
    }

    pub async fn start_handshake(self, mut stream: TcpStream) -> Result<(), BoxError> {
        trace!("Connection {}: Starting handshake", self.connection_id);
        let mut server = Handshake::new(PeerType::Server);

        // Sends s0 and s1 to the client
        match server.generate_outbound_p0_and_p1() {
            Ok(bytes) => stream.write_all(bytes.as_ref()).await?,
            Err(e) => {
                return Err(Box::new(e));
            }
        };

        let mut buffer = [0; 4096];

        loop {
            let bytes_read = stream.read(&mut buffer).await?;
            trace!(
                "Connection {}: Read {} bytes",
                self.connection_id,
                bytes_read
            );
            if bytes_read == 0 {
                return Ok(());
            }

            match server.process_bytes(&buffer[0..bytes_read]) {
                Ok(HandshakeProcessResult::Completed {
                    response_bytes,
                    remaining_bytes,
                }) => {
                    trace!(
                        "Connection {}: Handshake completed, sending {} bytes",
                        self.connection_id,
                        response_bytes.len()
                    );
                    // There might be some remaining bytes after the handshake that is the RTMP messages
                    stream.write_all(&response_bytes).await?;

                    let connection_id = self.connection_id;
                    tokio::spawn(async move {
                        if let Err(e) = self.start_connection_handler(stream, remaining_bytes).await
                        {
                            error!(
                                "Connection {}: Connection handler failed with error: {:?}",
                                connection_id, e
                            );
                        };
                    });
                    return Ok(());
                }
                Ok(HandshakeProcessResult::InProgress { response_bytes }) => {
                    trace!(
                        "Connection {}: Handshake in progress, sending {} bytes",
                        self.connection_id,
                        response_bytes.len()
                    );
                    stream.write_all(&response_bytes).await?;
                }
                Err(e) => {
                    warn!(
                        "Connection {}: Handshake failed with error: {:?}",
                        self.connection_id, e
                    );
                    return Err(Box::new(e));
                }
            }
        }
    }

    pub async fn start_connection_handler(
        mut self,
        stream: TcpStream,
        remaining_bytes: Vec<u8>,
    ) -> Result<(), BoxError> {
        let (stream_reader, stream_writer) = tokio::io::split(stream);
        let (read_bytes_tx, mut read_bytes_rx) = mpsc::unbounded_channel();
        let (mut write_bytes_tx, write_bytes_rx) = mpsc::unbounded_channel();

        let (message_tx, mut message_rx) = mpsc::unbounded_channel();

        let (_disconnection_tx, disconnection_rx) = mpsc::unbounded_channel();

        let message = StreamManagerMessage::NewConnection {
            connection_id: self.connection_id,
            sender: message_tx,
            disconnection: disconnection_rx,
        };

        trace!(
            "Connection {}: Sending new connection message",
            self.connection_id
        );
        if self.stream_manager_sender.send(message).is_err() {
            return Ok(());
        }

        // Spawns a new task to handle TCP Connection input
        tokio::spawn(async move {
            if let Err(e) =
                connection_reader(self.connection_id.clone(), stream_reader, read_bytes_tx).await
            {
                error!(
                    "Connection {}: Connection reader failed with error: {:?}",
                    self.connection_id, e
                );
            }
        });

        // Spawns a new task to handle TCP Connection output
        tokio::spawn(async move {
            if let Err(e) =
                connection_writer(self.connection_id.clone(), stream_writer, write_bytes_rx).await
            {
                error!(
                    "Connection {}: Connection writer failed with error: {:?}",
                    self.connection_id, e
                );
            }
        });

        let config = ServerSessionConfig::new();
        let (session, mut results) = ServerSession::new(config)
            .map_err(|e| format!("Server session error occured: {:?}", e))?;

        self.session = Some(session);

        let remaining_bytes_results = self
            .session
            .as_mut()
            .unwrap()
            .handle_input(&remaining_bytes)
            .map_err(|x| format!("Failed to handle input: {:?}", x))?;

        results.extend(remaining_bytes_results);

        loop {
            let action = self.handle_session_results(&mut results, &mut write_bytes_tx)?;

            if action == ConnectionAction::Disconnect {
                break;
            }

            tokio::select! {
                message = read_bytes_rx.recv() => {
                    match message {
                        Some(bytes) => {
                            let session_results = self.session.as_mut().unwrap().handle_input(&bytes).map_err(|x| format!("Failed to handle input: {:?}", x))?;
                            results.extend(session_results);
                        }
                        None => {
                            trace!("Connection {}: Stream manager channel closed/no messages in read_bytes", self.connection_id);
                            break;
                        }
                    }
                }

                manager_message = message_rx.recv() => {
                    match manager_message {
                        Some(message) => {
                            let (new_results, action) = self.handle_connection_message(message)?;

                            match action {
                                ConnectionAction::Disconnect => {
                                    info!("Connection {}: Disconnecting", self.connection_id);
                                    break;
                                }
                                _ => {}
                            }

                            results = new_results
                        }
                        None => {
                            trace!("Connection {}: Stream manager channel closed/no messages in message_rx", self.connection_id);
                            break;
                        }
                    }
                }
            }
        }

        info!("Connection {}: Shutting down", self.connection_id);

        Ok(())
    }

    fn handle_session_results(
        &mut self,
        results: &mut Vec<ServerSessionResult>,
        byte_writer: &mut UnboundedSender<Packet>,
    ) -> Result<ConnectionAction, BoxError> {
        if results.len() == 0 {
            return Ok(ConnectionAction::None);
        }

        let mut new_results = Vec::new();

        for result in results.drain(..) {
            match result {
                ServerSessionResult::OutboundResponse(packet) => match byte_writer.send(packet) {
                    Err(_) => break,
                    _ => {}
                },
                ServerSessionResult::RaisedEvent(event) => {
                    trace!(
                        "Connection {}: Raised event: {:?}",
                        self.connection_id,
                        event
                    );
                    let action = self.handle_raised_event(event, &mut new_results)?;
                    if action == ConnectionAction::Disconnect {
                        return Ok(ConnectionAction::Disconnect);
                    }
                }

                ServerSessionResult::UnhandleableMessageReceived(message) => {
                    warn!("Unhandleable message received: {:?}", message.data);
                }
            }
        }

        self.handle_session_results(&mut new_results, byte_writer)?;

        Ok(ConnectionAction::None)
    }

    fn handle_connection_message(
        &mut self,
        message: ConnectionMessage,
    ) -> Result<(Vec<ServerSessionResult>, ConnectionAction), BoxError> {
        match message {
            ConnectionMessage::RequestAccepted { .. } => {
                info!("Connection {}: Request accepted", self.connection_id);

                let (new_state, return_val) = match &self.state {
                    State::PublishRequested {
                        app_name,
                        stream_key,
                        request_id,
                    } => {
                        // Change state to publishing
                        let new_state = State::Publishing {
                            app_name: app_name.clone(),
                            stream_key: stream_key.clone(),
                        };

                        let results = self
                            .session
                            .as_mut()
                            .unwrap()
                            .accept_request(request_id.clone())
                            .map_err(|x| format!("Failed to accept request: {:?}", x))?;

                        (Some(new_state), (results, ConnectionAction::None))
                    }
                    _ => {
                        error!(
                            "Connection {}: Request accepted but not in publish requested state",
                            self.connection_id
                        );
                        (None, (Vec::new(), ConnectionAction::Disconnect))
                    }
                };

                if new_state.is_some() {
                    self.state = new_state.unwrap();
                }

                Ok(return_val)
            }

            ConnectionMessage::RequestDenied { .. } => {
                info!("Connection {}: Request denied", self.connection_id);

                return match &self.state {
                    State::PublishRequested { .. } => {
                        Ok((Vec::new(), ConnectionAction::Disconnect))
                    }

                    _ => {
                        error!(
                            "Connection {}: Request denied but not in playback requested state",
                            self.connection_id
                        );
                        Ok((Vec::new(), ConnectionAction::Disconnect))
                    }
                };
            }
        }
    }

    fn handle_raised_event(
        &mut self,
        event: ServerSessionEvent,
        new_results: &mut Vec<ServerSessionResult>,
    ) -> Result<ConnectionAction, BoxError> {
        match event {
            ServerSessionEvent::ConnectionRequested {
                request_id,
                app_name,
            } => {
                // Checks if the client is in the waiting state

                info!(
                    "Connection {}: Connection requested connection to app {}",
                    self.connection_id, app_name
                );

                if self.state != State::Waiting {
                    warn!(
                        "Connection {}: Connection requested but not in waiting state",
                        self.connection_id
                    );
                    return Ok(ConnectionAction::Disconnect);
                }

                new_results.extend(
                    self.session
                        .as_mut()
                        .unwrap()
                        .accept_request(request_id)
                        .map_err(|x| format!("Failed to accept request: {:?}", x))?,
                );

                self.state = State::Connected { app_name };
            }
            ServerSessionEvent::PublishStreamRequested {
                request_id,
                app_name,
                stream_key,
                mode,
            } => {
                // Checks if the client is in the connected state

                info!(
                    "Connection {}: Publish stream requested for app {} and stream key {}",
                    self.connection_id, app_name, stream_key
                );

                if mode != PublishMode::Live {}

                match &self.state {
                    State::Connected { .. } => {
                        self.state = State::PublishRequested {
                            app_name: app_name.clone(),
                            stream_key: stream_key.clone(),
                            request_id,
                        };

                        let message = StreamManagerMessage::PublishRequest {
                            connection_id: self.connection_id,
                            stream_key: stream_key.clone(),
                            rtmp_app: app_name.clone(),
                            request_id,
                        };

                        if self.stream_manager_sender.send(message).is_err() {
                            return Ok(ConnectionAction::Disconnect);
                        }
                    }

                    _ => {
                        warn!(
                            "Connection {}: Publish stream requested but not in connected state",
                            self.connection_id
                        );
                        return Ok(ConnectionAction::Disconnect);
                    }
                }
            }

            ServerSessionEvent::VideoDataReceived {
                timestamp, data, ..
            } => match &self.state {
                State::Publishing { .. } => {
                    let message = StreamManagerMessage::NewVideoData {
                        sending_connection_id: self.connection_id,
                        timestamp,
                        data,
                    };

                    if self.stream_manager_sender.send(message).is_err() {
                        return Ok(ConnectionAction::Disconnect);
                    }
                }
                _ => {
                    warn!(
                        "Connection {}: Video data received but not in publishing state",
                        self.connection_id
                    );
                    return Ok(ConnectionAction::Disconnect);
                }
            },

            ServerSessionEvent::AudioDataReceived {
                timestamp, data, ..
            } => match &self.state {
                State::Publishing { .. } => {
                    let message = StreamManagerMessage::NewAudioData {
                        sending_connection_id: self.connection_id,
                        timestamp,
                        data,
                    };

                    if self.stream_manager_sender.send(message).is_err() {
                        return Ok(ConnectionAction::Disconnect);
                    }
                }
                _ => {
                    warn!(
                        "Connection {}: Audio data received but not in publishing state",
                        self.connection_id
                    );
                    return Ok(ConnectionAction::Disconnect);
                }
            },

            ServerSessionEvent::PublishStreamFinished { .. } => {
                let message = StreamManagerMessage::PublishFinished {
                    connection_id: self.connection_id,
                };

                if self.stream_manager_sender.send(message).is_err() {
                    return Ok(ConnectionAction::Disconnect);
                }
            }

            ServerSessionEvent::PlayStreamRequested { .. } => {
                warn!(
                    "Connection {}: Only publish is supported but play stream requested",
                    self.connection_id
                );

                return Ok(ConnectionAction::Disconnect);
            }

            ServerSessionEvent::PlayStreamFinished { .. } => {
                warn!(
                    "Connection {}: Only publish is supported but play stream finished",
                    self.connection_id
                );

                return Ok(ConnectionAction::Disconnect);
            }

            x => {
                warn!(
                    "Connection {}: Unhandled event received: {:?}",
                    self.connection_id, x
                );
            }
        }

        Ok(ConnectionAction::None)
    }
}
