pub enum ConnectionMessage {
    RequestAccepted { request_id: u32 },
    RequestDenied { request_id: u32 },
}
