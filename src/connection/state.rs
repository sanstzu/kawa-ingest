#[derive(PartialEq)]
pub enum State {
    Waiting,
    Connected {
        app_name: String,
    },
    PublishRequested {
        app_name: String,
        stream_key: String,
        request_id: u32,
    },
    Publishing {
        app_name: String,
        stream_key: String,
    },
}
