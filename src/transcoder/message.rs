use bytes::Bytes;

pub enum Message {
    Audio(Bytes),
    Video(Bytes),
}
